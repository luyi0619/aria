//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "core/Table.h"
#include "protocol/Calvin/CalvinHelper.h"
#include "protocol/Calvin/CalvinMessage.h"
#include "protocol/Calvin/CalvinPartitioner.h"
#include "protocol/Calvin/CalvinTransaction.h"

namespace aria {

template <class Database> class Calvin {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using MessageType = CalvinMessage;
  using TransactionType = CalvinTransaction;

  using MessageFactoryType = CalvinMessageFactory;
  using MessageHandlerType = CalvinMessageHandler;

  Calvin(DatabaseType &db, CalvinPartitioner &partitioner)
      : db(db), partitioner(partitioner) {}

  void abort(TransactionType &txn, std::size_t lock_manager_id,
             std::size_t n_lock_manager, std::size_t replica_group_size) {
    // release read locks
    release_read_locks(txn, lock_manager_id, n_lock_manager,
                       replica_group_size);
  }

  bool commit(TransactionType &txn, std::size_t lock_manager_id,
              std::size_t n_lock_manager, std::size_t replica_group_size) {

    // write to db
    write(txn, lock_manager_id, n_lock_manager, replica_group_size);

    // release read/write locks
    release_read_locks(txn, lock_manager_id, n_lock_manager,
                       replica_group_size);
    release_write_locks(txn, lock_manager_id, n_lock_manager,
                        replica_group_size);

    return true;
  }

  void write(TransactionType &txn, std::size_t lock_manager_id,
             std::size_t n_lock_manager, std::size_t replica_group_size) {

    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      if (CalvinHelper::partition_id_to_lock_manager_id(
              writeKey.get_partition_id(), n_lock_manager,
              replica_group_size) != lock_manager_id) {
        continue;
      }

      auto key = writeKey.get_key();
      auto value = writeKey.get_value();
      table->update(key, value);
    }
  }

  void release_read_locks(TransactionType &txn, std::size_t lock_manager_id,
                          std::size_t n_lock_manager,
                          std::size_t replica_group_size) {
    // release read locks
    auto &readSet = txn.readSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      if (!readKey.get_read_lock_bit()) {
        continue;
      }

      if (CalvinHelper::partition_id_to_lock_manager_id(
              readKey.get_partition_id(), n_lock_manager, replica_group_size) !=
          lock_manager_id) {
        continue;
      }

      auto key = readKey.get_key();
      auto value = readKey.get_value();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      CalvinHelper::read_lock_release(tid);
    }
  }

  void release_write_locks(TransactionType &txn, std::size_t lock_manager_id,
                           std::size_t n_lock_manager,
                           std::size_t replica_group_size) {

    // release write lock
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      if (CalvinHelper::partition_id_to_lock_manager_id(
              writeKey.get_partition_id(), n_lock_manager,
              replica_group_size) != lock_manager_id) {
        continue;
      }

      auto key = writeKey.get_key();
      auto value = writeKey.get_value();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      CalvinHelper::write_lock_release(tid);
    }
  }

private:
  DatabaseType &db;
  CalvinPartitioner &partitioner;
};
} // namespace aria