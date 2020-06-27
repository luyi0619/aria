//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaMessage.h"
#include "protocol/Aria/AriaTransaction.h"

namespace aria {

template <class Database> class Aria {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using MessageType = AriaMessage;
  using TransactionType = AriaTransaction;

  using MessageFactoryType = AriaMessageFactory;
  using MessageHandlerType = AriaMessageHandler;

  Aria(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &messages) {
    // nothing needs to be done
  }

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {

    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        table->update(key, value);
      } else {
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_write_message(
            *messages[coordinatorID], *table, writeKey.get_key(),
            writeKey.get_value());
      }
    }

    return true;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  Partitioner &partitioner;
};
} // namespace aria