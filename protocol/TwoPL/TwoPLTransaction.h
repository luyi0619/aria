//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include "common/Message.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/TwoPL/TwoPLRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace aria {
class TwoPLTransaction {

public:
  using MetaDataType = std::atomic<uint64_t>;

  TwoPLTransaction(std::size_t coordinator_id, std::size_t partition_id,
                   Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
  }

  virtual ~TwoPLTransaction() = default;

  void reset() {
    pendingResponses = 0;
    network_size = 0;
    abort_lock = false;
    abort_read_validation = false;
    local_validated = false;
    si_in_serializable = false;
    distributed_transaction = false;
    execution_phase = true;
    operation.clear();
    readSet.clear();
    writeSet.clear();
  }

  virtual TransactionResult execute(std::size_t worker_id) = 0;

  virtual void reset_query() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {
    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit();
    readKey.set_read_lock_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {

    if (!partitioner.has_master_partition(partition_id)) {
      pendingResponses++;
    }

    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_lock_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {

    if (!partitioner.has_master_partition(partition_id)) {
      pendingResponses++;
    }

    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_write_lock_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {

    TwoPLRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  bool process_requests(std::size_t worker_id) {

    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (!readSet[i].get_read_lock_request_bit() &&
          !readSet[i].get_write_lock_request_bit()) {
        break;
      }

      const TwoPLRWKey &readKey = readSet[i];
      bool success, remote;
      auto tid = lock_request_handler(
          readKey.get_table_id(), readKey.get_partition_id(), i,
          readKey.get_key(), readKey.get_value(),
          readSet[i].get_local_index_read_bit(),
          readSet[i].get_write_lock_request_bit(), success, remote);

      if (!remote) {
        if (success) {
          readSet[i].set_tid(tid);
          if (readSet[i].get_read_lock_request_bit() &&
              !readSet[i].get_local_index_read_bit()) {
            readSet[i].set_read_lock_bit();
          }

          if (readSet[i].get_write_lock_request_bit()) {
            readSet[i].set_write_lock_bit();
          }
        } else {
          abort_lock = true;
        }
      }

      readSet[i].clear_read_lock_request_bit();
      readSet[i].clear_write_lock_request_bit();
    }

    if (pendingResponses > 0) {
      message_flusher();
      while (pendingResponses > 0) {
        remote_request_handler();
      }
    }
    return false;
  }

  TwoPLRWKey *get_read_key(const void *key) {

    for (auto i = 0u; i < readSet.size(); i++) {
      if (readSet[i].get_key() == key) {
        return &readSet[i];
      }
    }

    return nullptr;
  }

  std::size_t add_to_read_set(const TwoPLRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const TwoPLRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

public:
  std::size_t coordinator_id, partition_id;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;
  std::size_t network_size;
  bool abort_lock, abort_read_validation, local_validated, si_in_serializable;
  bool distributed_transaction;
  bool execution_phase;

  // table id, partition id, key, value, local_index_read?, write_lock?,
  // success?, remote?
  std::function<uint64_t(std::size_t, std::size_t, uint32_t, const void *,
                         void *, bool, bool, bool &, bool &)>
      lock_request_handler;
  // processed a request?
  std::function<std::size_t(void)> remote_request_handler;

  std::function<void()> message_flusher;

  Partitioner &partitioner;
  Operation operation;
  std::vector<TwoPLRWKey> readSet, writeSet;
};
} // namespace aria