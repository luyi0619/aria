//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <thread>

namespace aria {

class AriaTransaction {

public:
  using MetaDataType = std::atomic<uint64_t>;

  AriaTransaction(std::size_t coordinator_id, std::size_t partition_id,
                  Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
  }

  virtual ~AriaTransaction() = default;

  void reset() {
    abort_lock = false;
    abort_no_retry = false;
    abort_read_validation = false;
    distributed_transaction = false;
    execution_phase = false;
    waw = false;
    war = false;
    raw = false;
    pendingResponses = 0;
    network_size = 0;
    operation.clear();
    readSet.clear();
    writeSet.clear();
  }

  virtual TransactionResult execute(std::size_t worker_id) = 0;

  virtual void reset_query() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }

    AriaRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit();
    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }
    AriaRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }
    AriaRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    if (execution_phase) {
      return;
    }
    AriaRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const AriaRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const AriaRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(std::size_t id) { this->id = id; }

  void set_tid_offset(std::size_t offset) { this->tid_offset = offset; }

  void set_epoch(uint32_t epoch) { this->epoch = epoch; }

  bool process_requests(std::size_t worker_id) {

    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (!readSet[i].get_read_request_bit()) {
        break;
      }

      AriaRWKey &readKey = readSet[i];
      readRequestHandler(readKey, id, i);
      readSet[i].clear_read_request_bit();
    }

    return false;
  }

  bool is_read_only() { return writeSet.size() == 0; }

public:
  std::size_t coordinator_id, partition_id, id, tid_offset;
  uint32_t epoch;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;
  std::size_t network_size;

  bool abort_lock, abort_no_retry, abort_read_validation;
  bool distributed_transaction;
  bool execution_phase;
  bool waw, war, raw;

  // read_key, id, key_offset
  std::function<void(AriaRWKey &, std::size_t, std::size_t)> readRequestHandler;

  // processed a request?
  std::function<std::size_t(void)> remote_request_handler;

  std::function<void()> message_flusher;

  Partitioner &partitioner;
  Operation operation; // never used
  std::vector<AriaRWKey> readSet, writeSet;
};
} // namespace aria