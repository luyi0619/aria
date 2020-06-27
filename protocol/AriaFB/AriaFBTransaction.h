//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/AriaFB/AriaFBHelper.h"
#include "protocol/AriaFB/AriaFBRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <thread>

namespace aria {

class AriaFBTransaction {

public:
  using MetaDataType = std::atomic<uint64_t>;

  AriaFBTransaction(std::size_t coordinator_id, std::size_t partition_id,
                    Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    relevant = false;
    reset();
  }

  virtual ~AriaFBTransaction() = default;

  void reset() {

    run_in_aria = false;

    abort_lock = false;
    abort_no_retry = false;
    abort_read_validation = false;
    distributed_transaction = false;
    execution_phase = false;

    waw = false;
    war = false;
    raw = false;
    pendingResponses = 0;
    network_size.store(0);

    n_active_coordinators = 0;

    clear_working_set();

    local_read.store(0);
    remote_read.store(0);
  }

  void clear_working_set() {
    execution_phase = false;
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

    AriaFBRWKey readKey;

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

    AriaFBRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();
    readKey.set_read_lock_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }

    AriaFBRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();
    readKey.set_write_lock_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    if (execution_phase) {
      return;
    }

    AriaFBRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const AriaFBRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const AriaFBRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(std::size_t id) { this->id = id; }

  void set_tid_offset(std::size_t offset) { this->tid_offset = offset; }

  void set_epoch(uint32_t epoch) { this->epoch = epoch; }

  bool is_read_only() { return writeSet.size() == 0; }

  void setup_process_requests_in_prepare_phase() {
    // process the reads in read-only index
    // for general reads, increment the local_read and remote_read counter.
    // the function may be called multiple times, the keys are processed in
    // reverse order.
    process_requests = [this](std::size_t worker_id) {
      // cannot use unsigned type in reverse iteration
      for (int i = int(readSet.size()) - 1; i >= 0; i--) {
        // early return
        if (readSet[i].get_prepare_processed_bit()) {
          break;
        }

        if (readSet[i].get_local_index_read_bit() == false) {
          if (partitioner.has_master_partition(readSet[i].get_partition_id())) {
            local_read.fetch_add(1);
          } else {
            remote_read.fetch_add(1);
          }
        }
        readSet[i].set_prepare_processed_bit();
      }
      return false;
    };
  }

  void setup_process_requests_in_execution_phase() {

    process_requests = [this](std::size_t worker_id) {
      // cannot use unsigned type in reverse iteration
      for (int i = int(readSet.size()) - 1; i >= 0; i--) {
        // early return
        if (!readSet[i].get_read_request_bit()) {
          break;
        }

        AriaFBRWKey &readKey = readSet[i];
        if (readSet[i].get_local_index_read_bit() == false) {
          if (partitioner.has_master_partition(readSet[i].get_partition_id())) {
            local_read.fetch_add(1);
          } else {
            remote_read.fetch_add(1);
          }
        }

        AriaFB_read_handler(readKey, id, i);
        readSet[i].clear_read_request_bit();
      }
      return false;
    };
  }

  void
  setup_process_requests_in_fallback_phase(std::size_t n_lock_manager,
                                           std::size_t n_worker,
                                           std::size_t replica_group_size) {

    // only read the keys with locks from the lock_manager_id
    process_requests = [this, n_lock_manager, n_worker,
                        replica_group_size](std::size_t worker_id) {
      auto lock_manager_id = AriaFBHelper::worker_id_to_lock_manager_id(
          worker_id, n_lock_manager, n_worker);

      // cannot use unsigned type in reverse iteration
      for (int i = int(readSet.size()) - 1; i >= 0; i--) {

        if (readSet[i].get_local_index_read_bit()) {
          continue;
        }

        if (AriaFBHelper::partition_id_to_lock_manager_id(
                readSet[i].get_partition_id(), n_lock_manager,
                replica_group_size) != lock_manager_id) {
          continue;
        }

        // early return
        if (readSet[i].get_execution_processed_bit()) {
          break;
        }

        auto &readKey = readSet[i];
        calvin_read_handler(worker_id, readKey.get_table_id(),
                            readKey.get_partition_id(), id, i,
                            readKey.get_key(), readKey.get_value());

        readSet[i].set_execution_processed_bit();
      }

      message_flusher(worker_id);

      if (active_coordinators[coordinator_id]) {

        // spin on local & remote read
        while (local_read.load() > 0 || remote_read.load() > 0) {
          // process remote reads for other workers
          remote_request_handler(worker_id);
        }

        return false;
      } else {
        // abort if not active
        return true;
      }
    };
  }

  void clear_execution_bit() {
    for (auto i = 0u; i < readSet.size(); i++) {

      if (readSet[i].get_local_index_read_bit()) {
        continue;
      }

      readSet[i].clear_execution_processed_bit();
    }
  }

public:
  std::size_t coordinator_id, partition_id, id, tid_offset;
  uint32_t epoch;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;

  std::atomic<int32_t> network_size;
  std::atomic<int32_t> local_read, remote_read;

  bool abort_lock, abort_no_retry, abort_read_validation;
  bool distributed_transaction;
  bool relevant, run_in_aria;
  bool execution_phase;
  bool waw, war, raw;

  std::function<bool(std::size_t)> process_requests;

  // table id, partition id, key, value
  std::function<void(std::size_t, std::size_t, const void *, void *)>
      local_index_read_handler;

  // read_key, id, key_offset
  std::function<void(AriaFBRWKey &, std::size_t, std::size_t)>
      AriaFB_read_handler;

  // table id, partition id, id, key_offset, key, value
  std::function<void(std::size_t, std::size_t, std::size_t, std::size_t,
                     uint32_t, const void *, void *)>
      calvin_read_handler;

  // processed a request?
  std::function<std::size_t(std::size_t)> remote_request_handler;

  std::function<void(std::size_t)> message_flusher;

  Partitioner &partitioner;
  std::vector<bool> active_coordinators;
  std::size_t n_active_coordinators;
  Operation operation; // never used
  std::vector<AriaFBRWKey> readSet, writeSet;
};
} // namespace aria