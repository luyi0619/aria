//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/AriaFB/AriaFB.h"
#include "protocol/AriaFB/AriaFBExecutor.h"
#include "protocol/AriaFB/AriaFBHelper.h"
#include "protocol/AriaFB/AriaFBTransaction.h"

#include <atomic>
#include <thread>
#include <unordered_set>
#include <vector>

namespace aria {

template <class Workload> class AriaFBManager : public aria::Manager {
public:
  using base_type = aria::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = AriaFBTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  AriaFBManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)) {

    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
    partition_ids.resize(context.batch_size);
  }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {

      // the coordinator on each machine first moves the aborted transactions
      // from the last batch earlier to the next batch and set remaining
      // transaction slots to null.

      // then, each worker threads generates a transaction using the same seed.
      epoch.fetch_add(1);

      // LOG(INFO) << "Seed: " << random.get_seed();
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::AriaFB_READ);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      // wait for all machines until they finish the AriaFB_READ phase.
      wait4_ack();

      // Allow each worker to commit transactions
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::AriaFB_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();

      // clean batch now
      cleanup_batch();
      // wait for all machines until they finish the AriaFB_COMMIT phase.
      // this can be skipped
      // wait4_ack();
      wait4_abort_tids_from_non_master();
      broadcast_abort_tids();

      // prepare transactions for calvin and clear the metadata
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::AriaFB_Fallback_Prepare);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      // wait for all machines until they finish the AriaFB_COMMIT phase.
      wait4_ack();

      // calvin execution
      n_started_workers.store(0);
      n_completed_workers.store(0);
      clear_lock_manager_status();
      signal_worker(ExecutorStatus::AriaFB_Fallback);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      // wait for all machines until they finish the AriaFB_COMMIT phase.
      wait4_ack();
    }
    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {
      // LOG(INFO) << "Seed: " << random.get_seed();
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      DCHECK(status == ExecutorStatus::AriaFB_READ);
      // the coordinator on each machine first moves the aborted transactions
      // from the last batch earlier to the next batch and set remaining
      // transaction slots to null.

      epoch.fetch_add(1);

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::AriaFB_READ);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();

      status = wait4_signal();
      DCHECK(status == ExecutorStatus::AriaFB_COMMIT);
      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::AriaFB_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();

      // clean batch now
      cleanup_batch();
      // this can be skipped
      // send_ack();
      send_abort_tids_to_master();
      wait4_abort_tids_from_master();

      status = wait4_signal();
      DCHECK(status == ExecutorStatus::AriaFB_Fallback_Prepare);
      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::AriaFB_Fallback_Prepare);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();

      status = wait4_signal();
      DCHECK(status == ExecutorStatus::AriaFB_Fallback);
      n_started_workers.store(0);
      n_completed_workers.store(0);
      clear_lock_manager_status();
      set_worker_status(ExecutorStatus::AriaFB_Fallback);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();
    }
  }

  void cleanup_batch() {
    abort_tids.clear();
    for (auto i = 0u; i < transactions.size(); i++) {
      if (partitioner->has_master_partition(partition_ids[i]) == false)
        continue;
      if (transactions[i]->abort_lock) {
        abort_tids.push_back(transactions[i]->id);
      }
    }
  }

  // the following two functions are called on master

  void wait4_abort_tids_from_non_master() {
    CHECK(coordinator_id == 0);

    std::size_t n_coordinators = context.coordinator_num;

    for (auto i = 0u; i < n_coordinators - 1; i++) {
      vector_in_queue.wait_till_non_empty();
      std::unique_ptr<Message> message(vector_in_queue.front());
      bool ok = vector_in_queue.pop();
      CHECK(ok);
      CHECK(message->get_message_count() == 1);

      MessagePiece messagePiece = *(message->begin());
      auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
      CHECK(type == ControlMessage::VECTOR);

      decltype(abort_tids.size()) sz;
      StringPiece stringPiece = messagePiece.toStringPiece();
      Decoder dec(stringPiece);
      dec >> sz;
      for (auto k = 0u; k < sz; k++) {
        int val;
        dec >> val;
        transactions[val - 1]->abort_lock = true;
        abort_tids.push_back(val);
      }
    }
  }

  void broadcast_abort_tids() {
    CHECK(coordinator_id == 0);
    std::size_t n_coordinators = context.coordinator_num;
    for (auto i = 0u; i < n_coordinators; i++) {
      if (i == coordinator_id)
        continue;
      ControlMessageFactory::new_vector_message(*messages[i], abort_tids);
      flush_messages();
    }
  }

  // the following two functions are called on non-master
  void send_abort_tids_to_master() {
    CHECK(coordinator_id != 0);

    ControlMessageFactory::new_vector_message(*messages[0], abort_tids);
    flush_messages();
  }

  void wait4_abort_tids_from_master() {
    CHECK(coordinator_id != 0);
    std::size_t n_coordinators = context.coordinator_num;

    vector_in_queue.wait_till_non_empty();
    std::unique_ptr<Message> message(vector_in_queue.front());
    bool ok = vector_in_queue.pop();
    CHECK(ok);
    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());
    auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
    CHECK(type == ControlMessage::VECTOR);

    decltype(abort_tids.size()) sz;
    abort_tids.clear();
    StringPiece stringPiece = messagePiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> sz;
    for (auto k = 0u; k < sz; k++) {
      int val;
      dec >> val;
      abort_tids.push_back(val);
      transactions[val - 1]->abort_lock = true;
    }
  }

  void add_worker(const std::shared_ptr<AriaFBExecutor<WorkloadType>>

                      &w) {
    workers.push_back(w);
  }

  void clear_lock_manager_status() { lock_manager_status.store(0); }

public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::atomic<uint32_t> lock_manager_status;
  std::vector<std::shared_ptr<AriaFBExecutor<WorkloadType>>> workers;
  std::unique_ptr<Partitioner> partitioner;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::vector<std::size_t> partition_ids;
  std::atomic<uint32_t> total_abort;
  std::vector<int> abort_tids;
};
} // namespace aria