//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Pwv/PwvHelper.h"
#include "protocol/Pwv/PwvTransaction.h"
#include "protocol/Pwv/PwvWorkload.h"

#include <chrono>
#include <thread>

namespace aria {

template <class Database> class PwvExecutor : public Worker {
public:
  using DatabaseType = Database;
  using WorkloadType = PwvWorkload<Database>;
  using StorageType = typename DatabaseType::StorageType;
  using TransactionType = PwvTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  PwvExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
              const ContextType &context,
              std::vector<std::unique_ptr<TransactionType>> &transactions,
              std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
              std::atomic<uint32_t> &worker_status,
              std::atomic<uint32_t> &n_complete_workers,
              std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages), epoch(epoch),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers), workload(db, random),
        init_transaction(false),
        random(id), // make sure each worker has a different seed.
        sleep_random(reinterpret_cast<uint64_t>(this)) {}

  ~PwvExecutor() = default;

  void start() override {

    LOG(INFO) << "PwvExecutor" << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "PwvExecutor" << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::Pwv_Analysis);

      n_started_workers.fetch_add(1);
      generate_transactions();
      n_complete_workers.fetch_add(1);
      // wait to Execute
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Pwv_Analysis) {
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);
      run_transactions();
      n_complete_workers.fetch_add(1);
      // wait to execute
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Pwv_Execute) {
        std::this_thread::yield();
      }
    }
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";
  }

  void push_message(Message *message) override {}

  Message *pop_message() override { return nullptr; }

  void generate_transactions() {
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      auto partition_id = random.uniform_dist(0, context.partition_num - 1);
      transactions[i] =
          workload.next_transaction(context, partition_id, storages[i]);
      transactions[i]->build_pieces();
    }
    init_transaction = true;
  }

  void run_transactions() {
    for (auto i = 0u; i < transactions.size(); i++) {
      bool commit = transactions[i]->commit(id);
      if (transactions[i]->partition_id % context.worker_num == id) {
        if (commit) {
          n_commit.fetch_add(1);
        } else {
          n_abort_no_retry.fetch_add(1);
        }
      }
    }
  }

private:
  DatabaseType &db;
  const ContextType &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType> &storages;
  std::atomic<uint32_t> &epoch, &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  WorkloadType workload;
  bool init_transaction;
  RandomType random, sleep_random;
  Percentile<int64_t> percentile;
};
} // namespace aria