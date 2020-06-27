//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#include "core/Manager.h"
#include "protocol/Pwv/PwvExecutor.h"
#include "protocol/Pwv/PwvHelper.h"
#include "protocol/Pwv/PwvTransaction.h"

#include <thread>
#include <vector>

namespace aria {
template <class Database> class PwvManager : public aria::Manager {
public:
  using base_type = aria::Manager;
  using DatabaseType = Database;
  using WorkloadType = PwvWorkload<Database>;
  using StorageType = typename DatabaseType::StorageType;
  using TransactionType = PwvTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  PwvManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
             const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0) {

    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
  }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {

      // the coordinator generates a batch of transactions

      epoch.fetch_add(1);

      // LOG(INFO) << "Seed: " << random.get_seed();
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Pwv_Analysis);
      // Allow each worker to analyse the read/write set
      // each worker analyse i, i + n, i + 2n transaction
      wait_all_workers_start();
      wait_all_workers_finish();

      // Allow each worker to run transactions
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Pwv_Execute);
      wait_all_workers_start();
      wait_all_workers_finish();
    }

    signal_worker(ExecutorStatus::EXIT);
  }

public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
};
} // namespace aria