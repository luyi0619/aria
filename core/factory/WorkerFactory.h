//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Defs.h"
#include "core/Executor.h"
#include "core/Manager.h"

#include "benchmark/tpcc/Workload.h"
#include "benchmark/ycsb/Workload.h"

#include "protocol/TwoPL/TwoPL.h"
#include "protocol/TwoPL/TwoPLExecutor.h"

#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinExecutor.h"
#include "protocol/Calvin/CalvinManager.h"
#include "protocol/Calvin/CalvinTransaction.h"

#include "protocol/Bohm/Bohm.h"
#include "protocol/Bohm/BohmExecutor.h"
#include "protocol/Bohm/BohmManager.h"
#include "protocol/Bohm/BohmTransaction.h"

#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaExecutor.h"
#include "protocol/Aria/AriaManager.h"
#include "protocol/Aria/AriaTransaction.h"

#include "protocol/AriaFB/AriaFB.h"
#include "protocol/AriaFB/AriaFBExecutor.h"
#include "protocol/AriaFB/AriaFBManager.h"
#include "protocol/AriaFB/AriaFBTransaction.h"

#include "protocol/Pwv/PwvExecutor.h"
#include "protocol/Pwv/PwvManager.h"
#include "protocol/Pwv/PwvTransaction.h"

#include <unordered_set>

namespace aria {

template <class Context> class InferType {};

template <> class InferType<aria::tpcc::Context> {
public:
  template <class Transaction>
  using WorkloadType = aria::tpcc::Workload<Transaction>;
};

template <> class InferType<aria::ycsb::Context> {
public:
  template <class Transaction>
  using WorkloadType = aria::ycsb::Workload<Transaction>;
};

class WorkerFactory {

public:
  template <class Database, class Context>
  static std::vector<std::shared_ptr<Worker>>
  create_workers(std::size_t coordinator_id, Database &db,
                 const Context &context, std::atomic<bool> &stop_flag) {

    std::unordered_set<std::string> protocols = {
       "TwoPL",
        "Calvin", "Bohm",   "Aria",   "AriaFB", "Pwv"};
    CHECK(protocols.count(context.protocol) == 1);

    std::vector<std::shared_ptr<Worker>> workers;

    if (context.protocol == "TwoPL") {

      using TransactionType = aria::TwoPLTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<TwoPLExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);
    } else if (context.protocol == "Calvin") {

      using TransactionType = aria::CalvinTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager

      auto manager = std::make_shared<CalvinManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      std::vector<CalvinExecutor<WorkloadType> *> all_executors;

      for (auto i = 0u; i < context.worker_num; i++) {

        auto w = std::make_shared<CalvinExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->lock_manager_status,
            manager->worker_status, manager->n_completed_workers,
            manager->n_started_workers);
        workers.push_back(w);
        manager->add_worker(w);
        all_executors.push_back(w.get());
      }
      // push manager to workers
      workers.push_back(manager);

      for (auto i = 0u; i < context.worker_num; i++) {
        static_cast<CalvinExecutor<WorkloadType> *>(workers[i].get())
            ->set_all_executors(all_executors);
      }
    } else if (context.protocol == "Bohm") {

      using TransactionType = aria::BohmTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager

      auto manager = std::make_shared<BohmManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<BohmExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->epoch, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);
    } else if (context.protocol == "Aria") {

      using TransactionType = aria::AriaTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager

      auto manager = std::make_shared<AriaManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<AriaExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->epoch, manager->worker_status,
            manager->total_abort, manager->n_completed_workers,
            manager->n_started_workers));
      }

      workers.push_back(manager);
    } else if (context.protocol == "AriaFB") {

      using TransactionType = aria::AriaFBTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager

      auto manager = std::make_shared<AriaFBManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      std::vector<AriaFBExecutor<WorkloadType> *> all_executors;

      for (auto i = 0u; i < context.worker_num; i++) {
        auto w = std::make_shared<AriaFBExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->partition_ids, manager->storages, manager->epoch,
            manager->lock_manager_status, manager->worker_status,
            manager->total_abort, manager->n_completed_workers,
            manager->n_started_workers);
        workers.push_back(w);
        manager->add_worker(w);
        all_executors.push_back(w.get());
      }

      // push manager to workers
      workers.push_back(manager);

      for (auto i = 0u; i < context.worker_num; i++) {
        static_cast<AriaFBExecutor<WorkloadType> *>(workers[i].get())
            ->set_all_executors(all_executors);
      }
    } else if (context.protocol == "Pwv") {

      // create manager
      auto manager = std::make_shared<PwvManager<Database>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker
      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<PwvExecutor<Database>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->epoch, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);

    } else {
      CHECK(false) << "protocol: " << context.protocol << " is not supported.";
    }

    return workers;
  }
};
} // namespace aria