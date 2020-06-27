//
// Created by Yi Lu on 1/16/20.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Database.h"
#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Database.h"

namespace aria {
template <class Database> class PwvWorkload {
public:
  using DatabaseType = Database;
  using StorageType = typename DatabaseType::StorageType;
  using TransactionType = PwvTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  PwvWorkload(DatabaseType &db, RandomType &random) {
    CHECK(false) << "this workload is not supported";
  }
  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage) {
    CHECK(false) << "this workload is not supported";
    return nullptr;
  }
};

template <> class PwvWorkload<ycsb::Database> {
public:
  using DatabaseType = ycsb::Database;
  using StorageType = typename DatabaseType::StorageType;
  using TransactionType = PwvTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  PwvWorkload(DatabaseType &db, RandomType &random) : db(db), random(random) {}

  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage) {

    if (context.pwv_ycsb_star) {
      auto p = std::make_unique<PwvYCSBStarTransaction>(db, context, random,
                                                        storage, partition_id);
      return p;
    } else {
      auto p = std::make_unique<PwvYCSBTransaction>(db, context, random,
                                                    storage, partition_id);
      return p;
    }
  }

private:
  DatabaseType &db;
  RandomType &random;
};

template <> class PwvWorkload<tpcc::Database> {
public:
  using DatabaseType = tpcc::Database;
  using StorageType = typename DatabaseType::StorageType;
  using TransactionType = PwvTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  PwvWorkload(DatabaseType &db, RandomType &random) : db(db), random(random) {}

  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage) {

    int x = random.uniform_dist(1, 100);
    std::unique_ptr<TransactionType> p;

    if (context.workloadType == tpcc::TPCCWorkloadType::MIXED) {
      if (x <= 50) {
        p = std::make_unique<PwvNewOrderTransaction>(db, context, random,
                                                     storage, partition_id);
      } else {
        p = std::make_unique<PwvPaymentTransaction>(db, context, random,
                                                    storage, partition_id);
      }
    } else if (context.workloadType == tpcc::TPCCWorkloadType::NEW_ORDER_ONLY) {
      p = std::make_unique<PwvNewOrderTransaction>(db, context, random, storage,
                                                   partition_id);
    } else {
      p = std::make_unique<PwvPaymentTransaction>(db, context, random, storage,
                                                  partition_id);
    }

    return p;
  }

private:
  DatabaseType &db;
  RandomType &random;
};

} // namespace aria
