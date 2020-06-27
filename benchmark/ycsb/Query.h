//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Random.h"
#include "common/Zipf.h"

namespace aria {
namespace ycsb {

template <std::size_t N> struct YCSBQuery {
  int32_t Y_KEY[N];
  bool UPDATE[N];
};

template <std::size_t N> class makeYCSBQuery {

private:
  void make_multi_partitions(YCSBQuery<N> &query, const Context &context,
                             uint32_t partitionID, Random &random) const {
    int readOnly = random.uniform_dist(1, 100);
    int crossPartition = random.uniform_dist(1, 100);

    for (auto i = 0u; i < N; i++) {
      // read or write

      if (readOnly <= context.readOnlyTransaction) {
        query.UPDATE[i] = false;
      } else {
        int readOrWrite = random.uniform_dist(1, 100);
        if (readOrWrite <= context.readWriteRatio) {
          query.UPDATE[i] = false;
        } else {
          query.UPDATE[i] = true;
        }
      }

      int32_t key;

      // generate a key in a partition
      bool retry;
      do {
        retry = false;

        // a uniform key is generated in three cases
        // case 1: it is a uniform distribution
        // case 2: the skew pattern is read, but this is a key for update
        // case 3: the skew pattern is write, but this is a kew for read

        if (context.isUniform ||
            (context.skewPattern == YCSBSkewPattern::READ && query.UPDATE[i]) ||
            (context.skewPattern == YCSBSkewPattern::WRITE &&
             query.UPDATE[i] == false)) {
          key = random.uniform_dist(
              0, static_cast<int>(context.keysPerPartition) - 1);
        } else {
          key = Zipf::globalZipf().value(random.next_double());
        }

        if (crossPartition <= context.crossPartitionProbability &&
            context.partition_num > 1) {
          auto newPartitionID = partitionID;
          while (newPartitionID == partitionID) {
            newPartitionID = random.uniform_dist(0, context.partition_num - 1);
          }
          query.Y_KEY[i] = context.getGlobalKeyID(key, newPartitionID);
        } else {
          query.Y_KEY[i] = context.getGlobalKeyID(key, partitionID);
        }

        for (auto k = 0u; k < i; k++) {
          if (query.Y_KEY[k] == query.Y_KEY[i]) {
            retry = true;
            break;
          }
        }
      } while (retry);
    }
  }

  void make_two_partitions(YCSBQuery<N> &query, const Context &context,
                           uint32_t partitionID, Random &random) const {
    int readOnly = random.uniform_dist(1, 100);
    int crossPartition = random.uniform_dist(1, 100);
    auto newPartitionID = partitionID;
    if (crossPartition <= context.crossPartitionProbability &&
        context.partition_num > 1) {
      newPartitionID = partitionID;
      while (newPartitionID == partitionID) {
        newPartitionID = random.uniform_dist(0, context.partition_num - 1);
      }
    }

    for (auto i = 0u; i < N; i++) {
      // read or write

      if (readOnly <= context.readOnlyTransaction) {
        query.UPDATE[i] = false;
      } else {
        int readOrWrite = random.uniform_dist(1, 100);
        if (readOrWrite <= context.readWriteRatio) {
          query.UPDATE[i] = false;
        } else {
          query.UPDATE[i] = true;
        }
      }

      int32_t key;

      // generate a key in a partition
      bool retry;
      do {
        retry = false;

        if (context.isUniform) {
          key = random.uniform_dist(
              0, static_cast<int>(context.keysPerPartition) - 1);
        } else {
          key = Zipf::globalZipf().value(random.next_double());
        }

        if (2 * i >= N) {
          query.Y_KEY[i] = context.getGlobalKeyID(key, newPartitionID);
        } else {
          query.Y_KEY[i] = context.getGlobalKeyID(key, partitionID);
        }

        for (auto k = 0u; k < i; k++) {
          if (query.Y_KEY[k] == query.Y_KEY[i]) {
            retry = true;
            break;
          }
        }
      } while (retry);
    }
  }

  void make_global_key_space_query(YCSBQuery<N> &query, const Context &context,
                                   uint32_t partitionID, Random &random) const {
    int readOnly = random.uniform_dist(1, 100);

    for (auto i = 0u; i < N; i++) {
      // read or write
      if (readOnly <= context.readOnlyTransaction) {
        query.UPDATE[i] = false;
      } else {
        int readOrWrite = random.uniform_dist(1, 100);
        if (readOrWrite <= context.readWriteRatio) {
          query.UPDATE[i] = false;
        } else {
          query.UPDATE[i] = true;
        }
      }

      int32_t key;

      bool retry;
      do {
        retry = false;

        if (context.isUniform) {
          key =
              random.uniform_dist(0, static_cast<int>(context.keysPerPartition *
                                                      context.partition_num) -
                                         1);
        } else {
          key = Zipf::globalZipf().value(random.next_double());
        }
        query.Y_KEY[i] = key;

        for (auto k = 0u; k < i; k++) {
          if (query.Y_KEY[k] == query.Y_KEY[i]) {
            retry = true;
            break;
          }
        }
      } while (retry);
    }
  }

public:
  YCSBQuery<N> operator()(const Context &context, uint32_t partitionID,
                          Random &random) const {

    YCSBQuery<N> query;

    if (context.global_key_space) {
      make_global_key_space_query(query, context, partitionID, random);
    } else {
      if (context.two_partitions) {
        make_two_partitions(query, context, partitionID, random);
      } else {
        make_multi_partitions(query, context, partitionID, random);
      }
    }
    return query;
  }
};
} // namespace ycsb
} // namespace aria
