//
// Created by Yi Lu on 2019-09-05.
//

#pragma once

#include <boost/algorithm/string.hpp>
#include <string>
#include <vector>

#include <glog/logging.h>

namespace aria {

class BohmHelper {
public:
  using MetaDataType = std::atomic<uint64_t>;

  static void read(const std::tuple<MetaDataType *, void *> &row, void *dest,
                   std::size_t size) {
    // placeholder contains the TID of the transaction that will update the
    MetaDataType &placeholder = *std::get<0>(row);
    DCHECK(is_placeholder_ready(placeholder))
        << "placeholder not ready to read.";
    void *src = std::get<1>(row);
    std::memcpy(dest, src, size);
  }

  // value 0 meaning the value is ready to read
  static bool is_placeholder_ready(std::atomic<uint64_t> &a) {
    return a.load() == 0;
  }

  static void set_placeholder_to_ready(std::atomic<uint64_t> &a) { a.store(0); }

  // assume the replication group size is 3 and we have partitions 0..8
  // the 1st coordinator has partition 0, 3, 6.
  // the 2nd coordinator has partition 1, 4, 7.
  // the 3rd coordinator has partition 2, 5, 8.
  // the function first maps all partition id to 0, 1, 2 and then use % hash to
  // assign each partition to a lock manager.

  static std::size_t partition_id_to_worker_id(std::size_t partition_id,
                                               std::size_t n_worker,
                                               std::size_t n_coordinator) {
    return partition_id / n_coordinator % n_worker;
  }

public:
  static uint64_t get_epoch(uint64_t value) {
    return (value >> EPOCH_OFFSET) & EPOCH_MASK;
  }

  static uint64_t set_epoch(uint64_t value, uint64_t epoch) {
    DCHECK(epoch < (1ull << 32));
    return (value & (~(EPOCH_MASK << EPOCH_OFFSET))) | (epoch << EPOCH_OFFSET);
  }

  static uint64_t get_pos(uint64_t value) {
    return (value >> POS_OFFSET) & POS_MASK;
  }

  static uint64_t set_pos(uint64_t value, uint64_t pos) {
    DCHECK(pos < (1ull << 32));
    return (value & (~(POS_MASK << POS_OFFSET))) | (pos << POS_OFFSET);
  }

  static uint64_t get_tid(uint64_t epoch, uint64_t pos) {
    DCHECK(epoch < (1ull << 32));
    DCHECK(pos < (1ull << 32));
    return (epoch << EPOCH_OFFSET) | pos;
  }

public:
  /*
   * [ epoch (32) | pos (32) ]
   *
   */

  static constexpr int EPOCH_OFFSET = 32;
  static constexpr uint64_t EPOCH_MASK = 0xfffffffffffull;

  static constexpr int POS_OFFSET = 0;
  static constexpr uint64_t POS_MASK = 0xffffffull;
};
} // namespace aria