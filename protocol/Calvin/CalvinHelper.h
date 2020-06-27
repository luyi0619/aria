//
// Created by Yi Lu on 9/15/18.
//

#pragma once

#include <boost/algorithm/string.hpp>
#include <string>
#include <vector>

#include <glog/logging.h>

namespace aria {

class CalvinHelper {

public:
  using MetaDataType = std::atomic<uint64_t>;

  static std::vector<std::size_t> string_to_vint(const std::string &str) {
    std::vector<std::string> vstr;
    boost::algorithm::split(vstr, str, boost::is_any_of(","));
    std::vector<std::size_t> vint;
    for (auto i = 0u; i < vstr.size(); i++) {
      vint.push_back(std::atoi(vstr[i].c_str()));
    }
    return vint;
  }

  static std::size_t
  n_lock_manager(std::size_t replica_group_id, std::size_t id,
                 const std::vector<std::size_t> &lock_managers) {
    CHECK(replica_group_id < lock_managers.size());
    return lock_managers[replica_group_id];
  }

  // assume there are n = 2 lock managers and m = 4 workers
  // the following function maps
  // (2, 2, 4) => 0
  // (3, 2, 4) => 0
  // (4, 2, 4) => 1
  // (5, 2, 4) => 1

  static std::size_t worker_id_to_lock_manager_id(std::size_t id,
                                                  std::size_t n_lock_manager,
                                                  std::size_t n_worker) {
    if (id < n_lock_manager) {
      return id;
    }
    return (id - n_lock_manager) / (n_worker / n_lock_manager);
  }

  // assume the replication group size is 3 and we have partitions 0..8
  // the 1st coordinator has partition 0, 3, 6.
  // the 2nd coordinator has partition 1, 4, 7.
  // the 3rd coordinator has partition 2, 5, 8.
  // the function first maps all partition id to 0, 1, 2 and then use % hash to
  // assign each partition to a lock manager.

  static std::size_t
  partition_id_to_lock_manager_id(std::size_t partition_id,
                                  std::size_t n_lock_manager,
                                  std::size_t replica_group_size) {
    return partition_id / replica_group_size % n_lock_manager;
  }

  static void read(const std::tuple<MetaDataType *, void *> &row, void *dest,
                   std::size_t size) {

    MetaDataType &tid = *std::get<0>(row);
    void *src = std::get<1>(row);
    std::memcpy(dest, src, size);
  }

  /**
   *
   * The following code is adapted from TwoPLHelper.h
   * For Calvin, we can use lower 63 bits for read locks.
   * However, 511 locks are enough and the code above is well tested.
   *
   * [write lock bit (1) |  read lock bit (9) -- 512 - 1 locks ]
   *
   */

  static bool is_read_locked(uint64_t value) {
    return value & (READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
  }

  static bool is_write_locked(uint64_t value) {
    return value & (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
  }

  static uint64_t read_lock_num(uint64_t value) {
    return (value >> READ_LOCK_BIT_OFFSET) & READ_LOCK_BIT_MASK;
  }

  static uint64_t read_lock_max() { return READ_LOCK_BIT_MASK; }

  static uint64_t read_lock(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    do {
      do {
        old_value = a.load();
      } while (is_write_locked(old_value) ||
               read_lock_num(old_value) == read_lock_max());
      new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
    return remove_lock_bit(old_value);
  }

  static uint64_t write_lock(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;

    do {
      do {
        old_value = a.load();
      } while (is_read_locked(old_value) || is_write_locked(old_value));

      new_value = old_value + (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);

    } while (!a.compare_exchange_weak(old_value, new_value));
    return remove_lock_bit(old_value);
  }

  static void read_lock_release(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      DCHECK(is_read_locked(old_value));
      DCHECK(!is_write_locked(old_value));
      new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
  }

  static void write_lock_release(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    old_value = a.load();
    DCHECK(!is_read_locked(old_value));
    DCHECK(is_write_locked(old_value));
    new_value = old_value - (1ull << WRITE_LOCK_BIT_OFFSET);
    bool ok = a.compare_exchange_strong(old_value, new_value);
    DCHECK(ok);
  }

  static uint64_t remove_lock_bit(uint64_t value) {
    return value & ~(LOCK_BIT_MASK << LOCK_BIT_OFFSET);
  }

  static uint64_t remove_read_lock_bit(uint64_t value) {
    return value & ~(READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
  }

  static uint64_t remove_write_lock_bit(uint64_t value) {
    return value & ~(WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
  }

public:
  static constexpr int LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t LOCK_BIT_MASK = 0x3ffull;

  static constexpr int READ_LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t READ_LOCK_BIT_MASK = 0x1ffull;

  static constexpr int WRITE_LOCK_BIT_OFFSET = 63;
  static constexpr uint64_t WRITE_LOCK_BIT_MASK = 0x1ull;
};
} // namespace aria