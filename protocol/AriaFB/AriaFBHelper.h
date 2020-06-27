//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include <atomic>
#include <cstring>
#include <tuple>

#include "core/Table.h"

#include "glog/logging.h"
#include "protocol/AriaFB/AriaFBRWKey.h"

namespace aria {

class AriaFBHelper {

public:
  using MetaDataType = std::atomic<uint64_t>;

  static uint64_t read(const std::tuple<MetaDataType *, void *> &row,
                       void *dest, std::size_t size) {
    MetaDataType &tid = *std::get<0>(row);
    void *src = std::get<1>(row);
    std::memcpy(dest, src, size);
    return tid.load();
  }

  static void
  set_key_tid(AriaFBRWKey &key,
              const std::tuple<std::atomic<uint64_t> *, void *> &row) {
    key.set_tid(std::get<0>(row));
  }

  static std::atomic<uint64_t> &get_metadata(ITable *table,
                                             const AriaFBRWKey &key) {
    auto tid = key.get_tid();
    if (!tid) {
      tid = &table->search_metadata(key.get_key());
    }
    return *tid;
  }

  static bool reserve_read(std::atomic<uint64_t> &a, uint64_t epoch,
                           uint32_t tid) {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      uint64_t old_epoch = get_epoch(old_value);
      uint64_t old_rts = get_rts(old_value);

      CHECK(epoch >= old_epoch);
      if (epoch > old_epoch) {
        new_value = set_epoch(0, epoch);
        new_value = set_rts(new_value, tid);
      } else {

        if (old_rts < tid && old_rts != 0) {
          return false;
        }
        // keep wts
        new_value = old_value;
        new_value = set_rts(new_value, tid);
      }
    } while (!a.compare_exchange_weak(old_value, new_value));
    return true;
  }

  static bool reserve_write(std::atomic<uint64_t> &a, uint64_t epoch,
                            uint32_t tid) {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      uint64_t old_epoch = get_epoch(old_value);
      uint64_t old_wts = get_wts(old_value);

      CHECK(epoch >= old_epoch);
      if (epoch > old_epoch) {
        new_value = set_epoch(0, epoch);
        new_value = set_wts(new_value, tid);
      } else {

        if (old_wts < tid && old_wts != 0) {
          return false;
        }
        // keep rts
        new_value = old_value;
        new_value = set_wts(new_value, tid);
      }
    } while (!a.compare_exchange_weak(old_value, new_value));
    return true;
  }

  static uint64_t get_epoch(uint64_t value) {
    return (value >> EPOCH_OFFSET) & EPOCH_MASK;
  }

  static uint64_t set_epoch(uint64_t value, uint64_t epoch) {
    DCHECK(epoch < (1ull << 24));
    return (value & (~(EPOCH_MASK << EPOCH_OFFSET))) | (epoch << EPOCH_OFFSET);
  }

  static uint64_t get_rts(uint64_t value) {
    return (value >> RTS_OFFSET) & RTS_MASK;
  }

  static uint64_t set_rts(uint64_t value, uint64_t rts) {
    DCHECK(rts < (1ull << 20));
    return (value & (~(RTS_MASK << RTS_OFFSET))) | (rts << RTS_OFFSET);
  }

  static uint64_t get_wts(uint64_t value) {
    return (value >> WTS_OFFSET) & WTS_MASK;
  }

  static uint64_t set_wts(uint64_t value, uint64_t wts) {
    DCHECK(wts < (1ull << 20));
    return (value & (~(WTS_MASK << WTS_OFFSET))) | (wts << WTS_OFFSET);
  }

  static void set_tid(std::atomic<uint64_t> &a, uint64_t tid) { a.store(tid); }

  /* the following functions are for Calvin */

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
  /*
   * [epoch (24) | read-rts  (20) | write-wts (20)]
   *
   */

  static constexpr int EPOCH_OFFSET = 40;
  static constexpr uint64_t EPOCH_MASK = 0xffffffull;

  static constexpr int RTS_OFFSET = 20;
  static constexpr uint64_t RTS_MASK = 0xfffffull;

  static constexpr int WTS_OFFSET = 0;
  static constexpr uint64_t WTS_MASK = 0xfffffull;

  /* the following masks are for Calvin */

  static constexpr int LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t LOCK_BIT_MASK = 0x3ffull;

  static constexpr int READ_LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t READ_LOCK_BIT_MASK = 0x1ffull;

  static constexpr int WRITE_LOCK_BIT_OFFSET = 63;
  static constexpr uint64_t WRITE_LOCK_BIT_MASK = 0x1ull;
};

} // namespace aria