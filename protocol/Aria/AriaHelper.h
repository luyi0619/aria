//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include <atomic>
#include <cstring>
#include <tuple>

#include "core/Table.h"

#include "glog/logging.h"
#include "protocol/Aria/AriaRWKey.h"

namespace aria {

class AriaHelper {

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
  set_key_tid(AriaRWKey &key,
              const std::tuple<std::atomic<uint64_t> *, void *> &row) {
    key.set_tid(std::get<0>(row));
  }

  static std::atomic<uint64_t> &get_metadata(ITable *table,
                                             const AriaRWKey &key) {
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
};

} // namespace aria