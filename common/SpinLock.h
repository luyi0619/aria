//
// Created by Yi Lu on 7/14/18.
//

#pragma once

#include <atomic>
#include <ostream>

namespace aria {
class SpinLock {
public:
  // constructors
  SpinLock() = default;

  SpinLock(const SpinLock &) = delete;            // non construction-copyable
  SpinLock &operator=(const SpinLock &) = delete; // non copyable

  // Modifiers
  void lock() {
    while (lock_.test_and_set(std::memory_order_acquire))
      ;
  }

  void unlock() { lock_.clear(std::memory_order_release); }

  // friend declaration
  friend std::ostream &operator<<(std::ostream &, const SpinLock &);

private:
  std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
};
} // namespace aria
