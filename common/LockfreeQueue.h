//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "glog/logging.h"
#include <boost/lockfree/spsc_queue.hpp>

namespace aria {

/*
 * boost::lockfree::spsc_queue does not support move only objects, e.g.,
 * std::unique_ptr<T>. As a result, only Message* can be pushed into
 * MessageQueue. Usage: std::unique_ptr<Message> ptr; MessageQueue q;
 * q.push(ptr.release());
 *
 * std::unique_ptr<Message> ptr1(q.front());
 * q.pop();
 *
 */

template <class T, std::size_t N = 1024>
class LockfreeQueue
    : public boost::lockfree::spsc_queue<T, boost::lockfree::capacity<N>> {
public:
  using element_type = T;
  using base_type =
      boost::lockfree::spsc_queue<T, boost::lockfree::capacity<N>>;

  void push(const T &value) {
    while (base_type::write_available() == 0) {
      nop_pause();
    }
    bool ok = base_type::push(value);
    CHECK(ok);
  }

  void wait_till_non_empty() {
    while (base_type::empty()) {
      nop_pause();
    }
  }

  auto capacity() { return N; }

private:
  void nop_pause() { __asm volatile("pause" : :); }
};
} // namespace aria
