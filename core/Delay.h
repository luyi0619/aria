//
// Created by Yi Lu on 9/28/18.
//

#pragma once

#include <glog/logging.h>

namespace aria {

// delay in us from the sender side

class Delay {

public:
  Delay(std::size_t coordinator_id, std::size_t coordinator_num) {
    DCHECK(coordinator_id < coordinator_num);
    this->coordinator_id = coordinator_id;
    this->coordinator_num = coordinator_num;
  }

  virtual ~Delay() = default;

  virtual int64_t message_delay() const = 0;

  virtual bool delay_enabled() const = 0;

protected:
  std::size_t coordinator_id;
  std::size_t coordinator_num;
};

class SameDelay : public Delay {

public:
  SameDelay(std::size_t coordinator_id, std::size_t coordinator_num,
            int64_t delay_time)
      : Delay(coordinator_id, coordinator_num), delay_time(delay_time) {
    DCHECK(delay_time >= 0);
  }

  virtual ~SameDelay() = default;

  int64_t message_delay() const override { return delay_time; }

  bool delay_enabled() const override { return delay_time != 0; }

protected:
  int64_t delay_time;
};

} // namespace aria