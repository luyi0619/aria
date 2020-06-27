//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include <chrono>

namespace aria {

class Time {
public:
  static uint64_t now() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now - startTime)
        .count();
  }

  static std::chrono::steady_clock::time_point startTime;
};

} // namespace aria
