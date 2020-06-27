//
// Created by Yi Lu on 8/7/19.
//

#pragma once

#include <chrono>
#include <iostream>

class FastSleep {

public:
  // in microseconds
  static int64_t sleep_for(int64_t length) {

    std::chrono::steady_clock::time_point start, end;
    start = std::chrono::steady_clock::now();
    std::chrono::steady_clock::duration d;

    // use microseconds for accuracy.
    int64_t nano_length = length * 1000;

    do {
      nop();
      end = std::chrono::steady_clock::now();
      d = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    } while (d.count() < nano_length);

    return d.count() / 1000;
  }

private:
  static void nop() { asm("nop"); }
};
