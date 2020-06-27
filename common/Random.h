//
// Created by Yi Lu on 7/14/18.
//

#pragma once

#include <string>

namespace aria {

class Random {
public:
  Random(uint64_t seed = 0) { init_seed(seed); }

  void init_seed(uint64_t seed) {
    seed_ = (seed ^ 0x5DEECE66DULL) & ((1ULL << 48) - 1);
  }

  void set_seed(uint64_t seed) { seed_ = seed; }

  uint64_t get_seed() { return seed_; }

  uint64_t next() { return ((uint64_t)next(32) << 32) + next(32); }

  uint64_t next(unsigned int bits) {
    seed_ = (seed_ * 0x5DEECE66DULL + 0xBULL) & ((1ULL << 48) - 1);
    return (seed_ >> (48 - bits));
  }

  /* [0.0, 1.0) */
  double next_double() {
    return (((uint64_t)next(26) << 27) + next(27)) / (double)(1ULL << 53);
  }

  uint64_t uniform_dist(uint64_t a, uint64_t b) {
    if (a == b)
      return a;
    return next() % (b - a + 1) + a;
  }

  std::string rand_str(std::size_t length, const std::string &str) {
    std::string result;
    auto str_len = str.length();
    for (auto i = 0u; i < length; i++) {
      int k = uniform_dist(0, str_len - 1);
      result += str[k];
    }
    return result;
  }

  std::string a_string(std::size_t min_len, std::size_t max_len) {
    auto len = uniform_dist(min_len, max_len);
    return rand_str(len, alpha());
  }

private:
  static const std::string &alpha() {
    static std::string alpha_ =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    return alpha_;
  };

  uint64_t seed_;
};
} // namespace aria
