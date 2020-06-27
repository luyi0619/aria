//
// Created by Yi Lu on 7/14/18.
//

#pragma once

#include <string>
#include <vector>

#include "common/Random.h"

namespace aria {
namespace tpcc {
class Random : public aria::Random {
public:
  using aria::Random::Random;

  uint64_t non_uniform_distribution(uint64_t A, uint64_t x, uint64_t y) {
    return (uniform_dist(0, A) | uniform_dist(x, y)) % (y - x + 1) + x;
  }

  std::string n_string(std::size_t min_len, std::size_t max_len) {
    auto len = uniform_dist(min_len, max_len);
    return rand_str(len, numeric());
  }

  std::string rand_zip() {
    auto zip = n_string(4, 4);
    // append "11111"
    for (int i = 0; i < 5; i++) {
      zip += '1';
    }
    return zip;
  }

  std::string rand_last_name(int n) {
    const auto &last_names = customer_last_names();
    const auto &s1 = last_names[n / 100];
    const auto &s2 = last_names[n / 10 % 10];
    const auto &s3 = last_names[n % 10];
    return s1 + s2 + s3;
  }

private:
  static const std::vector<std::string> &customer_last_names() {
    static std::vector<std::string> last_names = {
        "BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
        "ESE", "ANTI",  "CALLY", "ATION", "EING"};
    return last_names;
  }

  static const std::string &numeric() {
    static std::string numeric_ = "0123456789";
    return numeric_;
  };
};
} // namespace tpcc
} // namespace aria
