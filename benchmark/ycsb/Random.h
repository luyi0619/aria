//
// Created by Yi Lu on 7/14/18.
//

#pragma once

#include <string>
#include <vector>

#include "common/Random.h"

namespace aria {
namespace ycsb {
class Random : public aria::Random {
public:
  using aria::Random::Random;

  std::string rand_str(std::size_t length) {
    auto &characters_ = characters();
    auto characters_len = characters_.length();
    std::string result;
    for (auto i = 0u; i < length; i++) {
      int k = uniform_dist(0, characters_len - 1);
      result += characters_[k];
    }
    return result;
  }

private:
  static const std::string &characters() {
    static std::string characters_ =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    return characters_;
  };
};
} // namespace ycsb
} // namespace aria
