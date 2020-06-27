//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#include <atomic>

#include "glog/logging.h"

namespace aria {

class PwvHelper {
public:
  using MetaDataType = std::atomic<uint64_t>;
  static void read(const std::tuple<MetaDataType *, void *> &row, void *dest,
                   std::size_t size) {
    void *src = std::get<1>(row);
    std::memcpy(dest, src, size);
  }
};
} // namespace aria