//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "benchmark/ycsb/Schema.h"

namespace aria {

namespace ycsb {
struct Storage {
  ycsb::key ycsb_keys[YCSB_FIELD_SIZE];
  ycsb::value ycsb_values[YCSB_FIELD_SIZE];
};

} // namespace ycsb
} // namespace aria