//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "benchmark/tpcc/Schema.h"

namespace aria {
namespace tpcc {
struct Storage {
  warehouse::key warehouse_key;
  warehouse::value warehouse_value;

  district::key district_key;
  district::value district_value;

  customer_name_idx::key customer_name_idx_key;
  customer_name_idx::value customer_name_idx_value;

  customer::key customer_key;
  customer::value customer_value;

  item::key item_keys[15];
  item::value item_values[15];

  stock::key stock_keys[15];
  stock::value stock_values[15];

  new_order::key new_order_key;

  order::key order_key;
  order::value order_value;

  order_line::key order_line_keys[15];
  order_line::value order_line_values[15];

  history::key h_key;
  history::value h_value;
};
} // namespace tpcc
} // namespace aria