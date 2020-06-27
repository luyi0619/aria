//
// Created by Yi Lu on 7/18/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Schema.h"
#include "benchmark/tpcc/Storage.h"
#include "common/Operation.h"
#include "common/Time.h"
#include "core/Partitioner.h"
#include "core/Table.h"

#include <glog/logging.h>

namespace aria {
namespace tpcc {

class Database {
public:
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;

  ITable *find_table(std::size_t table_id, std::size_t partition_id) {
    DCHECK(table_id < tbl_vecs.size());
    DCHECK(partition_id < tbl_vecs[table_id].size());
    return tbl_vecs[table_id][partition_id];
  }

  ITable *tbl_warehouse(std::size_t partition_id) {
    DCHECK(partition_id < tbl_warehouse_vec.size());
    return tbl_warehouse_vec[partition_id].get();
  }

  ITable *tbl_district(std::size_t partition_id) {
    DCHECK(partition_id < tbl_district_vec.size());
    return tbl_district_vec[partition_id].get();
  }

  ITable *tbl_customer(std::size_t partition_id) {
    DCHECK(partition_id < tbl_customer_vec.size());
    return tbl_customer_vec[partition_id].get();
  }

  ITable *tbl_customer_name_idx(std::size_t partition_id) {
    DCHECK(partition_id < tbl_customer_name_idx_vec.size());
    return tbl_customer_name_idx_vec[partition_id].get();
  }

  ITable *tbl_history(std::size_t partition_id) {
    DCHECK(partition_id < tbl_history_vec.size());
    return tbl_history_vec[partition_id].get();
  }

  ITable *tbl_new_order(std::size_t partition_id) {
    DCHECK(partition_id < tbl_new_order_vec.size());
    return tbl_new_order_vec[partition_id].get();
  }

  ITable *tbl_order(std::size_t partition_id) {
    DCHECK(partition_id < tbl_order_vec.size());
    return tbl_order_vec[partition_id].get();
  }

  ITable *tbl_order_line(std::size_t partition_id) {
    DCHECK(partition_id < tbl_order_line_vec.size());
    return tbl_order_line_vec[partition_id].get();
  }

  ITable *tbl_item(std::size_t partition_id) {
    DCHECK(partition_id < tbl_item_vec.size());
    return tbl_item_vec[partition_id].get();
  }

  ITable *tbl_stock(std::size_t partition_id) {
    DCHECK(partition_id < tbl_stock_vec.size());
    return tbl_stock_vec[partition_id].get();
  }

  template <class InitFunc>
  void initTables(const std::string &name, InitFunc initFunc,
                  std::size_t partitionNum, std::size_t threadsNum,
                  Partitioner *partitioner) {

    std::vector<int> all_parts;

    for (auto i = 0u; i < partitionNum; i++) {
      if (partitioner == nullptr ||
          partitioner->is_partition_replicated_on_me(i)) {
        all_parts.push_back(i);
      }
    }

    std::vector<std::thread> v;
    auto now = std::chrono::steady_clock::now();
    for (auto threadID = 0u; threadID < threadsNum; threadID++) {
      v.emplace_back([=]() {
        for (auto i = threadID; i < all_parts.size(); i += threadsNum) {
          auto partitionID = all_parts[i];
          initFunc(partitionID);
        }
      });
    }
    for (auto &t : v) {
      t.join();
    }
    LOG(INFO) << name << " initialization finished in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
  }

  void initialize(const Context &context) {

    std::size_t coordinator_id = context.coordinator_id;
    std::size_t partitionNum = context.partition_num;
    std::size_t threadsNum = context.worker_num;

    auto partitioner = PartitionerFactory::create_partitioner(
        context.partitioner, coordinator_id, context.coordinator_num);

    auto now = std::chrono::steady_clock::now();

    LOG(INFO) << "creating hash tables for database...";

    for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
      auto warehouseTableID = warehouse::tableID;
      tbl_warehouse_vec.push_back(
          TableFactory::create_table<997, warehouse::key, warehouse::value>(
              context, warehouseTableID, partitionID));

      auto districtTableID = district::tableID;
      tbl_district_vec.push_back(
          TableFactory::create_table<997, district::key, district::value>(
              context, districtTableID, partitionID));

      auto customerTableID = customer::tableID;
      tbl_customer_vec.push_back(
          TableFactory::create_table<997, customer::key, customer::value>(
              context, customerTableID, partitionID));

      auto customerNameIdxTableID = customer_name_idx::tableID;
      tbl_customer_name_idx_vec.push_back(
          TableFactory::create_table<997, customer_name_idx::key,
                                     customer_name_idx::value>(
              context, customerNameIdxTableID, partitionID));

      auto historyTableID = history::tableID;
      tbl_history_vec.push_back(
          TableFactory::create_table<997, history::key, history::value>(
              context, historyTableID, partitionID));

      auto newOrderTableID = new_order::tableID;
      tbl_new_order_vec.push_back(
          TableFactory::create_table<997, new_order::key, new_order::value>(
              context, newOrderTableID, partitionID));

      auto orderTableID = order::tableID;
      tbl_order_vec.push_back(
          TableFactory::create_table<997, order::key, order::value>(
              context, orderTableID, partitionID));

      auto orderLineTableID = order_line::tableID;
      tbl_order_line_vec.push_back(
          TableFactory::create_table<997, order_line::key, order_line::value>(
              context, orderLineTableID, partitionID));

      auto stockTableID = stock::tableID;
      tbl_stock_vec.push_back(
          TableFactory::create_table<997, stock::key, stock::value>(
              context, stockTableID, partitionID));
    }
    auto itemTableID = item::tableID;
    tbl_item_vec.push_back(
        std::make_unique<Table<997, item::key, item::value>>(itemTableID, 0));

    // there are 10 tables in tpcc
    tbl_vecs.resize(10);

    auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

    std::transform(tbl_warehouse_vec.begin(), tbl_warehouse_vec.end(),
                   std::back_inserter(tbl_vecs[0]), tFunc);
    std::transform(tbl_district_vec.begin(), tbl_district_vec.end(),
                   std::back_inserter(tbl_vecs[1]), tFunc);
    std::transform(tbl_customer_vec.begin(), tbl_customer_vec.end(),
                   std::back_inserter(tbl_vecs[2]), tFunc);
    std::transform(tbl_customer_name_idx_vec.begin(),
                   tbl_customer_name_idx_vec.end(),
                   std::back_inserter(tbl_vecs[3]), tFunc);
    std::transform(tbl_history_vec.begin(), tbl_history_vec.end(),
                   std::back_inserter(tbl_vecs[4]), tFunc);
    std::transform(tbl_new_order_vec.begin(), tbl_new_order_vec.end(),
                   std::back_inserter(tbl_vecs[5]), tFunc);
    std::transform(tbl_order_vec.begin(), tbl_order_vec.end(),
                   std::back_inserter(tbl_vecs[6]), tFunc);
    std::transform(tbl_order_line_vec.begin(), tbl_order_line_vec.end(),
                   std::back_inserter(tbl_vecs[7]), tFunc);
    std::transform(tbl_item_vec.begin(), tbl_item_vec.end(),
                   std::back_inserter(tbl_vecs[8]), tFunc);
    std::transform(tbl_stock_vec.begin(), tbl_stock_vec.end(),
                   std::back_inserter(tbl_vecs[9]), tFunc);

    DLOG(INFO) << "hash tables created in "
               << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - now)
                      .count()
               << " milliseconds.";

    using std::placeholders::_1;
    initTables(
        "warehouse",
        [&context, this](std::size_t partitionID) {
          warehouseInit(context, partitionID);
        },
        partitionNum, threadsNum, partitioner.get());
    initTables(
        "district",
        [&context, this](std::size_t partitionID) {
          districtInit(context, partitionID);
        },
        partitionNum, threadsNum, partitioner.get());
    initTables(
        "customer",
        [&context, this](std::size_t partitionID) {
          customerInit(context, partitionID);
        },
        partitionNum, threadsNum, partitioner.get());
    initTables(
        "customer_name_idx",
        [&context, this](std::size_t partitionID) {
          customerNameIdxInit(context, partitionID);
        },
        partitionNum, threadsNum, partitioner.get());
    /*    initTables("history",
                   [&context, this](std::size_t partitionID) {
                     historyInit(context, partitionID);
                   },
                   partitionNum, threadsNum, partitioner.get());
        initTables("new_order",
                   [&context, this](std::size_t partitionID) {
                     newOrderInit(context, partitionID);
                   },
                   partitionNum, threadsNum, partitioner.get());
        initTables("order",
                   [&context, this](std::size_t partitionID) {
                     orderInit(context, partitionID);
                   },
                   partitionNum, threadsNum, partitioner.get());
        initTables("order_line",
                   [&context, this](std::size_t partitionID) {
                     orderLineInit(context, partitionID);
                   },
                   partitionNum, threadsNum, partitioner.get());*/
    initTables(
        "item",
        [&context, this](std::size_t partitionID) {
          itemInit(context, partitionID);
        },
        1, 1, nullptr);
    initTables(
        "stock",
        [&context, this](std::size_t partitionID) {
          stockInit(context, partitionID);
        },
        partitionNum, threadsNum, partitioner.get());
  }

  void apply_operation(const Operation &operation) {

    Decoder dec(operation.data);
    bool is_neworder;
    dec >> is_neworder;

    if (is_neworder) {
      // district
      auto districtTableID = district::tableID;
      district::key district_key;
      dec >> district_key.D_W_ID >> district_key.D_ID;

      auto row =
          tbl_district_vec[district_key.D_W_ID - 1]->search(&district_key);
      MetaDataType &tid = *std::get<0>(row);
      tid.store(operation.tid);
      district::value &district_value =
          *static_cast<district::value *>(std::get<1>(row));
      dec >> district_value.D_NEXT_O_ID;

      // stock
      auto stockTableID = stock::tableID;
      while (dec.size() > 0) {
        stock::key stock_key;
        dec >> stock_key.S_W_ID >> stock_key.S_I_ID;

        auto row = tbl_stock_vec[stock_key.S_W_ID - 1]->search(&stock_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);
        stock::value &stock_value =
            *static_cast<stock::value *>(std::get<1>(row));

        dec >> stock_value.S_QUANTITY >> stock_value.S_YTD >>
            stock_value.S_ORDER_CNT >> stock_value.S_REMOTE_CNT;
      }
    } else {
      {
        // warehouse
        auto warehouseTableID = warehouse::tableID;
        warehouse::key warehouse_key;
        dec >> warehouse_key.W_ID;

        auto row =
            tbl_warehouse_vec[warehouse_key.W_ID - 1]->search(&warehouse_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);

        warehouse::value &warehouse_value =
            *static_cast<warehouse::value *>(std::get<1>(row));

        dec >> warehouse_value.W_YTD;
      }

      {
        // district
        auto districtTableID = district::tableID;
        district::key district_key;
        dec >> district_key.D_W_ID >> district_key.D_ID;

        auto row =
            tbl_district_vec[district_key.D_W_ID - 1]->search(&district_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);

        district::value &district_value =
            *static_cast<district::value *>(std::get<1>(row));

        dec >> district_value.D_YTD;
      }

      {
        // custoemer
        auto customerTableID = customer::tableID;
        customer::key customer_key;
        dec >> customer_key.C_W_ID >> customer_key.C_D_ID >> customer_key.C_ID;

        auto row =
            tbl_customer_vec[customer_key.C_W_ID - 1]->search(&customer_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);

        customer::value &customer_value =
            *static_cast<customer::value *>(std::get<1>(row));

        char C_DATA[501];
        const char *old_C_DATA = customer_value.C_DATA.c_str();
        uint32_t total_written;
        dec >> total_written;
        dec.read_n_bytes(C_DATA, total_written);
        std::memcpy(C_DATA + total_written, old_C_DATA, 500 - total_written);
        C_DATA[500] = 0;
        customer_value.C_DATA.assign(C_DATA);

        dec >> customer_value.C_BALANCE >> customer_value.C_YTD_PAYMENT >>
            customer_value.C_PAYMENT_CNT;
      }
    }
  }

private:
  void warehouseInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_warehouse_vec[partitionID].get();

    warehouse::key key;
    key.W_ID = partitionID + 1; // partitionID is from 0, W_ID is from 1

    warehouse::value value;
    value.W_NAME.assign(random.a_string(6, 10));
    value.W_STREET_1.assign(random.a_string(10, 20));
    value.W_STREET_2.assign(random.a_string(10, 20));
    value.W_CITY.assign(random.a_string(10, 20));
    value.W_STATE.assign(random.a_string(2, 2));
    value.W_ZIP.assign(random.rand_zip());
    value.W_TAX = static_cast<float>(random.uniform_dist(0, 2000)) / 10000;
    value.W_YTD = 30000;

    table->insert(&key, &value);
  }

  void districtInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_district_vec[partitionID].get();

    // For each row in the WAREHOUSE table, context.n_district rows in the
    // DISTRICT table

    for (int i = 1; i <= context.n_district; i++) {

      district::key key;
      key.D_W_ID = partitionID + 1;
      key.D_ID = i;

      district::value value;
      value.D_NAME.assign(random.a_string(6, 10));
      value.D_STREET_1.assign(random.a_string(10, 20));
      value.D_STREET_2.assign(random.a_string(10, 20));
      value.D_CITY.assign(random.a_string(10, 20));
      value.D_STATE.assign(random.a_string(2, 2));
      value.D_ZIP.assign(random.rand_zip());
      value.D_TAX = static_cast<float>(random.uniform_dist(0, 2000)) / 10000;
      value.D_YTD = 30000;
      value.D_NEXT_O_ID = 3001;

      table->insert(&key, &value);
    }
  }

  void customerInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_customer_vec[partitionID].get();

    // For each row in the WAREHOUSE table, context.n_district rows in the
    // DISTRICT table For each row in the DISTRICT table, 3,000 rows in the
    // CUSTOMER table

    for (int i = 1; i <= context.n_district; i++) {
      for (int j = 1; j <= 3000; j++) {

        customer::key key;
        key.C_W_ID = partitionID + 1;
        key.C_D_ID = i;
        key.C_ID = j;

        customer::value value;
        value.C_MIDDLE.assign("OE");
        value.C_FIRST.assign(random.a_string(8, 16));
        value.C_STREET_1.assign(random.a_string(10, 20));
        value.C_STREET_2.assign(random.a_string(10, 20));
        value.C_CITY.assign(random.a_string(10, 20));
        value.C_STATE.assign(random.a_string(2, 2));
        value.C_ZIP.assign(random.rand_zip());
        value.C_PHONE.assign(random.n_string(16, 16));
        value.C_SINCE = Time::now();
        value.C_CREDIT_LIM = 50000;
        value.C_DISCOUNT =
            static_cast<float>(random.uniform_dist(0, 5000)) / 10000;
        value.C_BALANCE = -10;
        value.C_YTD_PAYMENT = 10;
        value.C_PAYMENT_CNT = 1;
        value.C_DELIVERY_CNT = 1;
        value.C_DATA.assign(random.a_string(300, 500));

        int last_name;

        if (j <= 1000) {
          last_name = j - 1;
        } else {
          last_name = random.non_uniform_distribution(255, 0, 999);
        }

        value.C_LAST.assign(random.rand_last_name(last_name));

        // For 10% of the rows, selected at random , C_CREDIT = "BC"

        int x = random.uniform_dist(1, 10);

        if (x == 1) {
          value.C_CREDIT.assign("BC");
        } else {
          value.C_CREDIT.assign("GC");
        }

        table->insert(&key, &value);
      }
    }
  }

  void customerNameIdxInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_customer_name_idx_vec[partitionID].get();

    // For each row in the WAREHOUSE table, context.n_district rows in the
    // DISTRICT table For each row in the DISTRICT table, 3,000 rows in the
    // CUSTOMER table

    ITable *customer_table = find_table(customer::tableID, partitionID);

    std::unordered_map<FixedString<16>,
                       std::vector<std::tuple<FixedString<16>, int32_t>>>
        last_name_to_first_names_and_c_ids;

    for (int i = 1; i <= context.n_district; i++) {
      for (int j = 1; j <= 3000; j++) {
        customer::key customer_key;
        customer_key.C_W_ID = partitionID + 1;
        customer_key.C_D_ID = i;
        customer_key.C_ID = j;

        // no concurrent write, it is ok to read without validation on
        // MetaDataType
        const customer::value &customer_value = *static_cast<customer::value *>(
            customer_table->search_value(&customer_key));
        last_name_to_first_names_and_c_ids[customer_value.C_LAST].push_back(
            std::make_tuple(customer_value.C_FIRST, customer_key.C_ID));
      }

      for (auto it = last_name_to_first_names_and_c_ids.begin();
           it != last_name_to_first_names_and_c_ids.end(); it++) {
        auto &v = std::get<1>(*it);
        std::sort(v.begin(), v.end());

        // insert ceiling(n/2) to customer_last_name_idx, n starts from 1
        customer_name_idx::key cni_key(partitionID + 1, i, std::get<0>(*it));
        customer_name_idx::value cni_value(std::get<1>(v[(v.size() - 1) / 2]));
        CHECK(cni_value.C_ID > 0) << "C_ID is not valid.";
        table->insert(&cni_key, &cni_value);
      }
    }
  }

  void historyInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_history_vec[partitionID].get();

    // For each row in the WAREHOUSE table, context.n_district rows in the
    // DISTRICT table For each row in the DISTRICT table, 3,000 rows in the
    // CUSTOMER table For each row in the CUSTOMER table, 1 row in the HISTORY
    // table

    for (int i = 1; i <= context.n_district; i++) {
      for (int j = 1; j <= 3000; j++) {

        history::key key;

        key.H_W_ID = partitionID + 1;
        key.H_D_ID = i;
        key.H_C_W_ID = partitionID + 1;
        key.H_C_D_ID = i;
        key.H_C_ID = j;
        key.H_DATE = Time::now();

        history::value value;
        value.H_AMOUNT = 10;
        value.H_DATA.assign(random.a_string(12, 24));

        table->insert(&key, &value);
      }
    }
  }

  void newOrderInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_new_order_vec[partitionID].get();

    // For each row in the WAREHOUSE table, context.n_district rows in the
    // DISTRICT table For each row in the DISTRICT table, 3,000 rows in the
    // ORDER table For each row in the ORDER table from 2101 to 3000, 1 row in
    // the NEW_ORDER table

    for (int i = 1; i <= context.n_district; i++) {
      for (int j = 2101; j <= 3000; j++) {

        new_order::key key;
        key.NO_W_ID = partitionID + 1;
        key.NO_D_ID = i;
        key.NO_O_ID = j;

        new_order::value value;

        table->insert(&key, &value);
      }
    }
  }

  void orderInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_order_vec[partitionID].get();

    // For each row in the WAREHOUSE table, context.n_district rows in the
    // DISTRICT table For each row in the DISTRICT table, 3,000 rows in the
    // ORDER table

    std::vector<int> perm;

    for (int i = 1; i <= 3000; i++) {
      perm.push_back(i);
    }

    for (int i = 1; i <= context.n_district; i++) {

      std::shuffle(perm.begin(), perm.end(), std::default_random_engine());

      for (int j = 1; j <= 3000; j++) {

        order::key key;
        key.O_W_ID = partitionID + 1;
        key.O_D_ID = i;
        key.O_ID = j;

        order::value value;
        value.O_C_ID = perm[j - 1];
        value.O_ENTRY_D = Time::now();
        value.O_OL_CNT = random.uniform_dist(5, 15);
        value.O_ALL_LOCAL = true;

        if (key.O_ID < 2101) {
          value.O_CARRIER_ID = random.uniform_dist(1, 10);
        } else {
          value.O_CARRIER_ID = 0;
        }

        table->insert(&key, &value);
      }
    }
  }

  void orderLineInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_order_line_vec[partitionID].get();

    // For each row in the WAREHOUSE table, context.n_district rows in the
    // DISTRICT table For each row in the DISTRICT table, 3,000 rows in the
    // ORDER table For each row in the ORDER table, O_OL_CNT rows in the
    // ORDER_LINE table

    ITable *order_table = find_table(order::tableID, partitionID);

    for (int i = 1; i <= context.n_district; i++) {

      order::key order_key;
      order_key.O_W_ID = partitionID + 1;
      order_key.O_D_ID = i;

      for (int j = 1; j <= 3000; j++) {
        order_key.O_ID = j;

        // no concurrent write, it is ok to read without validation on
        // MetaDataType
        const order::value &order_value =
            *static_cast<order::value *>(order_table->search_value(&order_key));

        for (int k = 1; k <= order_value.O_OL_CNT; k++) {
          order_line::key key;
          key.OL_W_ID = partitionID + 1;
          key.OL_D_ID = i;
          key.OL_O_ID = j;
          key.OL_NUMBER = k;

          order_line::value value;
          value.OL_I_ID = random.uniform_dist(1, 100000);
          value.OL_SUPPLY_W_ID = partitionID + 1;
          value.OL_QUANTITY = 5;
          value.OL_DIST_INFO.assign(random.a_string(24, 24));

          if (key.OL_O_ID < 2101) {
            value.OL_DELIVERY_D = order_value.O_ENTRY_D;
            value.OL_AMOUNT = 0;
          } else {
            value.OL_DELIVERY_D = 0;
            value.OL_AMOUNT =
                static_cast<float>(random.uniform_dist(1, 999999)) / 100;
          }
          table->insert(&key, &value);
        }
      }
    }
  }

  void itemInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_item_vec[partitionID].get();

    std::string i_original = "ORIGINAL";

    // 100,000 rows in the ITEM table

    for (int i = 1; i <= 100000; i++) {

      item::key key;
      key.I_ID = i;

      item::value value;
      value.I_IM_ID = random.uniform_dist(1, 10000);
      value.I_NAME.assign(random.a_string(14, 24));
      value.I_PRICE = random.uniform_dist(1, 100);

      std::string i_data = random.a_string(26, 50);

      /*
          For 10% of the rows, selected at random,
          the string "ORIGINAL" must be held by 8 consecutive characters
         starting at a random position within I_DATA
      */

      int x = random.uniform_dist(1, 10);

      if (x == 1) {
        int pos = random.uniform_dist(0, i_data.length() - i_original.length());
        memcpy(&i_data[0] + pos, &i_original[0], i_original.length());
      }

      value.I_DATA.assign(i_data);

      table->insert(&key, &value);
    }
  }

  void stockInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_stock_vec[partitionID].get();

    std::string s_original = "ORIGINAL";

    // For each row in the WAREHOUSE table, 100,000 rows in the STOCK table

    for (int i = 1; i <= 100000; i++) {

      stock::key key;
      key.S_W_ID = partitionID + 1; // partition_id from 0, W_ID from 1
      key.S_I_ID = i;

      stock::value value;

      value.S_QUANTITY = random.uniform_dist(10, 100);
      value.S_DIST_01.assign(random.a_string(24, 24));
      value.S_DIST_02.assign(random.a_string(24, 24));
      value.S_DIST_03.assign(random.a_string(24, 24));
      value.S_DIST_04.assign(random.a_string(24, 24));
      value.S_DIST_05.assign(random.a_string(24, 24));
      value.S_DIST_06.assign(random.a_string(24, 24));
      value.S_DIST_07.assign(random.a_string(24, 24));
      value.S_DIST_08.assign(random.a_string(24, 24));
      value.S_DIST_09.assign(random.a_string(24, 24));
      value.S_DIST_10.assign(random.a_string(24, 24));
      value.S_YTD = 0;
      value.S_ORDER_CNT = 0;
      value.S_REMOTE_CNT = 0;

      /*
       For 10% of the rows, selected at random,
       the string "ORIGINAL" must be held by 8 consecutive characters starting
       at a random position within S_DATA
       */

      std::string s_data = random.a_string(26, 40);

      int x = random.uniform_dist(1, 10);

      if (x == 1) {
        int pos = random.uniform_dist(0, s_data.length() - s_original.length());
        memcpy(&s_data[0] + pos, &s_original[0], s_original.length());
      }

      value.S_DATA.assign(s_data);

      table->insert(&key, &value);
    }
  }

private:
  std::vector<std::vector<ITable *>> tbl_vecs;

  std::vector<std::unique_ptr<ITable>> tbl_warehouse_vec;
  std::vector<std::unique_ptr<ITable>> tbl_district_vec;
  std::vector<std::unique_ptr<ITable>> tbl_customer_vec;
  std::vector<std::unique_ptr<ITable>> tbl_customer_name_idx_vec;
  std::vector<std::unique_ptr<ITable>> tbl_history_vec;
  std::vector<std::unique_ptr<ITable>> tbl_new_order_vec;
  std::vector<std::unique_ptr<ITable>> tbl_order_vec;
  std::vector<std::unique_ptr<ITable>> tbl_order_line_vec;
  std::vector<std::unique_ptr<ITable>> tbl_item_vec;
  std::vector<std::unique_ptr<ITable>> tbl_stock_vec;
};
} // namespace tpcc
} // namespace aria
