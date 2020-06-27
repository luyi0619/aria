//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Query.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Storage.h"
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Query.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Storage.h"
#include "protocol/Pwv/PwvHelper.h"
#include "protocol/Pwv/PwvRWKey.h"

namespace aria {

class PwvStatement {
public:
  virtual ~PwvStatement() = default;

  virtual void prepare_read_and_write_set() = 0;

  virtual std::size_t piece_partition_id() = 0;

  virtual void execute() = 0;

public:
  std::vector<PwvRWKey> readSet, writeSet;
};

class PwvYCSBStatement : public PwvStatement {
public:
  static constexpr std::size_t keys_num = 10;

  PwvYCSBStatement(ycsb::Database &db, const ycsb::Context &context,
                   ycsb::Random &random, ycsb::Storage &storage,
                   std::size_t partition_id,
                   const ycsb::YCSBQuery<keys_num> &query, int idx)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query), idx(idx) {}

  ~PwvYCSBStatement() override = default;

  void prepare_read_and_write_set() override {
    int ycsbTableID = ycsb::ycsb::tableID;

    auto key = query.Y_KEY[idx];
    storage.ycsb_keys[idx].Y_KEY = key;
    PwvRWKey rwkey;
    rwkey.set_table_id(ycsbTableID);
    rwkey.set_partition_id(context.getPartitionID(key));
    rwkey.set_key(&storage.ycsb_keys[idx]);
    rwkey.set_value(&storage.ycsb_values[idx]);

    readSet.push_back(rwkey);
    if (query.UPDATE[idx]) {
      writeSet.push_back(rwkey);
    }

    CHECK(readSet.size() == 1) << "Check on readSet size failed!";
    CHECK(query.UPDATE[idx] ? writeSet.size() == 1 : writeSet.size() == 0)
        << "Check on writeSet size failed!";
  }

  std::size_t piece_partition_id() override {
    auto key = query.Y_KEY[idx];
    return context.getPartitionID(key);
  }

  void execute() override {

    int ycsbTableID = ycsb::ycsb::tableID;

    // read
    auto tableId = ycsbTableID;
    auto partitionId = context.getPartitionID(storage.ycsb_keys[idx].Y_KEY);
    auto key = &storage.ycsb_keys[idx];
    auto value = &storage.ycsb_values[idx];
    ITable *table = db.find_table(tableId, partitionId);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    PwvHelper::read(row, value, value_bytes);

    // compute
    if (query.UPDATE[idx]) {
      ycsb::Random local_random;
      storage.ycsb_values[idx].Y_F01.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F02.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F03.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F04.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F05.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F06.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F07.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F08.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F09.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F10.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
    }

    // write
    if (query.UPDATE[idx]) {
      table->update(key, value);
    }
  }

public:
  ycsb::Database &db;
  const ycsb::Context &context;
  ycsb::Random &random;
  ycsb::Storage &storage;
  std::size_t partition_id;
  const ycsb::YCSBQuery<keys_num> &query;
  int idx;
};

class PwvYCSBStarStatement : public PwvStatement {
public:
  static constexpr std::size_t keys_num = 10;

  PwvYCSBStarStatement(ycsb::Database &db, const ycsb::Context &context,
                       ycsb::Random &random, ycsb::Storage &storage,
                       std::size_t partition_id,
                       const ycsb::YCSBQuery<keys_num> &query, int idx,
                       bool read, std::atomic<int> &commit_rvp)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query), idx(idx), read(read),
        commit_rvp(commit_rvp) {}

  ~PwvYCSBStarStatement() override = default;

  void prepare_read_and_write_set() override {
    int ycsbTableID = ycsb::ycsb::tableID;

    auto key = query.Y_KEY[idx];
    storage.ycsb_keys[idx].Y_KEY = key;
    PwvRWKey rwkey;
    rwkey.set_table_id(ycsbTableID);
    rwkey.set_partition_id(context.getPartitionID(key));
    rwkey.set_key(&storage.ycsb_keys[idx]);
    rwkey.set_value(&storage.ycsb_values[idx]);

    readSet.push_back(rwkey);
    if (query.UPDATE[idx]) {
      writeSet.push_back(rwkey);
    }

    CHECK(readSet.size() == 1) << "Check on readSet size failed!";
    CHECK(query.UPDATE[idx] ? writeSet.size() == 1 : writeSet.size() == 0)
        << "Check on writeSet size failed!";
  }

  std::size_t piece_partition_id() override {
    auto key = query.Y_KEY[idx];
    return context.getPartitionID(key);
  }

  void execute() override {

    int ycsbTableID = ycsb::ycsb::tableID;

    auto tableId = ycsbTableID;
    auto partitionId = context.getPartitionID(storage.ycsb_keys[idx].Y_KEY);
    auto key = &storage.ycsb_keys[idx];
    auto value = &storage.ycsb_values[idx];
    ITable *table = db.find_table(tableId, partitionId);

    if (read) {
      auto value_bytes = table->value_size();
      auto row = table->search(key);
      PwvHelper::read(row, value, value_bytes);
      commit_rvp.fetch_add(-1);
    } else {
      // compute
      CHECK(query.UPDATE[idx]) << idx << " is not supposed to be updated.";

      ycsb::Random local_random;
      storage.ycsb_values[idx].Y_F01.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F02.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F03.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F04.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F05.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F06.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F07.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F08.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F09.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F10.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));

      // write
      table->update(key, value);
    }
  }

public:
  ycsb::Database &db;
  const ycsb::Context &context;
  ycsb::Random &random;
  ycsb::Storage &storage;
  std::size_t partition_id;
  const ycsb::YCSBQuery<keys_num> &query;
  int idx;
  bool read;
  std::atomic<int> &commit_rvp;
};

class PwvNewOrderWarehouseStatement : public PwvStatement {
public:
  PwvNewOrderWarehouseStatement(tpcc::Database &db,
                                const tpcc::Context &context,
                                tpcc::Random &random, tpcc::Storage &storage,
                                std::size_t partition_id,
                                const tpcc::NewOrderQuery &query)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query) {}

  ~PwvNewOrderWarehouseStatement() override = default;

  void prepare_read_and_write_set() override {

    int32_t W_ID = partition_id + 1;

    // The input data (see Clause 2.4.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;

    // The row in the WAREHOUSE table with matching W_ID is selected and W_TAX,
    // the warehouse tax rate, is retrieved.

    auto warehouseTableID = tpcc::warehouse::tableID;
    storage.warehouse_key = tpcc::warehouse::key(W_ID);
    {
      PwvRWKey rwkey;
      rwkey.set_table_id(warehouseTableID);
      rwkey.set_partition_id(W_ID - 1);
      rwkey.set_key(&storage.warehouse_key);
      rwkey.set_value(&storage.warehouse_value);
      readSet.push_back(rwkey);
    }
    // The row in the DISTRICT table with matching D_W_ID and D_ ID is selected,
    // D_TAX, the district tax rate, is retrieved, and D_NEXT_O_ID, the next
    // available order number for the district, is retrieved and incremented by
    // one.

    auto districtTableID = tpcc::district::tableID;
    storage.district_key = tpcc::district::key(W_ID, D_ID);
    {
      PwvRWKey rwkey;
      rwkey.set_table_id(districtTableID);
      rwkey.set_partition_id(W_ID - 1);
      rwkey.set_key(&storage.district_key);
      rwkey.set_value(&storage.district_value);

      readSet.push_back(rwkey);
      writeSet.push_back(rwkey);
    }

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerTableID = tpcc::customer::tableID;
    storage.customer_key = tpcc::customer::key(W_ID, D_ID, C_ID);
    {
      PwvRWKey rwkey;
      rwkey.set_table_id(customerTableID);
      rwkey.set_partition_id(W_ID - 1);
      rwkey.set_key(&storage.customer_key);
      rwkey.set_value(&storage.customer_value);
      readSet.push_back(rwkey);
    }
    CHECK(readSet.size() == 3) << "Check on readSet size failed!";
    CHECK(writeSet.size() == 1) << "Check on writeSet size failed!";
  }

  std::size_t piece_partition_id() override { return partition_id; }

  void execute() override {

    // The changes are committed in transaction.commit();

    int32_t D_NEXT_O_ID = storage.district_value.D_NEXT_O_ID;
    storage.district_value.D_NEXT_O_ID += 1;
  }

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::NewOrderQuery &query;
};

class PwvNewOrderStockStatement : public PwvStatement {
public:
  PwvNewOrderStockStatement(tpcc::Database &db, const tpcc::Context &context,
                            tpcc::Random &random, tpcc::Storage &storage,
                            std::size_t partition_id,
                            const tpcc::NewOrderQuery &query, int idx,
                            std::atomic<int> &commit_rvp)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query), idx(idx),
        commit_rvp(commit_rvp) {}

  ~PwvNewOrderStockStatement() override = default;

  void prepare_read_and_write_set() override {

    auto itemTableID = tpcc::item::tableID;
    auto stockTableID = tpcc::stock::tableID;

    // The row in the ITEM table with matching I_ID (equals OL_I_ID) is
    // selected and I_PRICE, the price of the item, I_NAME, the name of the
    // item, and I_DATA are retrieved. If I_ID has an unused value (see
    // Clause 2.4.1.5), a "not-found" condition is signaled, resulting in a
    // rollback of the database transaction (see Clause 2.4.2.3).

    int32_t OL_I_ID = query.INFO[idx].OL_I_ID;
    int8_t OL_QUANTITY = query.INFO[idx].OL_QUANTITY;
    int32_t OL_SUPPLY_W_ID = query.INFO[idx].OL_SUPPLY_W_ID;

    storage.item_keys[idx] = tpcc::item::key(OL_I_ID);

    // If I_ID has an unused value, rollback.
    // In OCC, rollback can return without going through commit protocol

    if (storage.item_keys[idx].I_ID == 0) {
      return; // abort
    }

    auto tableId = itemTableID;
    auto partitionId = 0;
    auto key = &storage.item_keys[idx];
    auto value = &storage.item_values[idx];
    ITable *table = db.find_table(tableId, partitionId);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    PwvHelper::read(row, value, value_bytes);

    // The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
    // S_W_ID (equals OL_SUPPLY_W_ID) is selected.

    storage.stock_keys[idx] = tpcc::stock::key(OL_SUPPLY_W_ID, OL_I_ID);

    PwvRWKey rwkey;
    rwkey.set_table_id(stockTableID);
    rwkey.set_partition_id(OL_SUPPLY_W_ID - 1);
    rwkey.set_key(&storage.stock_keys[idx]);
    rwkey.set_value(&storage.stock_values[idx]);
    readSet.push_back(rwkey);
    writeSet.push_back(rwkey);

    CHECK(readSet.size() == 1) << "Check on readSet size failed!";
    CHECK(writeSet.size() == 1) << "Check on writeSet size failed!";
  }

  std::size_t piece_partition_id() override {
    int32_t OL_SUPPLY_W_ID = query.INFO[idx].OL_SUPPLY_W_ID;
    return OL_SUPPLY_W_ID - 1;
  }

  void execute() override {

    int32_t W_ID = this->partition_id + 1;
    int32_t OL_I_ID = query.INFO[idx].OL_I_ID;
    int8_t OL_QUANTITY = query.INFO[idx].OL_QUANTITY;
    int32_t OL_SUPPLY_W_ID = query.INFO[idx].OL_SUPPLY_W_ID;
    float I_PRICE = storage.item_values[idx].I_PRICE;

    if (storage.item_keys[idx].I_ID == 0) {
      commit_rvp.fetch_add(-query.O_OL_CNT - 1);
      return;
    }

    // read
    auto tableId = tpcc::stock::tableID;
    auto partitionId = OL_SUPPLY_W_ID - 1;
    auto key = &storage.stock_keys[idx];
    auto value = &storage.stock_values[idx];
    ITable *table = db.find_table(tableId, partitionId);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    PwvHelper::read(row, value, value_bytes);

    // compute

    // S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the
    // district number, and S_DATA are retrieved. If the retrieved value for
    // S_QUANTITY exceeds OL_QUANTITY by 10 or more, then S_QUANTITY is
    // decreased by OL_QUANTITY; otherwise S_QUANTITY is updated to
    // (S_QUANTITY - OL_QUANTITY)+91. S_YTD is increased by OL_QUANTITY and
    // S_ORDER_CNT is incremented by 1. If the order-line is remote, then
    // S_REMOTE_CNT is incremented by 1.

    if (storage.stock_values[idx].S_QUANTITY >= OL_QUANTITY + 10) {
      storage.stock_values[idx].S_QUANTITY -= OL_QUANTITY;
    } else {
      storage.stock_values[idx].S_QUANTITY =
          storage.stock_values[idx].S_QUANTITY - OL_QUANTITY + 91;
    }

    storage.stock_values[idx].S_YTD += OL_QUANTITY;
    storage.stock_values[idx].S_ORDER_CNT++;

    if (OL_SUPPLY_W_ID != W_ID) {
      storage.stock_values[idx].S_REMOTE_CNT++;
    }
    commit_rvp.fetch_add(-1);
  }

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::NewOrderQuery &query;
  int idx;
  std::atomic<int> &commit_rvp;
};

class PwvNewOrderOrderStatement : public PwvStatement {
public:
  PwvNewOrderOrderStatement(tpcc::Database &db, const tpcc::Context &context,
                            tpcc::Random &random, tpcc::Storage &storage,
                            std::size_t partition_id,
                            const tpcc::NewOrderQuery &query,
                            float &total_amount)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query), total_amount(total_amount) {}

  ~PwvNewOrderOrderStatement() override = default;

  void prepare_read_and_write_set() override {
    // no read/write set currently
  }

  std::size_t piece_partition_id() override { return partition_id; }

  void execute() override {
    // just pure computation

    int32_t W_ID = this->partition_id + 1;
    // The input data (see Clause 2.4.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;

    int32_t D_NEXT_O_ID = storage.district_value.D_NEXT_O_ID;

    float W_TAX = storage.warehouse_value.W_YTD;
    float D_TAX = storage.district_value.D_TAX;
    float C_DISCOUNT = storage.customer_value.C_DISCOUNT;

    // write to district
    {
      auto tableId = tpcc::warehouse::tableID;
      auto partitionId = W_ID - 1;
      auto key = &storage.district_key;
      auto value = &storage.district_value;
      ITable *table = db.find_table(tableId, partitionId);
      table->update(key, value);
    }

    // write to stocks
    for (int i = 0; i < query.O_OL_CNT; i++) {

      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;
      auto tableId = tpcc::stock::tableID;
      auto partitionId = OL_SUPPLY_W_ID - 1;
      auto key = &storage.stock_keys[i];
      auto value = &storage.stock_values[i];
      ITable *table = db.find_table(tableId, partitionId);
      table->update(key, value);
    }

    // new order

    storage.new_order_key = tpcc::new_order::key(W_ID, D_ID, D_NEXT_O_ID);

    // order

    storage.order_key = tpcc::order::key(W_ID, D_ID, D_NEXT_O_ID);
    storage.order_value.O_ENTRY_D = Time::now();
    storage.order_value.O_CARRIER_ID = 0;
    storage.order_value.O_OL_CNT = query.O_OL_CNT;
    storage.order_value.O_C_ID = query.C_ID;
    storage.order_value.O_ALL_LOCAL = !query.isRemote();

    // orderline

    auto orderLineTableID = tpcc::order_line::tableID;

    for (int i = 0; i < query.O_OL_CNT; i++) {

      int32_t OL_I_ID = query.INFO[i].OL_I_ID;
      int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

      float I_PRICE = storage.item_values[i].I_PRICE;

      float OL_AMOUNT = I_PRICE * OL_QUANTITY;
      storage.order_line_keys[i] =
          tpcc::order_line::key(W_ID, D_ID, D_NEXT_O_ID, i + 1);

      storage.order_line_values[i].OL_I_ID = OL_I_ID;
      storage.order_line_values[i].OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
      storage.order_line_values[i].OL_DELIVERY_D = 0;
      storage.order_line_values[i].OL_QUANTITY = OL_QUANTITY;
      storage.order_line_values[i].OL_AMOUNT = OL_AMOUNT;

      switch (D_ID) {
      case 1:
        storage.order_line_values[i].OL_DIST_INFO =
            storage.stock_values[i].S_DIST_01;
        break;
      case 2:
        storage.order_line_values[i].OL_DIST_INFO =
            storage.stock_values[i].S_DIST_02;
        break;
      case 3:
        storage.order_line_values[i].OL_DIST_INFO =
            storage.stock_values[i].S_DIST_03;
        break;
      case 4:
        storage.order_line_values[i].OL_DIST_INFO =
            storage.stock_values[i].S_DIST_04;
        break;
      case 5:
        storage.order_line_values[i].OL_DIST_INFO =
            storage.stock_values[i].S_DIST_05;
        break;
      case 6:
        storage.order_line_values[i].OL_DIST_INFO =
            storage.stock_values[i].S_DIST_06;
        break;
      case 7:
        storage.order_line_values[i].OL_DIST_INFO =
            storage.stock_values[i].S_DIST_07;
        break;
      case 8:
        storage.order_line_values[i].OL_DIST_INFO =
            storage.stock_values[i].S_DIST_08;
        break;
      case 9:
        storage.order_line_values[i].OL_DIST_INFO =
            storage.stock_values[i].S_DIST_09;
        break;
      case 10:
        storage.order_line_values[i].OL_DIST_INFO =
            storage.stock_values[i].S_DIST_10;
        break;
      default:
        DCHECK(false);
        break;
      }
      total_amount += OL_AMOUNT * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX);
    }
  }

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::NewOrderQuery &query;
  float &total_amount;
};

class PwvPaymentDistrictStatement : public PwvStatement {
public:
  PwvPaymentDistrictStatement(tpcc::Database &db, const tpcc::Context &context,
                              tpcc::Random &random, tpcc::Storage &storage,
                              std::size_t partition_id,
                              const tpcc::PaymentQuery &query)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query) {}

  ~PwvPaymentDistrictStatement() override = default;

  void prepare_read_and_write_set() override {

    int32_t W_ID = partition_id + 1;

    // The input data (see Clause 2.5.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    int32_t C_D_ID = query.C_D_ID;
    int32_t C_W_ID = query.C_W_ID;
    float H_AMOUNT = query.H_AMOUNT;

    if (context.write_to_w_ytd) {
      auto warehouseTableID = tpcc::warehouse::tableID;
      // The row in the WAREHOUSE table with matching W_ID is selected.
      // W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are
      // retrieved and W_YTD,
      storage.warehouse_key = tpcc::warehouse::key(W_ID);
      PwvRWKey rwkey;
      rwkey.set_table_id(warehouseTableID);
      rwkey.set_partition_id(W_ID - 1);
      rwkey.set_key(&storage.warehouse_key);
      rwkey.set_value(&storage.warehouse_value);

      readSet.push_back(rwkey);
      writeSet.push_back(rwkey);
    }

    // The row in the DISTRICT table with matching D_W_ID and D_ID is selected.
    // D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved
    // and D_YTD,

    auto districtTableID = tpcc::district::tableID;
    storage.district_key = tpcc::district::key(W_ID, D_ID);

    {
      PwvRWKey rwkey;
      rwkey.set_table_id(districtTableID);
      rwkey.set_partition_id(W_ID - 1);
      rwkey.set_key(&storage.district_key);
      rwkey.set_value(&storage.district_value);

      readSet.push_back(rwkey);
      writeSet.push_back(rwkey);
    }

    CHECK(readSet.size() == (context.write_to_w_ytd ? 2 : 1))
        << "Check on readSet size failed!";
    CHECK(writeSet.size() == (context.write_to_w_ytd ? 2 : 1))
        << "Check on writeSet size failed!";
  }

  std::size_t piece_partition_id() override { return partition_id; }

  void execute() override {

    int32_t W_ID = this->partition_id + 1;

    // The input data (see Clause 2.5.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    int32_t C_D_ID = query.C_D_ID;
    int32_t C_W_ID = query.C_W_ID;
    float H_AMOUNT = query.H_AMOUNT;
    auto partitionId = W_ID - 1;

    // read & compute & write
    if (context.write_to_w_ytd) {
      auto warehouseTableId = tpcc::warehouse::tableID;
      ITable *warehouseTable = db.find_table(warehouseTableId, partitionId);
      auto warehouse_key = &storage.warehouse_key;
      auto warehouse_value = &storage.warehouse_value;
      auto value_bytes = warehouseTable->value_size();
      auto row = warehouseTable->search(warehouse_key);
      PwvHelper::read(row, warehouse_value, value_bytes);
      storage.warehouse_value.W_YTD += H_AMOUNT;
      warehouseTable->update(warehouse_key, warehouse_value);
    }

    auto districtTableId = tpcc::district::tableID;
    ITable *districtTable = db.find_table(districtTableId, partitionId);
    auto district_key = &storage.district_key;
    auto district_value = &storage.district_value;
    auto value_bytes = districtTable->value_size();
    auto row = districtTable->search(district_key);
    PwvHelper::read(row, district_value, value_bytes);
    storage.district_value.D_YTD += H_AMOUNT;
    districtTable->update(district_key, district_value);
  }

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::PaymentQuery &query;
};

class PwvPaymentCustomerStatement : public PwvStatement {
public:
  PwvPaymentCustomerStatement(tpcc::Database &db, const tpcc::Context &context,
                              tpcc::Random &random, tpcc::Storage &storage,
                              std::size_t partition_id,
                              const tpcc::PaymentQuery &query)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query) {}

  ~PwvPaymentCustomerStatement() override = default;

  void prepare_read_and_write_set() override {

    int32_t W_ID = this->partition_id + 1;

    // The input data (see Clause 2.5.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    int32_t C_D_ID = query.C_D_ID;
    int32_t C_W_ID = query.C_W_ID;
    float H_AMOUNT = query.H_AMOUNT;

    CHECK(C_ID > 0) << "Check on C_ID failed!";

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerTableID = tpcc::customer::tableID;
    storage.customer_key = tpcc::customer::key(C_W_ID, C_D_ID, C_ID);

    PwvRWKey rwkey;
    rwkey.set_table_id(customerTableID);
    rwkey.set_partition_id(C_W_ID - 1);
    rwkey.set_key(&storage.customer_key);
    rwkey.set_value(&storage.customer_value);

    readSet.push_back(rwkey);
    writeSet.push_back(rwkey);

    CHECK(readSet.size() == 1) << "Check on readSet size failed!";
    CHECK(writeSet.size() == 1) << "Check on writeSet size failed!";
  }

  std::size_t piece_partition_id() override {
    int32_t C_W_ID = query.C_W_ID;
    return C_W_ID - 1;
  }

  void execute() override {

    int32_t W_ID = this->partition_id + 1;

    // The input data (see Clause 2.5.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    int32_t C_D_ID = query.C_D_ID;
    int32_t C_W_ID = query.C_W_ID;
    float H_AMOUNT = query.H_AMOUNT;

    // read

    auto tableId = tpcc::customer::tableID;
    auto partitionId = C_W_ID - 1;
    auto key = &storage.customer_key;
    auto value = &storage.customer_value;
    ITable *table = db.find_table(tableId, partitionId);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    PwvHelper::read(row, value, value_bytes);

    char C_DATA[501];
    int total_written = 0;

    if (storage.customer_value.C_CREDIT == "BC") {
      int written;

      written = std::sprintf(C_DATA + total_written, "%d ", C_ID);
      total_written += written;

      written = std::sprintf(C_DATA + total_written, "%d ", C_D_ID);
      total_written += written;

      written = std::sprintf(C_DATA + total_written, "%d ", C_W_ID);
      total_written += written;

      written = std::sprintf(C_DATA + total_written, "%d ", D_ID);
      total_written += written;

      written = std::sprintf(C_DATA + total_written, "%d ", W_ID);
      total_written += written;

      written = std::sprintf(C_DATA + total_written, "%.2f ", H_AMOUNT);
      total_written += written;

      const char *old_C_DATA = storage.customer_value.C_DATA.c_str();

      std::memcpy(C_DATA + total_written, old_C_DATA, 500 - total_written);
      C_DATA[500] = 0;

      storage.customer_value.C_DATA.assign(C_DATA);
    }

    storage.customer_value.C_BALANCE -= H_AMOUNT;
    storage.customer_value.C_YTD_PAYMENT += H_AMOUNT;
    storage.customer_value.C_PAYMENT_CNT += 1;

    // compute for history

    char H_DATA[25];
    int written;

    written =
        std::sprintf(H_DATA, "%s    %s", storage.warehouse_value.W_NAME.c_str(),
                     storage.district_value.D_NAME.c_str());
    H_DATA[written] = 0;

    storage.h_key =
        tpcc::history::key(W_ID, D_ID, C_W_ID, C_D_ID, C_ID, Time::now());
    storage.h_value.H_AMOUNT = H_AMOUNT;
    storage.h_value.H_DATA.assign(H_DATA, written);

    // write
    table->update(key, value);
  }

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::PaymentQuery &query;
};

} // namespace aria