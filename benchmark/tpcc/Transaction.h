//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "glog/logging.h"

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Query.h"
#include "benchmark/tpcc/Schema.h"
#include "benchmark/tpcc/Storage.h"
#include "common/Operation.h"
#include "common/Time.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"

namespace aria {
namespace tpcc {

template <class Transaction> class NewOrder : public Transaction {
public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = Storage;

  NewOrder(std::size_t coordinator_id, std::size_t partition_id,
           DatabaseType &db, const ContextType &context, RandomType &random,
           Partitioner &partitioner, Storage &storage)
      : Transaction(coordinator_id, partition_id, partitioner), db(db),
        context(context), random(random), storage(storage),
        partition_id(partition_id),
        query(makeNewOrderQuery()(context, partition_id + 1, random)) {}

  virtual ~NewOrder() override = default;

  TransactionResult execute(std::size_t worker_id) override {

    int32_t W_ID = this->partition_id + 1;

    // The input data (see Clause 2.4.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;

    // The row in the WAREHOUSE table with matching W_ID is selected and W_TAX,
    // the warehouse tax rate, is retrieved.

    auto warehouseTableID = warehouse::tableID;
    storage.warehouse_key = warehouse::key(W_ID);
    this->search_for_read(warehouseTableID, W_ID - 1, storage.warehouse_key,
                          storage.warehouse_value);

    // The row in the DISTRICT table with matching D_W_ID and D_ ID is selected,
    // D_TAX, the district tax rate, is retrieved, and D_NEXT_O_ID, the next
    // available order number for the district, is retrieved and incremented by
    // one.

    auto districtTableID = district::tableID;
    storage.district_key = district::key(W_ID, D_ID);
    this->search_for_update(districtTableID, W_ID - 1, storage.district_key,
                            storage.district_value);

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerTableID = customer::tableID;
    storage.customer_key = customer::key(W_ID, D_ID, C_ID);
    this->search_for_read(customerTableID, W_ID - 1, storage.customer_key,
                          storage.customer_value);

    auto itemTableID = item::tableID;
    auto stockTableID = stock::tableID;

    for (int i = 0; i < query.O_OL_CNT; i++) {

      // The row in the ITEM table with matching I_ID (equals OL_I_ID) is
      // selected and I_PRICE, the price of the item, I_NAME, the name of the
      // item, and I_DATA are retrieved. If I_ID has an unused value (see
      // Clause 2.4.1.5), a "not-found" condition is signaled, resulting in a
      // rollback of the database transaction (see Clause 2.4.2.3).

      int32_t OL_I_ID = query.INFO[i].OL_I_ID;
      int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

      storage.item_keys[i] = item::key(OL_I_ID);

      // If I_ID has an unused value, rollback.
      // In OCC, rollback can return without going through commit protocol

      if (storage.item_keys[i].I_ID == 0) {
        // abort();
        return TransactionResult::ABORT_NORETRY;
      }

      this->search_local_index(itemTableID, 0, storage.item_keys[i],
                               storage.item_values[i]);

      // The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
      // S_W_ID (equals OL_SUPPLY_W_ID) is selected.

      storage.stock_keys[i] = stock::key(OL_SUPPLY_W_ID, OL_I_ID);

      this->search_for_update(stockTableID, OL_SUPPLY_W_ID - 1,
                              storage.stock_keys[i], storage.stock_values[i]);
    }

    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    }

    float W_TAX = storage.warehouse_value.W_YTD;

    float D_TAX = storage.district_value.D_TAX;
    int32_t D_NEXT_O_ID = storage.district_value.D_NEXT_O_ID;

    storage.district_value.D_NEXT_O_ID += 1;

    this->update(districtTableID, W_ID - 1, storage.district_key,
                 storage.district_value);

    if (context.operation_replication) {
      Encoder encoder(this->operation.data);
      this->operation.partition_id = this->partition_id;
      encoder << true << storage.district_key.D_W_ID
              << storage.district_key.D_ID
              << storage.district_value.D_NEXT_O_ID;
    }

    float C_DISCOUNT = storage.customer_value.C_DISCOUNT;

    // A new row is inserted into both the NEW-ORDER table and the ORDER table
    // to reflect the creation of the new order. O_CARRIER_ID is set to a null
    // value. If the order includes only home order-lines, then O_ALL_LOCAL is
    // set to 1, otherwise O_ALL_LOCAL is set to 0.

    storage.new_order_key = new_order::key(W_ID, D_ID, D_NEXT_O_ID);

    storage.order_key = order::key(W_ID, D_ID, D_NEXT_O_ID);

    storage.order_value.O_ENTRY_D = Time::now();
    storage.order_value.O_CARRIER_ID = 0;
    storage.order_value.O_OL_CNT = query.O_OL_CNT;
    storage.order_value.O_C_ID = query.C_ID;
    storage.order_value.O_ALL_LOCAL = !query.isRemote();

    float total_amount = 0;

    auto orderLineTableID = order_line::tableID;

    for (int i = 0; i < query.O_OL_CNT; i++) {

      int32_t OL_I_ID = query.INFO[i].OL_I_ID;
      int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

      float I_PRICE = storage.item_values[i].I_PRICE;

      // S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the
      // district number, and S_DATA are retrieved. If the retrieved value for
      // S_QUANTITY exceeds OL_QUANTITY by 10 or more, then S_QUANTITY is
      // decreased by OL_QUANTITY; otherwise S_QUANTITY is updated to
      // (S_QUANTITY - OL_QUANTITY)+91. S_YTD is increased by OL_QUANTITY and
      // S_ORDER_CNT is incremented by 1. If the order-line is remote, then
      // S_REMOTE_CNT is incremented by 1.

      if (storage.stock_values[i].S_QUANTITY >= OL_QUANTITY + 10) {
        storage.stock_values[i].S_QUANTITY -= OL_QUANTITY;
      } else {
        storage.stock_values[i].S_QUANTITY =
            storage.stock_values[i].S_QUANTITY - OL_QUANTITY + 91;
      }

      storage.stock_values[i].S_YTD += OL_QUANTITY;
      storage.stock_values[i].S_ORDER_CNT++;

      if (OL_SUPPLY_W_ID != W_ID) {
        storage.stock_values[i].S_REMOTE_CNT++;
      }

      this->update(stockTableID, OL_SUPPLY_W_ID - 1, storage.stock_keys[i],
                   storage.stock_values[i]);

      if (context.operation_replication) {
        Encoder encoder(this->operation.data);
        encoder << storage.stock_keys[i].S_W_ID << storage.stock_keys[i].S_I_ID
                << storage.stock_values[i].S_QUANTITY
                << storage.stock_values[i].S_YTD
                << storage.stock_values[i].S_ORDER_CNT
                << storage.stock_values[i].S_REMOTE_CNT;
      }
      if (this->execution_phase) {
        float OL_AMOUNT = I_PRICE * OL_QUANTITY;
        storage.order_line_keys[i] =
            order_line::key(W_ID, D_ID, D_NEXT_O_ID, i + 1);

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

    return TransactionResult::READY_TO_COMMIT;
  }

  void reset_query() override {
    query = makeNewOrderQuery()(context, partition_id, random);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
  NewOrderQuery query;
};

template <class Transaction> class Payment : public Transaction {
public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = Storage;

  Payment(std::size_t coordinator_id, std::size_t partition_id,
          DatabaseType &db, const ContextType &context, RandomType &random,
          Partitioner &partitioner, Storage &storage)
      : Transaction(coordinator_id, partition_id, partitioner), db(db),
        context(context), random(random), storage(storage),
        partition_id(partition_id),
        query(makePaymentQuery()(context, partition_id + 1, random)) {}

  virtual ~Payment() override = default;

  TransactionResult execute(std::size_t worker_id) override {

    int32_t W_ID = this->partition_id + 1;

    // The input data (see Clause 2.5.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    int32_t C_D_ID = query.C_D_ID;
    int32_t C_W_ID = query.C_W_ID;
    float H_AMOUNT = query.H_AMOUNT;

    auto warehouseTableID = warehouse::tableID;
    if (context.write_to_w_ytd) {
      // The row in the WAREHOUSE table with matching W_ID is selected.
      // W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are
      // retrieved and W_YTD,
      storage.warehouse_key = warehouse::key(W_ID);
      this->search_for_update(warehouseTableID, W_ID - 1, storage.warehouse_key,
                              storage.warehouse_value);
    }

    // The row in the DISTRICT table with matching D_W_ID and D_ID is selected.
    // D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved
    // and D_YTD,

    auto districtTableID = district::tableID;
    storage.district_key = district::key(W_ID, D_ID);
    this->search_for_update(districtTableID, W_ID - 1, storage.district_key,
                            storage.district_value);

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerNameIdxTableID = customer_name_idx::tableID;

    if (C_ID == 0) {
      storage.customer_name_idx_key =
          customer_name_idx::key(C_W_ID, C_D_ID, query.C_LAST);
      this->search_for_read(customerNameIdxTableID, C_W_ID - 1,
                            storage.customer_name_idx_key,
                            storage.customer_name_idx_value);

      this->process_requests(worker_id);
      C_ID = storage.customer_name_idx_value.C_ID;
      CHECK(C_ID > 0) << "Invalid C_ID read from index";
    }

    auto customerTableID = customer::tableID;
    storage.customer_key = customer::key(C_W_ID, C_D_ID, C_ID);
    this->search_for_update(customerTableID, C_W_ID - 1, storage.customer_key,
                            storage.customer_value);
    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    }

    if (context.write_to_w_ytd) {
      // the warehouse's year-to-date balance, is increased by H_ AMOUNT.
      storage.warehouse_value.W_YTD += H_AMOUNT;
      this->update(warehouseTableID, W_ID - 1, storage.warehouse_key,
                   storage.warehouse_value);

      if (context.operation_replication) {
        this->operation.partition_id = this->partition_id;
        Encoder encoder(this->operation.data);
        encoder << false << storage.warehouse_key.W_ID
                << storage.warehouse_value.W_YTD;
      }
    }

    // the district's year-to-date balance, is increased by H_AMOUNT.
    storage.district_value.D_YTD += H_AMOUNT;
    this->update(districtTableID, W_ID - 1, storage.district_key,
                 storage.district_value);

    if (context.operation_replication) {
      Encoder encoder(this->operation.data);
      encoder << storage.district_key.D_W_ID << storage.district_key.D_ID
              << storage.district_value.D_YTD;
    }

    char C_DATA[501];
    int total_written = 0;
    if (this->execution_phase) {
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
    }
    this->update(customerTableID, C_W_ID - 1, storage.customer_key,
                 storage.customer_value);

    if (context.operation_replication) {
      Encoder encoder(this->operation.data);
      encoder << storage.customer_key.C_W_ID << storage.customer_key.C_D_ID
              << storage.customer_key.C_ID;
      encoder << uint32_t(total_written);
      encoder.write_n_bytes(C_DATA, total_written);
      encoder << storage.customer_value.C_BALANCE
              << storage.customer_value.C_YTD_PAYMENT
              << storage.customer_value.C_PAYMENT_CNT;
    }

    char H_DATA[25];
    int written;
    if (this->execution_phase) {
      written = std::sprintf(H_DATA, "%s    %s",
                             storage.warehouse_value.W_NAME.c_str(),
                             storage.district_value.D_NAME.c_str());
      H_DATA[written] = 0;

      storage.h_key =
          history::key(W_ID, D_ID, C_W_ID, C_D_ID, C_ID, Time::now());
      storage.h_value.H_AMOUNT = H_AMOUNT;
      storage.h_value.H_DATA.assign(H_DATA, written);
    }
    return TransactionResult::READY_TO_COMMIT;
  }

  void reset_query() override {
    query = makePaymentQuery()(context, partition_id, random);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
  PaymentQuery query;
};

} // namespace tpcc
} // namespace aria
