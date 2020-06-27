//
// Created by Yi Lu on 7/15/18.
//

#pragma once

#include "common/ClassOf.h"
#include "common/FixedString.h"
#include "common/Hash.h"
#include "common/Serialization.h"
#include "core/SchemaDef.h"

// table definition for tpcc
namespace aria {
namespace tpcc {
static constexpr std::size_t __BASE_COUNTER__ = __COUNTER__ + 1;
}
} // namespace aria

#undef NAMESPACE_FIELDS
#define NAMESPACE_FIELDS(x) x(aria) x(tpcc)

#define WAREHOUSE_KEY_FIELDS(x, y) x(int32_t, W_ID)
#define WAREHOUSE_VALUE_FIELDS(x, y)                                           \
  x(FixedString<10>, W_NAME) y(FixedString<20>, W_STREET_1)                    \
      y(FixedString<20>, W_STREET_2) y(FixedString<20>, W_CITY)                \
          y(FixedString<2>, W_STATE) y(FixedString<9>, W_ZIP) y(float, W_TAX)  \
              y(float, W_YTD)

DO_STRUCT(warehouse, WAREHOUSE_KEY_FIELDS, WAREHOUSE_VALUE_FIELDS,
          NAMESPACE_FIELDS)

#define DISTRICT_KEY_FIELDS(x, y) x(int32_t, D_W_ID) y(int32_t, D_ID)
#define DISTRICT_VALUE_FIELDS(x, y)                                            \
  x(FixedString<10>, D_NAME) y(FixedString<20>, D_STREET_1)                    \
      y(FixedString<20>, D_STREET_2) y(FixedString<20>, D_CITY)                \
          y(FixedString<2>, D_STATE) y(FixedString<9>, D_ZIP) y(float, D_TAX)  \
              y(float, D_YTD) y(int32_t, D_NEXT_O_ID)

DO_STRUCT(district, DISTRICT_KEY_FIELDS, DISTRICT_VALUE_FIELDS,
          NAMESPACE_FIELDS)

#define CUSTOMER_KEY_FIELDS(x, y)                                              \
  x(int32_t, C_W_ID) y(int32_t, C_D_ID) y(int32_t, C_ID)
#define CUSTOMER_VALUE_FIELDS(x, y)                                            \
  x(FixedString<16>, C_FIRST) y(FixedString<2>, C_MIDDLE)                      \
      y(FixedString<16>, C_LAST) y(FixedString<20>, C_STREET_1)                \
          y(FixedString<20>, C_STREET_2) y(FixedString<20>, C_CITY)            \
              y(FixedString<2>, C_STATE) y(FixedString<9>, C_ZIP)              \
                  y(FixedString<16>, C_PHONE) y(uint64_t, C_SINCE)             \
                      y(FixedString<2>, C_CREDIT) y(float, C_CREDIT_LIM)       \
                          y(float, C_DISCOUNT) y(float, C_BALANCE)             \
                              y(float, C_YTD_PAYMENT)                          \
                                  y(int32_t, C_PAYMENT_CNT)                    \
                                      y(int32_t, C_DELIVERY_CNT)               \
                                          y(FixedString<500>, C_DATA)

DO_STRUCT(customer, CUSTOMER_KEY_FIELDS, CUSTOMER_VALUE_FIELDS,
          NAMESPACE_FIELDS)

#define CUSTOMER_NAME_IDX_KEY_FIELDS(x, y)                                     \
  x(int32_t, C_W_ID) y(int32_t, C_D_ID) y(FixedString<16>, C_LAST)
#define CUSTOMER_NAME_IDX_VALUE_FIELDS(x, y) x(int32_t, C_ID)

DO_STRUCT(customer_name_idx, CUSTOMER_NAME_IDX_KEY_FIELDS,
          CUSTOMER_NAME_IDX_VALUE_FIELDS, NAMESPACE_FIELDS)

#define HISTORY_KEY_FIELDS(x, y)                                               \
  x(int32_t, H_W_ID) y(int32_t, H_D_ID) y(int32_t, H_C_W_ID)                   \
      y(int32_t, H_C_D_ID) y(int32_t, H_C_ID) y(uint64_t, H_DATE)
#define HISTORY_VALUE_FIELDS(x, y) x(float, H_AMOUNT) y(FixedString<24>, H_DATA)

DO_STRUCT(history, HISTORY_KEY_FIELDS, HISTORY_VALUE_FIELDS, NAMESPACE_FIELDS)

#define NEW_ORDER_KEY_FIELDS(x, y)                                             \
  x(int32_t, NO_W_ID) y(int32_t, NO_D_ID) y(int32_t, NO_O_ID)
#define NEW_ORDER_VALUE_FIELDS(x, y) x(int32_t, NO_DUMMY)

DO_STRUCT(new_order, NEW_ORDER_KEY_FIELDS, NEW_ORDER_VALUE_FIELDS,
          NAMESPACE_FIELDS)

#define ORDER_KEY_FIELDS(x, y)                                                 \
  x(int32_t, O_W_ID) y(int32_t, O_D_ID) y(int32_t, O_ID)
#define ORDER_VALUE_FIELDS(x, y)                                               \
  x(float, O_C_ID) y(uint64_t, O_ENTRY_D) y(int32_t, O_CARRIER_ID)             \
      y(int8_t, O_OL_CNT) y(bool, O_ALL_LOCAL)

DO_STRUCT(order, ORDER_KEY_FIELDS, ORDER_VALUE_FIELDS, NAMESPACE_FIELDS)

#define ORDER_LINE_KEY_FIELDS(x, y)                                            \
  x(int32_t, OL_W_ID) y(int32_t, OL_D_ID) y(int32_t, OL_O_ID)                  \
      y(int8_t, OL_NUMBER)
#define ORDER_LINE_VALUE_FIELDS(x, y)                                          \
  x(int32_t, OL_I_ID) y(int32_t, OL_SUPPLY_W_ID) y(uint64_t, OL_DELIVERY_D)    \
      y(int8_t, OL_QUANTITY) y(float, OL_AMOUNT)                               \
          y(FixedString<24>, OL_DIST_INFO)

DO_STRUCT(order_line, ORDER_LINE_KEY_FIELDS, ORDER_LINE_VALUE_FIELDS,
          NAMESPACE_FIELDS)

#define ITEM_KEY_FIELDS(x, y) x(int32_t, I_ID)
#define ITEM_VALUE_FIELDS(x, y)                                                \
  x(int32_t, I_IM_ID) y(FixedString<24>, I_NAME) y(float, I_PRICE)             \
      y(FixedString<50>, I_DATA)

DO_STRUCT(item, ITEM_KEY_FIELDS, ITEM_VALUE_FIELDS, NAMESPACE_FIELDS)

#define STOCK_KEY_FIELDS(x, y) x(int32_t, S_W_ID) y(int32_t, S_I_ID)
#define STOCK_VALUE_FIELDS(x, y)                                               \
  x(int16_t, S_QUANTITY) y(FixedString<24>, S_DIST_01)                         \
      y(FixedString<24>, S_DIST_02) y(FixedString<24>, S_DIST_03)              \
          y(FixedString<24>, S_DIST_04) y(FixedString<24>, S_DIST_05)          \
              y(FixedString<24>, S_DIST_06) y(FixedString<24>, S_DIST_07)      \
                  y(FixedString<24>, S_DIST_08) y(FixedString<24>, S_DIST_09)  \
                      y(FixedString<24>, S_DIST_10) y(float, S_YTD)            \
                          y(int32_t, S_ORDER_CNT) y(int32_t, S_REMOTE_CNT)     \
                              y(FixedString<50>, S_DATA)

DO_STRUCT(stock, STOCK_KEY_FIELDS, STOCK_VALUE_FIELDS, NAMESPACE_FIELDS)

namespace aria {

template <> class Serializer<tpcc::warehouse::value> {
public:
  std::string operator()(const tpcc::warehouse::value &v) {
    return Serializer<decltype(v.W_YTD)>()(v.W_YTD);
  }
};

template <> class Deserializer<tpcc::warehouse::value> {
public:
  std::size_t operator()(StringPiece str,
                         tpcc::warehouse::value &result) const {
    return Deserializer<decltype(result.W_YTD)>()(str, result.W_YTD);
  }
};

template <> class ClassOf<tpcc::warehouse::value> {
public:
  static constexpr std::size_t size() {
    return ClassOf<decltype(tpcc::warehouse::value::W_YTD)>::size();
  }
};

template <> class Serializer<tpcc::district::value> {
public:
  std::string operator()(const tpcc::district::value &v) {
    return Serializer<decltype(v.D_YTD)>()(v.D_YTD) +
           Serializer<decltype(v.D_NEXT_O_ID)>()(v.D_NEXT_O_ID);
  }
};

template <> class Deserializer<tpcc::district::value> {
public:
  std::size_t operator()(StringPiece str, tpcc::district::value &result) const {
    std::size_t sz_ytd =
        Deserializer<decltype(result.D_YTD)>()(str, result.D_YTD);
    str.remove_prefix(sz_ytd);
    std::size_t sz_next_o_id =
        Deserializer<decltype(result.D_NEXT_O_ID)>()(str, result.D_NEXT_O_ID);
    return sz_ytd + sz_next_o_id;
  }
};

template <> class ClassOf<tpcc::district::value> {
public:
  static constexpr std::size_t size() {
    return ClassOf<decltype(tpcc::district::value::D_YTD)>::size() +
           ClassOf<decltype(tpcc::district::value::D_NEXT_O_ID)>::size();
  }
};

template <> class Serializer<tpcc::customer::value> {
public:
  std::string operator()(const tpcc::customer::value &v) {
    return Serializer<decltype(v.C_DATA)>()(v.C_DATA) +
           Serializer<decltype(v.C_BALANCE)>()(v.C_BALANCE) +
           Serializer<decltype(v.C_YTD_PAYMENT)>()(v.C_YTD_PAYMENT) +
           Serializer<decltype(v.C_PAYMENT_CNT)>()(v.C_PAYMENT_CNT);
  }
};

template <> class Deserializer<tpcc::customer::value> {
public:
  std::size_t operator()(StringPiece str, tpcc::customer::value &result) const {
    std::size_t sz_data =
        Deserializer<decltype(result.C_DATA)>()(str, result.C_DATA);
    str.remove_prefix(sz_data);
    std::size_t sz_balance =
        Deserializer<decltype(result.C_BALANCE)>()(str, result.C_BALANCE);
    str.remove_prefix(sz_balance);
    std::size_t sz_ytd_payment = Deserializer<decltype(result.C_YTD_PAYMENT)>()(
        str, result.C_YTD_PAYMENT);
    str.remove_prefix(sz_ytd_payment);
    std::size_t sz_payment_cnt = Deserializer<decltype(result.C_PAYMENT_CNT)>()(
        str, result.C_PAYMENT_CNT);

    return sz_data + sz_balance + sz_ytd_payment + sz_payment_cnt;
  }
};

template <> class ClassOf<tpcc::customer::value> {
public:
  static constexpr std::size_t size() {
    return ClassOf<decltype(tpcc::customer::value::C_DATA)>::size() +
           ClassOf<decltype(tpcc::customer::value::C_BALANCE)>::size() +
           ClassOf<decltype(tpcc::customer::value::C_YTD_PAYMENT)>::size() +
           ClassOf<decltype(tpcc::customer::value::C_PAYMENT_CNT)>::size();
  }
};

template <> class Serializer<tpcc::stock::value> {
public:
  std::string operator()(const tpcc::stock::value &v) {
    return Serializer<decltype(v.S_QUANTITY)>()(v.S_QUANTITY) +
           Serializer<decltype(v.S_YTD)>()(v.S_YTD) +
           Serializer<decltype(v.S_ORDER_CNT)>()(v.S_ORDER_CNT) +
           Serializer<decltype(v.S_REMOTE_CNT)>()(v.S_REMOTE_CNT);
  }
};

template <> class Deserializer<tpcc::stock::value> {
public:
  std::size_t operator()(StringPiece str, tpcc::stock::value &result) const {
    std::size_t sz_quantity =
        Deserializer<decltype(result.S_QUANTITY)>()(str, result.S_QUANTITY);
    str.remove_prefix(sz_quantity);
    std::size_t sz_ytd =
        Deserializer<decltype(result.S_YTD)>()(str, result.S_YTD);
    str.remove_prefix(sz_ytd);
    std::size_t sz_order_cnt =
        Deserializer<decltype(result.S_ORDER_CNT)>()(str, result.S_ORDER_CNT);
    str.remove_prefix(sz_order_cnt);
    std::size_t sz_remote_cnt =
        Deserializer<decltype(result.S_REMOTE_CNT)>()(str, result.S_REMOTE_CNT);

    return sz_quantity + sz_ytd + sz_order_cnt + sz_remote_cnt;
  }
};

template <> class ClassOf<tpcc::stock::value> {
public:
  static constexpr std::size_t size() {
    return ClassOf<decltype(tpcc::stock::value::S_QUANTITY)>::size() +
           ClassOf<decltype(tpcc::stock::value::S_YTD)>::size() +
           ClassOf<decltype(tpcc::stock::value::S_ORDER_CNT)>::size() +
           ClassOf<decltype(tpcc::stock::value::S_REMOTE_CNT)>::size();
  }
};

} // namespace aria
