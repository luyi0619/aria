//
// Created by Yi Lu on 7/18/18.
//

#pragma once

#include "common/ClassOf.h"
#include "common/Encoder.h"
#include "common/HashMap.h"
#include "common/MVCCHashMap.h"
#include "common/StringPiece.h"
#include <memory>

#include "core/Context.h"

namespace aria {

class ITable {
public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~ITable() = default;

  virtual std::tuple<MetaDataType *, void *> search(const void *key,
                                                    uint64_t version = 0) = 0;

  virtual void *search_value(const void *key, uint64_t version = 0) = 0;

  virtual MetaDataType &search_metadata(const void *key,
                                        uint64_t version = 0) = 0;

  virtual std::tuple<MetaDataType *, void *> search_prev(const void *key,
                                                         uint64_t version) = 0;

  virtual void *search_value_prev(const void *key, uint64_t version) = 0;

  virtual MetaDataType &search_metadata_prev(const void *key,
                                             uint64_t version) = 0;

  virtual void insert(const void *key, const void *value,
                      uint64_t version = 0) = 0;

  virtual void update(const void *key, const void *value,
                      uint64_t version = 0) = 0;

  virtual void garbage_collect(const void *key) = 0;

  virtual void deserialize_value(const void *key, StringPiece stringPiece,
                                 uint64_t version = 0) = 0;

  virtual void serialize_value(Encoder &enc, const void *value) = 0;

  virtual std::size_t key_size() = 0;

  virtual std::size_t value_size() = 0;

  virtual std::size_t field_size() = 0;

  virtual std::size_t tableID() = 0;

  virtual std::size_t partitionID() = 0;
};

/* parameter version is not used in Table. */
template <std::size_t N, class KeyType, class ValueType>
class Table : public ITable {
public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~Table() override = default;

  Table(std::size_t tableID, std::size_t partitionID)
      : tableID_(tableID), partitionID_(partitionID) {}

  std::tuple<MetaDataType *, void *> search(const void *key,
                                            uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto &v = map_[k];
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value(const void *key, uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return &std::get<1>(map_[k]);
  }

  MetaDataType &search_metadata(const void *key,
                                uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return std::get<0>(map_[k]);
  }

  std::tuple<MetaDataType *, void *> search_prev(const void *key,
                                                 uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto &v = map_[k];
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value_prev(const void *key, uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return &std::get<1>(map_[k]);
  }

  MetaDataType &search_metadata_prev(const void *key,
                                     uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return std::get<0>(map_[k]);
  }

  void insert(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    DCHECK(map_.contains(k) == false);
    auto &row = map_[k];
    std::get<0>(row).store(0);
    std::get<1>(row) = v;
  }

  void update(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto &row = map_[k];
    std::get<1>(row) = v;
  }

  void garbage_collect(const void *key) override {}

  void deserialize_value(const void *key, StringPiece stringPiece,
                         uint64_t version = 0) override {

    std::size_t size = stringPiece.size();
    const auto &k = *static_cast<const KeyType *>(key);
    auto &row = map_[k];
    auto &v = std::get<1>(row);

    Decoder dec(stringPiece);
    dec >> v;

    DCHECK(size - dec.size() == ClassOf<ValueType>::size());
  }

  void serialize_value(Encoder &enc, const void *value) override {

    std::size_t size = enc.size();
    const auto &v = *static_cast<const ValueType *>(value);
    enc << v;

    DCHECK(enc.size() - size == ClassOf<ValueType>::size());
  }

  std::size_t key_size() override { return sizeof(KeyType); }

  std::size_t value_size() override { return sizeof(ValueType); }

  std::size_t field_size() override { return ClassOf<ValueType>::size(); }

  std::size_t tableID() override { return tableID_; }

  std::size_t partitionID() override { return partitionID_; }

private:
  HashMap<N, KeyType, std::tuple<MetaDataType, ValueType>> map_;
  std::size_t tableID_;
  std::size_t partitionID_;
};

template <std::size_t N, class KeyType, class ValueType>
class MVCCTable : public ITable {
public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~MVCCTable() override = default;

  MVCCTable(std::size_t tableID, std::size_t partitionID)
      : tableID_(tableID), partitionID_(partitionID) {}

  std::tuple<MetaDataType *, void *> search(const void *key,
                                            uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value(const void *key, uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return &std::get<1>(v);
  }

  MetaDataType &search_metadata(const void *key,
                                uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return std::get<0>(v);
  }

  std::tuple<MetaDataType *, void *> search_prev(const void *key,
                                                 uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version_prev(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value_prev(const void *key, uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version_prev(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return &std::get<1>(v);
  }

  MetaDataType &search_metadata_prev(const void *key,
                                     uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version_prev(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return std::get<0>(v);
  }

  void insert(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    bool ok = map_.contains_key_version(k, version);
    DCHECK(ok == false) << "version: " << version << " already exists.";
    auto &row = map_.insert_key_version_holder(k, version);
    std::get<0>(row).store(version);
    std::get<1>(row) = v;
  }

  void update(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto *row_ptr = map_.get_key_version(k, version);
    CHECK(row_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &row = *row_ptr;
    std::get<0>(row).store(0);
    std::get<1>(row) = v;
  }

  void garbage_collect(const void *key) override {
    const auto &k = *static_cast<const KeyType *>(key);
    DCHECK(map_.contains_key(k) == true) << "key to update does not exist.";
    map_.vacuum_key_keep_latest(k);
  }

  void deserialize_value(const void *key, StringPiece stringPiece,
                         uint64_t version = 0) override {

    std::size_t size = stringPiece.size();
    const auto &k = *static_cast<const KeyType *>(key);
    auto *row_ptr = map_.get_key_version(k, version);
    CHECK(row_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &row = *row_ptr;
    auto &v = std::get<1>(row);
    Decoder dec(stringPiece);
    dec >> v;
    DCHECK(size - dec.size() == ClassOf<ValueType>::size());
  }

  void serialize_value(Encoder &enc, const void *value) override {

    std::size_t size = enc.size();
    const auto &v = *static_cast<const ValueType *>(value);
    enc << v;

    DCHECK(enc.size() - size == ClassOf<ValueType>::size());
  }

  std::size_t key_size() override { return sizeof(KeyType); }

  std::size_t value_size() override { return sizeof(ValueType); }

  std::size_t field_size() override { return ClassOf<ValueType>::size(); }

  std::size_t tableID() override { return tableID_; }

  std::size_t partitionID() override { return partitionID_; }

private:
  MVCCHashMap<N, KeyType, std::tuple<MetaDataType, ValueType>> map_;
  std::size_t tableID_;
  std::size_t partitionID_;
};

class TableFactory {
public:
  template <std::size_t N, class KeyType, class ValueType>
  static std::unique_ptr<ITable> create_table(const Context &context,
                                              std::size_t tableID,
                                              std::size_t partitionID) {
    if (context.mvcc) {
      return std::make_unique<MVCCTable<N, KeyType, ValueType>>(tableID,
                                                                partitionID);
    } else {
      return std::make_unique<Table<N, KeyType, ValueType>>(tableID,
                                                            partitionID);
    }
  }
};

} // namespace aria
