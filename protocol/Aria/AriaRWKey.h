//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include <glog/logging.h>

namespace aria {

class AriaRWKey {
public:
  // local index read bit

  void set_local_index_read_bit() {
    clear_local_index_read_bit();
    bitvec |= LOCAL_INDEX_READ_BIT_MASK << LOCAL_INDEX_READ_BIT_OFFSET;
  }

  void clear_local_index_read_bit() {
    bitvec &= ~(LOCAL_INDEX_READ_BIT_MASK << LOCAL_INDEX_READ_BIT_OFFSET);
  }

  uint32_t get_local_index_read_bit() const {
    return (bitvec >> LOCAL_INDEX_READ_BIT_OFFSET) & LOCAL_INDEX_READ_BIT_MASK;
  }

  // read request bit

  void set_read_request_bit() {
    clear_read_request_bit();
    bitvec |= READ_REQUEST_BIT_MASK << READ_REQUEST_BIT_OFFSET;
  }

  void clear_read_request_bit() {
    bitvec &= ~(READ_REQUEST_BIT_MASK << READ_REQUEST_BIT_OFFSET);
  }

  uint32_t get_read_request_bit() const {
    return (bitvec >> READ_REQUEST_BIT_OFFSET) & READ_REQUEST_BIT_MASK;
  }

  // table id

  void set_table_id(uint32_t table_id) {
    DCHECK(table_id < (1 << 5));
    clear_table_id();
    bitvec |= table_id << TABLE_ID_OFFSET;
  }

  void clear_table_id() { bitvec &= ~(TABLE_ID_MASK << TABLE_ID_OFFSET); }

  uint32_t get_table_id() const {
    return (bitvec >> TABLE_ID_OFFSET) & TABLE_ID_MASK;
  }
  // partition id

  void set_partition_id(uint32_t partition_id) {
    DCHECK(partition_id < (1 << 16));
    clear_partition_id();
    bitvec |= partition_id << PARTITION_ID_OFFSET;
  }

  void clear_partition_id() {
    bitvec &= ~(PARTITION_ID_MASK << PARTITION_ID_OFFSET);
  }

  uint32_t get_partition_id() const {
    return (bitvec >> PARTITION_ID_OFFSET) & PARTITION_ID_MASK;
  }

  // key
  void set_key(const void *key) { this->key = key; }

  const void *get_key() const { return key; }

  // value
  void set_value(void *value) { this->value = value; }

  void *get_value() const { return value; }

  void set_tid(std::atomic<uint64_t> *tid) { this->tid = tid; };

  std::atomic<uint64_t> *get_tid() const { return tid; };

private:
  /*
   * A bitvec is a 32-bit word.
   *
   * [ table id (5) ] | partition id (16) | unused bit (9) |
   * read request bit (1) | local index read (1)  ]
   *
   * local index read is set when the read is from a local read only index.
   */

  uint32_t bitvec = 0;
  const void *key = nullptr;
  void *value = nullptr;
  std::atomic<uint64_t> *tid = nullptr;

public:
  static constexpr uint32_t TABLE_ID_MASK = 0x1f;
  static constexpr uint32_t TABLE_ID_OFFSET = 27;

  static constexpr uint32_t PARTITION_ID_MASK = 0xffff;
  static constexpr uint32_t PARTITION_ID_OFFSET = 11;

  static constexpr uint32_t READ_REQUEST_BIT_MASK = 0x1;
  static constexpr uint32_t READ_REQUEST_BIT_OFFSET = 1;

  static constexpr uint32_t LOCAL_INDEX_READ_BIT_MASK = 0x1;
  static constexpr uint32_t LOCAL_INDEX_READ_BIT_OFFSET = 0;
};
} // namespace aria