//
// Created by Yi Lu on 8/30/18.
//

#pragma once

#include "common/StringPiece.h"

namespace aria {

/*
 * MessagePiece header format
 *
 * | Message type (12 => 4048) | Message length (31 => 2147483648) | table id (5
 * => 32) | partition id (16 => 65536) |
 *
 * Note that, the header is included in the message length.
 */

class MessagePiece {

public:
  using header_type = uint64_t;

  MessagePiece(const MessagePiece &messagePiece)
      : stringPiece(messagePiece.stringPiece) {}

  MessagePiece(const StringPiece &stringPiece) : stringPiece(stringPiece) {}

  uint64_t get_message_type() const {
    return (get_header() >> MESSAGE_TYPE_OFFSET) & MESSAGE_TYPE_MASK;
  }

  uint64_t get_message_length() const {
    return (get_header() >> MESSAGE_LENGTH_OFFSET) & MESSAGE_LENGTH_MASK;
  }

  uint64_t get_table_id() const {
    return (get_header() >> TABLE_ID_OFFSET) & TABLE_ID_MASK;
  }

  uint64_t get_partition_id() const {
    return (get_header() >> PARTITION_ID_OFFSET) & PARTITION_ID_MASK;
  }

  StringPiece toStringPiece() {
    return StringPiece(stringPiece.data() + get_header_size(),
                       get_message_length() - get_header_size());
  }

  bool operator==(const MessagePiece &that) const {
    return stringPiece == that.stringPiece;
  }

  bool operator!=(const MessagePiece &that) const {
    return stringPiece != that.stringPiece;
  }

private:
  uint64_t get_header() const {
    return *reinterpret_cast<const uint64_t *>(stringPiece.data());
  }

public:
  StringPiece stringPiece;

public:
  static uint64_t get_header_size() { return sizeof(header_type); }

  static uint64_t construct_message_piece_header(uint64_t message_type,
                                                 uint64_t message_length,
                                                 std::size_t table_id,
                                                 std::size_t partition_id) {
    DCHECK(message_type < (1ul << 12));
    DCHECK(message_length < (1ul << 31));
    DCHECK(table_id < (1ul << 5));
    DCHECK(partition_id < (1ul << 16));

    return (message_type << MESSAGE_TYPE_OFFSET) +
           (message_length << MESSAGE_LENGTH_OFFSET) +
           (table_id << TABLE_ID_OFFSET) +
           (partition_id << PARTITION_ID_OFFSET);
  }

  static constexpr uint64_t get_message_length(uint64_t header) {
    return (header >> MESSAGE_LENGTH_OFFSET) & MESSAGE_LENGTH_MASK;
  }

public:
  static constexpr uint64_t MESSAGE_TYPE_MASK = 0xfff;
  static constexpr uint64_t MESSAGE_TYPE_OFFSET = 52;
  static constexpr uint64_t MESSAGE_LENGTH_MASK = 0x7fffffff;
  static constexpr uint64_t MESSAGE_LENGTH_OFFSET = 21;
  static constexpr uint64_t TABLE_ID_MASK = 0x1f;
  static constexpr uint64_t TABLE_ID_OFFSET = 16;
  static constexpr uint64_t PARTITION_ID_MASK = 0xffff;
  static constexpr uint64_t PARTITION_ID_OFFSET = 0;
};
} // namespace aria
