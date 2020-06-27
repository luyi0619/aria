//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Aria/AriaRWKey.h"
#include "protocol/Aria/AriaTransaction.h"

namespace aria {

enum class AriaMessage {
  SEARCH_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  SEARCH_RESPONSE,
  RESERVE_REQUEST,
  CHECK_REQUEST,
  CHECK_RESPONSE,
  WRITE_REQUEST,
  NFIELDS
};

class AriaMessageFactory {
public:
  static std::size_t new_search_message(Message &message, ITable &table,
                                        uint32_t tid, uint32_t tid_offset,
                                        const void *key, uint32_t key_offset) {
    /*
     * The structure of a search request: (primary key, tid, tid_offset, read
     * key offset)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        sizeof(uint32_t) + sizeof(uint32_t) +
                        sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(AriaMessage::SEARCH_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << tid << tid_offset << key_offset;
    message.flush();
    return message_size;
  }

  static std::size_t new_reserve_message(Message &message, ITable &table,
                                         uint32_t tid, const void *key,
                                         uint32_t epoch, bool is_write) {
    /*
     * The structure of a reserve request: (primary key, tid, epoch, is_write)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        sizeof(uint32_t) + sizeof(epoch) + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(AriaMessage::RESERVE_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << tid << epoch << is_write;
    message.flush();
    return message_size;
  }

  static std::size_t new_check_message(Message &message, ITable &table,
                                       uint32_t tid, uint32_t tid_offset,
                                       const void *key, uint32_t epoch,
                                       bool is_write) {
    /*
     * The structure of a check request: (primary key, tid, tid_offset, epoch,
     * is_write)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        sizeof(uint32_t) + sizeof(uint32_t) + sizeof(epoch) +
                        sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(AriaMessage::CHECK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << tid << tid_offset << epoch << is_write;
    message.flush();
    return message_size;
  }

  static std::size_t new_write_message(Message &message, ITable &table,
                                       const void *key, const void *value) {

    /*
     * The structure of a write request: (primary key, field value)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size + field_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(AriaMessage::WRITE_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    message.flush();
    return message_size;
  }
};

class AriaMessageHandler {
  using Transaction = AriaTransaction;

public:
  static void
  search_request_handler(MessagePiece inputPiece, Message &responseMessage,
                         ITable &table,
                         std::vector<std::unique_ptr<Transaction>> &txns) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(AriaMessage::SEARCH_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read request: (primary key, tid, tid_offset, read key
     * offset) The structure of a read response: (value, tid, tid_offset, read
     * key offset)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t tid, tid_offset, key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(tid) +
               sizeof(tid_offset) + sizeof(key_offset));

    // get row, tid and offset
    const void *key = stringPiece.data();
    auto row = table.search(key);

    stringPiece.remove_prefix(key_size);
    aria::Decoder dec(stringPiece);
    dec >> tid >> tid_offset >> key_offset;

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + value_size +
                        sizeof(tid) + sizeof(tid_offset) + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(AriaMessage::SEARCH_RESPONSE), message_size,
        table_id, partition_id);

    aria::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    // reserve size for read
    responseMessage.data.append(value_size, 0);
    void *dest =
        &responseMessage.data[0] + responseMessage.data.size() - value_size;
    // read to message buffer
    AriaHelper::read(row, dest, value_size);
    encoder << tid << tid_offset << key_offset;
    responseMessage.flush();
  }

  static void
  search_response_handler(MessagePiece inputPiece, Message &responseMessage,
                          ITable &table,
                          std::vector<std::unique_ptr<Transaction>> &txns) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(AriaMessage::SEARCH_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read response: (value, tid, tid_offset, read key
     * offset)
     */

    uint32_t tid, tid_offset, key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + value_size + sizeof(tid) +
               sizeof(tid_offset) + sizeof(key_offset));

    StringPiece stringPiece = inputPiece.toStringPiece();
    stringPiece.remove_prefix(value_size);
    Decoder dec(stringPiece);
    dec >> tid >> tid_offset >> key_offset;

    CHECK(tid_offset >= 0 && tid_offset < txns.size());
    CHECK(txns[tid_offset]->id == tid);
    CHECK(key_offset < txns[tid_offset]->readSet.size());

    AriaRWKey &readKey = txns[tid_offset]->readSet[key_offset];
    dec = Decoder(inputPiece.toStringPiece());
    dec.read_n_bytes(readKey.get_value(), value_size);
    txns[tid_offset]->pendingResponses--;
    txns[tid_offset]->network_size += inputPiece.get_message_length();
  }

  static void
  reserve_request_handler(MessagePiece inputPiece, Message &responseMessage,
                          ITable &table,
                          std::vector<std::unique_ptr<Transaction>> &txns) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(AriaMessage::RESERVE_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read request: (primary key, tid, epoch, is_write)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t tid, epoch;
    bool is_write;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(tid) +
               sizeof(epoch) + sizeof(is_write));

    // get metadata, tid, epoch and is_write
    const void *key = stringPiece.data();
    std::atomic<uint64_t> &metadata = table.search_metadata(key);

    stringPiece.remove_prefix(key_size);
    aria::Decoder dec(stringPiece);
    dec >> tid >> epoch >> is_write;

    DCHECK(dec.size() == 0);

    if (is_write) {
      AriaHelper::reserve_write(metadata, epoch, tid);
    } else {
      AriaHelper::reserve_read(metadata, epoch, tid);
    }
  }

  static void
  check_request_handler(MessagePiece inputPiece, Message &responseMessage,
                        ITable &table,
                        std::vector<std::unique_ptr<Transaction>> &txns) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(AriaMessage::CHECK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a check request: (primary key, tid, tid_offset,  epoch,
     * is_write) The structure of a check response: (tid, tid_offset, is_write,
     * waw, war, raw)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t tid, tid_offset, epoch;
    bool is_write;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(tid) +
               sizeof(tid_offset) + sizeof(epoch) + sizeof(is_write));

    // get row, tid and offset
    const void *key = stringPiece.data();
    uint64_t metadata = table.search_metadata(key).load();

    stringPiece.remove_prefix(key_size);
    aria::Decoder dec(stringPiece);
    dec >> tid >> tid_offset >> epoch >> is_write;

    DCHECK(dec.size() == 0);

    bool waw = false, war = false, raw = false;

    if (is_write) {

      // analyze war and waw
      uint64_t reserve_epoch = AriaHelper::get_epoch(metadata);
      uint64_t reserve_rts = AriaHelper::get_rts(metadata);
      uint64_t reserve_wts = AriaHelper::get_wts(metadata);
      DCHECK(reserve_epoch == epoch);

      if (reserve_epoch == epoch && reserve_rts < tid && reserve_rts != 0) {
        war = true;
      }
      if (reserve_epoch == epoch && reserve_wts < tid && reserve_wts != 0) {
        waw = true;
      }
    } else {
      // analyze raw
      uint64_t reserve_epoch = AriaHelper::get_epoch(metadata);
      uint64_t reserve_wts = AriaHelper::get_wts(metadata);
      DCHECK(reserve_epoch == epoch);

      if (reserve_epoch == epoch && reserve_wts < tid && reserve_wts != 0) {
        raw = true;
      }
    }

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(tid) +
                        sizeof(tid_offset) + sizeof(bool) * 4;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(AriaMessage::CHECK_RESPONSE), message_size,
        table_id, partition_id);

    aria::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << tid << tid_offset << is_write << waw << war << raw;
    responseMessage.flush();
  }

  static void
  check_response_handler(MessagePiece inputPiece, Message &responseMessage,
                         ITable &table,
                         std::vector<std::unique_ptr<Transaction>> &txns) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(AriaMessage::CHECK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());

    /*
     * The structure of a check response: (tid, tid_offset, is_write, waw, war,
     * raw)
     */

    uint32_t tid, tid_offset;
    bool is_write;
    bool waw, war, raw;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(tid) + sizeof(tid_offset) +
               4 * sizeof(bool));

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> tid >> tid_offset >> is_write >> waw >> war >> raw;

    CHECK(tid_offset >= 0 && tid_offset < txns.size());
    CHECK(txns[tid_offset]->id == tid);

    if (is_write) {

      // analyze war and waw
      if (war) {
        txns[tid_offset]->war = true;
      }
      if (waw) {
        txns[tid_offset]->waw = true;
      }

    } else {
      // analyze raw
      if (raw) {
        txns[tid_offset]->raw = true;
      }
    }

    txns[tid_offset]->pendingResponses--;
    txns[tid_offset]->network_size += inputPiece.get_message_length();
  }

  static void
  write_request_handler(MessagePiece inputPiece, Message &responseMessage,
                        ITable &table,
                        std::vector<std::unique_ptr<Transaction>> &txns) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(AriaMessage::WRITE_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a write request: (primary key, field value)
     * The structure of a write response: null
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + field_size);

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    auto valueStringPiece = stringPiece;
    stringPiece.remove_prefix(field_size);

    table.deserialize_value(key, valueStringPiece);
  }

  static std::vector<
      std::function<void(MessagePiece, Message &, ITable &,
                         std::vector<std::unique_ptr<Transaction>> &)>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, ITable &,
                           std::vector<std::unique_ptr<Transaction>> &)>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(search_request_handler);
    v.push_back(search_response_handler);
    v.push_back(reserve_request_handler);
    v.push_back(check_request_handler);
    v.push_back(check_response_handler);
    v.push_back(write_request_handler);
    return v;
  }
};

} // namespace aria