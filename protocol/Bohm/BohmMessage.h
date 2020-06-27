//
// Created by Yi Lu on 2019-09-05.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Bohm/BohmRWKey.h"
#include "protocol/Bohm/BohmTransaction.h"

namespace aria {

enum class BohmMessage {
  READ_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  READ_RESPONSE,
  WRITE_REQUEST,
  NFIELDS
};

class BohmMessageFactory {
public:
  static std::size_t new_read_message(Message &message, ITable &table,
                                      uint64_t tid, uint32_t key_offset,
                                      const void *key) {
    /*
     * The structure of a read request: (primary key, key offset, tid)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        sizeof(key_offset) + sizeof(tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(BohmMessage::READ_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset << tid;
    message.flush();
    return message_size;
  }

  static std::size_t new_write_message(Message &message, ITable &table,
                                       uint64_t tid, const void *key,
                                       const void *value) {

    /*
     * The structure of a write request: (primary key, field value, tid)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + field_size + sizeof(tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(BohmMessage::WRITE_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << tid;
    message.flush();
    return message_size;
  }
};

class BohmMessageHandler {
  using Transaction = BohmTransaction;

public:
  static void
  read_request_handler(MessagePiece inputPiece, Message &responseMessage,
                       ITable &table,
                       std::vector<std::unique_ptr<Transaction>> &txns) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(BohmMessage::READ_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read request: (primary key, key offset, tid)
     * The structure of a read response: (success?, key offset, tid, value?)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;
    uint64_t tid;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset) +
               sizeof(tid));

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    aria::Decoder dec(stringPiece);
    dec >> key_offset >> tid;
    DCHECK(dec.size() == 0);

    auto row = table.search_prev(key, tid);
    std::atomic<uint64_t> &placeholder = *std::get<0>(row);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool) +
                        sizeof(key_offset) + sizeof(tid);

    bool success = BohmHelper::is_placeholder_ready(placeholder);
    if (success) {
      message_size += value_size;
    }

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(BohmMessage::READ_RESPONSE), message_size,
        table_id, partition_id);

    aria::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << key_offset << tid;

    if (success) {
      // reserve size for read
      responseMessage.data.append(value_size, 0);
      void *dest =
          &responseMessage.data[0] + responseMessage.data.size() - value_size;
      // read to message buffer
      BohmHelper::read(row, dest, value_size);
    }

    responseMessage.flush();
  }

  static void
  read_response_handler(MessagePiece inputPiece, Message &responseMessage,
                        ITable &table,
                        std::vector<std::unique_ptr<Transaction>> &txns) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(BohmMessage::READ_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read response: (success?, key offset, tid, value?)
     */

    bool success;
    uint32_t key_offset;
    uint64_t tid;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success >> key_offset >> tid;

    auto pos = BohmHelper::get_pos(tid);

    if (success) {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset) + sizeof(tid) + value_size);

      BohmRWKey &readKey = txns[pos]->readSet[key_offset];
      readKey.clear_read_request_bit();
      dec.read_n_bytes(readKey.get_value(), value_size);
      txns[pos]->remote_read.fetch_add(-1);
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset) + sizeof(tid));
      txns[pos]->abort_read_not_ready = true;
    }

    txns[pos]->pendingResponses--;
    txns[pos]->network_size += inputPiece.get_message_length();
  }

  static void
  write_request_handler(MessagePiece inputPiece, Message &responseMessage,
                        ITable &table,
                        std::vector<std::unique_ptr<Transaction>> &txns) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(BohmMessage::WRITE_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a write request: (primary key, field value, tid)
     */

    uint64_t tid;

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  key_size + field_size +
                                                  sizeof(tid));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    auto valueStringPiece = stringPiece;
    stringPiece.remove_prefix(field_size);

    Decoder dec(stringPiece);
    dec >> tid;

    DCHECK(dec.size() == 0);
    std::atomic<uint64_t> &placeholder = table.search_metadata(key, tid);
    CHECK(BohmHelper::is_placeholder_ready(placeholder) == false);
    table.deserialize_value(key, valueStringPiece, tid);
    BohmHelper::set_placeholder_to_ready(placeholder);
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
    v.push_back(read_request_handler);
    v.push_back(read_response_handler);
    v.push_back(write_request_handler);
    return v;
  }
};

} // namespace aria