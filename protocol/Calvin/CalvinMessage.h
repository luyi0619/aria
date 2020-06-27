//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Calvin/CalvinRWKey.h"
#include "protocol/Calvin/CalvinTransaction.h"

namespace aria {

enum class CalvinMessage {
  READ_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  NFIELDS
};

class CalvinMessageFactory {

public:
  static std::size_t new_read_message(Message &message, ITable &table,
                                      uint32_t tid, uint32_t key_offset,
                                      const void *value) {

    /*
     * The structure of a read request: (tid, key offset, value)
     */

    auto value_size = table.value_size();

    auto message_size = MessagePiece::get_header_size() + sizeof(tid) +
                        sizeof(key_offset) + value_size;

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(CalvinMessage::READ_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << tid << key_offset;
    encoder.write_n_bytes(value, value_size);
    message.flush();
    return message_size;
  }
};

class CalvinMessageHandler {
  using Transaction = CalvinTransaction;

public:
  static void
  read_request_handler(MessagePiece inputPiece, Message &responseMessage,
                       ITable &table,
                       std::vector<std::unique_ptr<Transaction>> &txns) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(CalvinMessage::READ_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto value_size = table.value_size();

    /*
     * The structure of a read request: (tid, key offset, value)
     * The structure of a read response: null
     */

    uint32_t tid;
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(tid) + sizeof(key_offset) +
               value_size);

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> tid >> key_offset;
    DCHECK(tid < txns.size());
    DCHECK(key_offset < txns[tid]->readSet.size());
    CalvinRWKey &readKey = txns[tid]->readSet[key_offset];
    dec.read_n_bytes(readKey.get_value(), value_size);
    txns[tid]->remote_read.fetch_add(-1);
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
    return v;
  }
};

} // namespace aria