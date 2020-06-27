//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"

#include <vector>

namespace aria {

enum class ControlMessage { STATISTICS, VECTOR, SIGNAL, ACK, STOP, NFIELDS };

class ControlMessageFactory {

public:
  static std::size_t new_statistics_message(Message &message, double value) {
    /*
     * The structure of a statistics message: (statistics value : double)
     *
     */

    // the message is not associated with a table or a partition, use 0.
    auto message_size = MessagePiece::get_header_size() + sizeof(double);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::STATISTICS), message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << value;
    message.flush();
    return message_size;
  }

  static std::size_t new_vector_message(Message &message,
                                        const std::vector<int> &v) {
    /*
     * The structure of a vector message: (vector value : v)
     *
     */

    // the message is not associated with a table or a partition, use 0.
    auto message_size = MessagePiece::get_header_size() +
                        sizeof(decltype(v.size())) + sizeof(int) * v.size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::VECTOR), message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << v.size();
    for (auto i = 0u; i < v.size(); i++) {
      encoder << v[i];
    }
    message.flush();
    return message_size;
  }

  static std::size_t new_signal_message(Message &message, uint32_t value) {

    /*
     * The structure of a signal message: (signal value : uint32_t)
     */

    // the message is not associated with a table or a partition, use 0.
    auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::SIGNAL), message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << value;
    message.flush();
    return message_size;
  }

  static std::size_t new_ack_message(Message &message) {
    /*
     * The structure of an ack message: ()
     */

    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::ACK), message_size, 0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header;
    message.flush();
    return message_size;
  }

  static std::size_t new_stop_message(Message &message) {
    /*
     * The structure of a stop message: ()
     */

    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::STOP), message_size, 0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header;
    message.flush();
    return message_size;
  }
};

} // namespace aria
