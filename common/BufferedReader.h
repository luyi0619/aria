//
// Created by Yi Lu on 8/30/18.
//

#pragma once

#include "common/Message.h"
#include "common/Socket.h"

#include <glog/logging.h>

namespace aria {
class BufferedReader {
public:
  BufferedReader(Socket &socket)
      : socket(&socket), bytes_read(0), bytes_total(0) {}

  // BufferedReader is not copyable
  BufferedReader(const BufferedReader &) = delete;

  BufferedReader &operator=(const BufferedReader &) = delete;

  // BufferedReader is movable

  BufferedReader(BufferedReader &&that)
      : socket(that.socket), bytes_read(that.bytes_read),
        bytes_total(that.bytes_total) {
    that.socket = nullptr;
    that.bytes_read = 0;
    that.bytes_total = 0;
  }

  BufferedReader &operator=(BufferedReader &&that) {
    socket = that.socket;
    bytes_read = that.bytes_read;
    bytes_total = that.bytes_total;

    that.socket = nullptr;
    that.bytes_read = 0;
    that.bytes_total = 0;
    return *this;
  }

  std::unique_ptr<Message> next_message() {
    DCHECK(socket != nullptr);

    fetch_message();
    if (!has_message()) {
      return nullptr;
    }

    // read header and deadbeef;
    auto header =
        *reinterpret_cast<Message::header_type *>(buffer + bytes_read);
    auto deadbeef = *reinterpret_cast<Message::deadbeef_type *>(
        buffer + bytes_read + sizeof(header));

    // check deadbeaf
    DCHECK(deadbeef == Message::DEADBEEF);
    auto message = std::make_unique<Message>();
    auto length = Message::get_message_length(header);
    message->resize(length);

    // copy the data
    DCHECK(bytes_read + length <= bytes_total);
    std::memcpy(message->get_raw_ptr(), buffer + bytes_read, length);
    bytes_read += length;
    DCHECK(bytes_read <= bytes_total);

    return message;
  }

private:
  void fetch_message() {
    DCHECK(socket != nullptr);

    // return if there is a message left
    if (has_message()) {
      return;
    }

    // copy left bytes
    DCHECK(bytes_read <= bytes_total);
    auto bytes_left = bytes_total - bytes_read;
    bytes_total = 0;

    if (bytes_left > 0 && bytes_read > 0) {

      if (bytes_left <= bytes_read) { // non overlapping
        std::memcpy(buffer, buffer + bytes_read, bytes_left);
      } else {
        for (auto i = 0u; i < bytes_left; i++) {
          buffer[i] = buffer[i + bytes_read];
        }
      }
    }
    bytes_total += bytes_left;
    bytes_read = 0;

    // read new message

    auto bytes_received =
        socket->read_async(buffer + bytes_total, BUFFER_SIZE - bytes_total);

    if (bytes_received > 0) {
      // successful read
      bytes_total += bytes_received;
    }
  }

  bool has_message() {
    // check if the buffer has a message header
    if (bytes_read + Message::get_prefix_size() > bytes_total) {
      return false;
    }

    // read header and deadbeef;
    auto header =
        *reinterpret_cast<Message::header_type *>(buffer + bytes_read);
    auto deadbeef = *reinterpret_cast<Message::deadbeef_type *>(
        buffer + bytes_read + sizeof(header));

    // check deadbeaf
    DCHECK(deadbeef == Message::DEADBEEF);

    // check if the buffer has a message
    return bytes_read + Message::get_message_length(header) <= bytes_total;
  }

public:
  static constexpr uint32_t BUFFER_SIZE = 1024 * 1024 * 4; // 4MB

private:
  Socket *socket;
  char buffer[BUFFER_SIZE];
  std::size_t bytes_read, bytes_total;
};
} // namespace aria
