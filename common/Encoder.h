//
// Created by Yi Lu on 7/17/18.
//

#pragma once

#include <iostream>
#include <string>

#include "Serialization.h"
#include "StringPiece.h"

namespace aria {
class Encoder {
public:
  Encoder(std::string &bytes) : bytes(bytes) {}

  template <class T> friend Encoder &operator<<(Encoder &enc, const T &rhs);

  StringPiece toStringPiece() {
    return StringPiece(bytes.data(), bytes.size());
  }

  void write_n_bytes(const void *ptr, std::size_t size) {
    bytes.append(static_cast<const char *>(ptr), size);
  }

  std::size_t size() { return bytes.size(); }

private:
  std::string &bytes;
};

template <class T> Encoder &operator<<(Encoder &enc, const T &rhs) {
  Serializer<T> serializer;
  enc.bytes += serializer(rhs);
  return enc;
}

class Decoder {
public:
  Decoder(StringPiece bytes) : bytes(bytes) {}

  template <class T> friend Decoder &operator>>(Decoder &dec, T &rhs);

  void read_n_bytes(void *ptr, std::size_t size) {
    DCHECK(bytes.size() >= size);
    std::memcpy(ptr, bytes.data(), size);
    bytes.remove_prefix(size);
  }

  std::size_t size() { return bytes.size(); }

private:
  StringPiece bytes;
};

template <class T> Decoder &operator>>(Decoder &dec, T &rhs) {
  Deserializer<T> deserializer;
  std::size_t size = deserializer(dec.bytes, rhs);
  dec.bytes.remove_prefix(size);
  return dec;
}
} // namespace aria
