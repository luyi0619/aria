//
// Created by Yi Lu on 7/17/18.
//

#pragma once

#include <cstring>
#include <string>

#include "StringPiece.h"

namespace aria {
template <class T> class Serializer {
public:
  std::string operator()(const T &v) {
    std::string result(sizeof(T), 0);
    memcpy(&result[0], &v, sizeof(T));
    return result;
  }
};

template <class T> class Deserializer {
public:
  std::size_t operator()(StringPiece str, T &result) const {
    std::memcpy(&result, str.data(), sizeof(T));
    return sizeof(T);
  }
};

template <> class Serializer<std::string> {
public:
  std::string operator()(const std::string &v) {
    return Serializer<std::string::size_type>()(v.size()) + v;
  }
};

template <> class Deserializer<std::string> {
public:
  std::size_t operator()(StringPiece str, std::string &result) const {
    std::string::size_type string_length;
    std::size_t size =
        Deserializer<std::string::size_type>()(str, string_length);
    size += string_length;
    str.remove_prefix(sizeof(string_length));
    result = std::string(str.begin(), str.begin() + string_length);
    return size;
  }
};

} // namespace aria
