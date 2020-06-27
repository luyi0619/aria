//
// Created by Yi Lu on 7/13/18.
//

#pragma once

#include <array>
#include <iostream>
#include <string>

#include "ClassOf.h"
#include "Hash.h"
#include "Serialization.h"
#include "StringPiece.h"

namespace aria {

template <std::size_t N> class FixedString {
public:
  static_assert(N > 0, "string length should be positive.");

  using size_type = std::size_t;

  FixedString() { assign(""); }

  FixedString(const char *str) { assign(std::string(str)); }

  FixedString(const std::string &str) { assign(str); }

  int compare(const FixedString &that) const {

    for (auto i = 0u; i < N; i++) {
      if (data_[i] < that.data_[i]) {
        return -1;
      }

      if (data_[i] > that.data_[i]) {
        return 1;
      }
    }
    return 0;
  }

  bool operator<(const FixedString &that) const { return compare(that) < 0; }

  bool operator<=(const FixedString &that) const { return compare(that) <= 0; }

  bool operator>(const FixedString &that) const { return compare(that) > 0; }

  bool operator>=(const FixedString &that) const { return compare(that) >= 0; }

  bool operator==(const FixedString &that) const { return compare(that) == 0; }

  bool operator!=(const FixedString &that) const { return compare(that) != 0; }

  FixedString &assign(const std::string &str) {
    return assign(str, str.length());
  }

  FixedString &assign(const std::string &str, size_type length) {
    DCHECK(length <= str.length());
    DCHECK(length <= N);
    std::copy(str.begin(), str.begin() + length, data_.begin());
    DCHECK(data_.begin() + length <= data_.end() - 1);
    std::fill(data_.begin() + length, data_.end() - 1, ' ');
    data_[N] = 0;
    return *this;
  }

  const char *c_str() { return &data_[0]; }

  std::size_t hash_code() const {
    std::hash<char> h;
    std::size_t hashCode = 0;
    for (auto i = 0u; i < N; i++) {
      hashCode = aria::hash_combine(hashCode, h(data_[i]));
    }
    return hashCode;
  }

  constexpr size_type length() const { return N; }

  constexpr size_type size() const { return N; }

  std::string toString() const {
    std::string str;
    // the last char is \0
    std::copy(data_.begin(), data_.end() - 1, std::back_inserter(str));
    DCHECK(str.length() == N);
    return str;
  }

private:
  std::array<char, N + 1> data_;
};

template <class C, std::size_t N>
inline std::basic_ostream<C> &operator<<(std::basic_ostream<C> &os,
                                         const FixedString<N> &str) {
  os << str.toString();
  return os;
}

template <std::size_t N> class Serializer<FixedString<N>> {
public:
  std::string operator()(const FixedString<N> &v) { return v.toString(); }
};

template <std::size_t N> class Deserializer<FixedString<N>> {
public:
  std::size_t operator()(StringPiece str, FixedString<N> &result) const {
    result.assign(str.data(), N);
    return N;
  }
};

template <std::size_t N> class ClassOf<FixedString<N>> {
public:
  static constexpr std::size_t size() { return N; }
};

} // namespace aria

namespace std {
template <std::size_t N> struct hash<aria::FixedString<N>> {
  std::size_t operator()(const aria::FixedString<N> &k) const {
    return k.hash_code();
  }
};
} // namespace std
