//
// Created by Yi Lu on 8/28/18.
//

#pragma once

#include <cstring>
#include <string>

#include <glog/logging.h>

/*
 *  StringPiece is adapted from
 *  https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/stubs/stringpiece.h
 */

namespace aria {

class StringPiece {

public:
  using iterator = const char *;
  using size_type = std::size_t;

  StringPiece() : data_(nullptr), length_(0) {}

  StringPiece(const char *str) : data_(str), length_(0) {
    if (data_ != nullptr) {
      length_ = strlen(data_);
    }
  }

  StringPiece(const char *str, size_type length)
      : data_(str), length_(length) {}

  StringPiece(const std::string &str)
      : data_(str.data()), length_(str.length()) {}

  StringPiece(const StringPiece &that)
      : data_(that.data_), length_(that.length_) {}

  const char *data() const { return data_; }

  size_type size() const { return length_; }

  size_type length() const { return length_; }

  bool empty() const { return length_ == 0; }

  void clear() {
    data_ = nullptr;
    length_ = 0;
  }

  void set(const char *data, size_type length) {
    data_ = data;
    length_ = length;
  }

  void set(const char *data) {
    data_ = data;
    if (data_ == nullptr) {
      length_ = 0;
    } else {
      length_ = strlen(data);
    }
  }

  char operator[](size_type i) const {
    DCHECK(i < length_);
    return data_[i];
  }

  void remove_prefix(size_type len) {
    DCHECK(len <= length_);
    data_ += len;
    length_ -= len;
  }

  void remove_suffix(size_type len) {
    DCHECK(len <= length_);
    length_ -= len;
  }

  int compare(const StringPiece &that) const {
    size_type minSize = length_ < that.length_ ? length_ : that.length_;
    int r = strncmp(data_, that.data_, minSize);
    if (r < 0)
      return -1;
    if (r > 0)
      return 1;
    if (length_ < that.length_)
      return -1;
    if (length_ > that.length_)
      return 1;
    return 0;
  }

  bool operator<(const StringPiece &that) const { return compare(that) < 0; }

  bool operator<=(const StringPiece &that) const { return compare(that) <= 0; }

  bool operator>(const StringPiece &that) const { return compare(that) > 0; }

  bool operator>=(const StringPiece &that) const { return compare(that) >= 0; }

  bool operator==(const StringPiece &that) const { return compare(that) == 0; }

  bool operator!=(const StringPiece &that) const { return compare(that) != 0; }

  std::string toString() const {
    if (data_ == nullptr)
      return std::string();
    else
      return std::string(data_, length_);
  }

  iterator begin() const { return data_; }

  iterator end() const { return data_ + length_; }

private:
  const char *data_;
  size_t length_;
};
} // namespace aria