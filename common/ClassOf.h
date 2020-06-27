//
// Created by Yi Lu on 9/5/18.
//

#pragma once

#include <cstddef>

namespace aria {
template <class T> class ClassOf {
public:
  static constexpr std::size_t size() { return sizeof(T); }
};
} // namespace aria