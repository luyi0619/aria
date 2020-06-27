//
// Created by Yi Lu on 7/14/18.
//

#pragma once

#include "SpinLock.h"
#include <atomic>
#include <glog/logging.h>
#include <unordered_map>

namespace aria {

template <std::size_t N, class KeyType, class ValueType> class HashMap {

public:
  using HashMapType = std::unordered_map<KeyType, ValueType>;
  using HasherType = typename HashMapType::hasher;

public:
  bool remove(const KeyType &key) {
    return apply(
        [&key](HashMapType &map) {
          auto it = map.find(key);
          if (it == map.end()) {
            return false;
          } else {
            map.erase(it);
            return true;
          }
        },
        bucket_number(key));
  }

  bool contains(const KeyType &key) {
    return apply(
        [&key](const HashMapType &map) { return map.find(key) != map.end(); },
        bucket_number(key));
  }

  bool insert(const KeyType &key, const ValueType &value) {
    return apply(
        [&key, &value](HashMapType &map) {
          if (map.find(key) != map.end()) {
            return false;
          }
          map[key] = value;
          return true;
        },
        bucket_number(key));
  }

  ValueType &operator[](const KeyType &key) {
    return apply_ref(
        [&key](HashMapType &map) -> ValueType & { return map[key]; },
        bucket_number(key));
  }

  std::size_t size() {
    return fold(0, [](std::size_t totalSize, const HashMapType &map) {
      return totalSize + map.size();
    });
  }

  void clear() {
    map([](HashMapType &map) { map.clear(); });
  }

private:
  template <class ApplyFunc>
  auto &apply_ref(ApplyFunc applyFunc, std::size_t i) {
    DCHECK(i < N) << "index " << i << " is greater than " << N;
    locks[i].lock();
    auto &result = applyFunc(maps[i]);
    locks[i].unlock();
    return result;
  }

  template <class ApplyFunc> auto apply(ApplyFunc applyFunc, std::size_t i) {
    DCHECK(i < N) << "index " << i << " is greater than " << N;
    locks[i].lock();
    auto result = applyFunc(maps[i]);
    locks[i].unlock();
    return result;
  }

  template <class MapFunc> void map(MapFunc mapFunc) {
    for (auto i = 0u; i < N; i++) {
      locks[i].lock();
      mapFunc(maps[i]);
      locks[i].unlock();
    }
  }

  template <class T, class FoldFunc>
  auto fold(const T &firstValue, FoldFunc foldFunc) {
    T finalValue = firstValue;
    for (auto i = 0u; i < N; i++) {
      locks[i].lock();
      finalValue = foldFunc(finalValue, maps[i]);
      locks[i].unlock();
    }
    return finalValue;
  }

  auto bucket_number(const KeyType &key) { return hasher(key) % N; }

private:
  HasherType hasher;
  HashMapType maps[N];
  SpinLock locks[N];
};

} // namespace aria
