//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"
#include <sys/types.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  if (width == 0 || depth == 0) {
    throw std::invalid_argument("width or depth = 0");
  }
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  matrix_.resize(depth_);
  for (size_t i = 0; i < depth_; i++) {
    for (size_t j = 0; j < width_; j++) {
      matrix_[i].emplace_back(std::make_unique<Element>());
    }
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  /** @TODO(student) Implement this function! */
  this->matrix_ = std::move(other.matrix_);
  this->hash_functions_ = std::move(other.hash_functions_);
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  this->depth_ = other.depth_;
  this->width_ = other.width_;

  this->matrix_ = std::move(other.matrix_);
  this->hash_functions_ = std::move(other.hash_functions_);
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  for (size_t i = 0; i < depth_; i++) {
    auto &temp = matrix_[i][hash_functions_[i](item) % width_];
    std::unique_lock<std::mutex> lock(temp->mutex_);
    temp->count_ += 1;
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < other.depth_; i++) {
    for (size_t j = 0; j < other.width_; j++) {
      std::unique_lock<std::mutex> lock(this->matrix_[i][j]->mutex_);
      std::unique_lock<std::mutex> lock_other(other.matrix_[i][j]->mutex_);
      this->matrix_[i][j]->count_ += other.matrix_[i][j]->count_;
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t result = UINT32_MAX;
  for (size_t i = 0; i < this->depth_; i++) {
    auto &temp = this->matrix_[i][hash_functions_[i](item) % width_];
    std::unique_lock<std::mutex> lock(temp->mutex_);
    result = std::min(result, temp->count_);
  }
  return result;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < this->depth_; i++) {
    for (size_t j = 0; j < this->width_; j++) {
      std::unique_lock<std::mutex> lock(this->matrix_[i][j]->mutex_);
      this->matrix_[i][j]->count_ = 0;
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  if (k > candidates.size()) {
    k = candidates.size();
  }
  std::vector<std::pair<KeyType, uint32_t>> temp;

  temp.reserve(candidates.size());
  for (const auto &item : candidates) {
    temp.push_back(std::make_pair(item, Count(item)));
  }

  auto desc_by_count = [](const auto &p1, const auto &p2) { return p1.second > p2.second; };
  std::sort(temp.begin(), temp.end(), desc_by_count);
  std::vector<std::pair<KeyType, uint32_t>> result(temp.begin(), temp.begin() + k);
  return result;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
