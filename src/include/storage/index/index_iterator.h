//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>
#define SHORT_INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator();

  IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, page_id_t page_id);

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (itr.page_id_==page_id_&&index_==itr.index_) {
      return true;
    }
    return false;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    if (itr.page_id_==page_id_&&index_==itr.index_) {
      return false;
    }
    return true;
  }

private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE* page_;

  WritePageGuard guard_;

  page_id_t page_id_;

  int index_;

  std::shared_ptr<TracedBufferPoolManager> bpm_;

  // Store current key and value to avoid returning references to temporary objects
  KeyType current_key_;
  ValueType current_value_;

};

}  // namespace bustub
