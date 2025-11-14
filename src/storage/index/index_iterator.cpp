//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, page_id_t page_id) {
  if (page_id==INVALID_PAGE_ID) {
    page_id_=page_id;
    index_=-1;
    return;
  }
  bpm_=bpm;
  page_id_=page_id;
  guard_=bpm_->WritePage(page_id);
  page_=guard_.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  index_=0;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (page_id_==-1) {
    return true;
  }
  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  // Store key and value in member variables to avoid returning references to temporary objects
  current_key_ = (*page_).KeyAt(index_);
  current_value_ = (*page_).ValueAt(index_);
  return {current_key_, current_value_};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  //跳过墓碑
  while (page_->IsTombstone(index_)&&index_ <page_->GetSize()) {
    index_++;
  }

    //切换到下一页
  if (index_>=page_->GetSize()) {
    page_id_t next_page_id = page_->GetNextPageId();
    //没有下一页了
    if (next_page_id == INVALID_PAGE_ID) {
      page_id_ =INVALID_PAGE_ID;
      index_=-1;
      return *this;
    }

    guard_ = bpm_->WritePage(next_page_id);
    page_ = guard_.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    index_ = 0;
    page_id_=next_page_id;

    //跳过墓碑
    while (index_ <page_->GetSize() &&page_->IsTombstone(index_)) {
      index_++;
    }
  }


  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
