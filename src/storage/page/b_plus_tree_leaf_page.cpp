//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
@brief 创建新叶子页后的初始化方法
从缓冲池创建新的叶子页后，必须调用初始化方法来设置默认值，
包括设置页类型、将当前大小设为零、设置页 ID / 父 ID、设置
下一页 ID 以及设置最大大小。
@param max_size 叶子节点的最大大小
*/
//TODO(wwz): next_page_id_ 页id 父页id还未初始化
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetFatherPageId(INVALID_PAGE_ID);
  SetPageId(INVALID_PAGE_ID);
  num_tombstones_ = 0;
  prev_page_id_=INVALID_PAGE_ID;
  next_page_id_=INVALID_PAGE_ID;
}

/**
@brief 获取页面墓碑的辅助函数。
@return 此页面中按时间顺序（最旧的在前面）排列的最后NumTombs个带有待删除标记的键。
*/
//TODO(wwz): 需求有点不明确
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> v;
  for (size_t i = 0; i < num_tombstones_; i++) {
    v.push_back(key_array_[tombstones_[i]]);
  }
  return v;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNumTombstones() -> size_t {
  return num_tombstones_;
}

/**
 * Helper methods to set/get next page id
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return key_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetMinKey() -> KeyType {
  return key_array_[0];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertKeyValue(const KeyComparator &comparator, const KeyType &key,
                                                const ValueType &value) -> bool {
  if (GetSize() == 0) {
    key_array_[0] = key;
    rid_array_[0] = value;
  }else {
    auto index = BinarySearch(comparator, key);
    if (index==-1) {
      return false;
    }
    if (index == GetSize()) {
      key_array_[index] = key;
      rid_array_[index] = value;
    } else {
      for (int i = GetSize() - 1; i >= index; i--) {
        key_array_[i + 1] = key_array_[i];
        rid_array_[i + 1] = rid_array_[i];
      }
      key_array_[index] = key;
      rid_array_[index] = value;
    }
  }
  ChangeSizeBy(1);
  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::BinarySearch(const KeyComparator &comparator,
                                              const KeyType &key) -> int {
  int begin = 0;
  int end = GetSize() - 1;
  int result = GetSize();
  while (begin <= end) {
    int mid = (end - begin) / 2 + begin;
    int res=comparator(key_array_[mid], key);
    if (res > 0) {
      end = mid - 1;
      result = mid;
    } else if (res < 0){
      begin = mid + 1;
    }else {
      return -1;
    }
  }
  return result;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Split(B_PLUS_TREE_LEAF_PAGE_TYPE* new_leaf_page) {
  //设置其 next_page_id_ 为原节点的 next_page_id_
  new_leaf_page->next_page_id_=next_page_id_;
  new_leaf_page->prev_page_id_=GetPageId();
  next_page_id_=new_leaf_page->GetPageId();
  for (int mid=GetMaxSize()/2;mid<GetMaxSize();mid++) {
    //由于已经是有序的 新叶子页所以直接顺序加入
    new_leaf_page->key_array_[GetSize()]=key_array_[mid];
    new_leaf_page->rid_array_[GetSize()]=rid_array_[mid];
    new_leaf_page->ChangeSizeBy(1);
    //被移出去的键值对加入墓碑数组
    tombstones_[GetNumTombstones()]=mid;
    num_tombstones_++;
    //同时更新size
    ChangeSizeBy(-1);
  }
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
} // namespace bustub