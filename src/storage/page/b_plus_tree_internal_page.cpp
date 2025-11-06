//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
@brief 创建新的内部页后的初始化方法。
向新创建的页写入必要的头部信息，
包括设置页类型、设置当前大小、设置页 ID、设置父 ID 以及设置最大页大小，
必须在创建新页后调用，以生成有效的 BPlusTreeInternalPage。
@param max_size 页的最大大小
*/
//TODO(wwz): 页类型 size 和maxsize已经设置   剩余页id 和父id
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(INVALID_PAGE_ID);
  SetFatherPageId(INVALID_PAGE_ID);
}

/**
@brief 辅助方法，用于获取 / 设置与输入 “index”（也称为数组偏移量）相关联的键。
@param index 要获取的键的索引。索引必须非零。
@return 索引处的键
*/
//TODO(wwz) index为0还没有处理
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return key_array_[index - 1];
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index == 0) {
    return;
  }
  key_array_[index - 1] = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndexForPage_id_t(const page_id_t &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return page_id_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::BinarySearch(const KeyComparator &comparator,
                                              const KeyType &key) const -> int {
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

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::FirstInsert(
    const KeyType &key, const ValueType &left_page_id, const ValueType &right_page_id) {
    key_array_[0]=key;
    page_id_array_[0]=left_page_id;
    page_id_array_[1]=right_page_id;
    ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyValue(const KeyComparator &comparator,
                                                    const KeyType &key, const ValueType &value) {
  //TODO(wwz): insert不处理满了的逻辑 只在未满的前提下进行插入
    auto index = BinarySearch(comparator, key);
    if (index==-1) {
      return false;
    }
    if (index == GetSize()) {
      key_array_[index] = key;
      page_id_array_[index] = value;
    } else {
      for (int i = GetSize() - 1; i >= index; i--) {
        key_array_[i + 1] = key_array_[i];
        page_id_array_[i + 1] = page_id_array_[i];
      }
      key_array_[index] = key;
      page_id_array_[index] = value;
    }
  ChangeSizeBy(1);
  return true;
}

//将内页里的key键值对 更新成传入叶子页的首键值对 对应左
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpdateKey(KeyType key, std::pair<KeyType,ValueType> pair, const KeyComparator &comparator)
  -> void {
  auto index=MatchKey(key,comparator);
    key_array_[index]=pair.first;
    page_id_array_[index]=pair.second;
}

//吸收函数 将page里的键值对吸收到末尾 但是不做删除处理 外部负责处理
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Absorb(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *page) -> KeyType {
  auto begin_key=page->KeyAt(0);
  for (int i=0;i<page->GetSize();i++) {
    InsertBack(std::make_pair(page->key_array_[i], page->page_id_array_[i]));
  }
  //更新大小
  page->ChangeSizeBy(-page->GetSize());
  return begin_key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertBack(std::pair<KeyType, page_id_t> pair){
  key_array_[GetSize()]=pair.first;
  page_id_array_[GetSize()]=pair.second;
  ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MatchKey(KeyType key, const KeyComparator &comparator) -> int {
  int begin = 0;
  int end = GetSize() - 1;
  while (begin <= end) {
    int mid = (end - begin) / 2 + begin;
    int res=comparator(key_array_[mid], key);
    if (res > 0) {
      end = mid - 1;
    } else if (res < 0){
      begin = mid + 1;
    }else {
      return mid;
    }
  }
  return -1;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopBack() -> std::pair<KeyType, ValueType> {
  ChangeSizeBy(-1);
  return std::make_pair(key_array_[GetSize()],page_id_array_[GetSize()]);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopFront() -> std::pair<KeyType, ValueType> {
  auto pair=std::make_pair(key_array_[0],page_id_array_[0]);
  for (int i=0;i<GetSize();i++) {
    key_array_[i]=key_array_[i+1];
    page_id_array_[i]=page_id_array_[i+1];
  }
  ChangeSizeBy(-1);
  return pair;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPrePageId(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* father_write) -> page_id_t {
  //返回这个页id的物理下标
  auto index= father_write->ValueIndexForPage_id_t(GetPageId());
  if (index==0) {
    return INVALID_PAGE_ID;
  }
  //直接定位到左页id
  return father_write->page_id_array_[index-1];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertBegin(std::pair<KeyType, ValueType> pair){
  for (int i=GetSize()-1;i>=0;i--) {
    key_array_[i+1]=key_array_[i];
    page_id_array_[i+1]=page_id_array_[i];
  }
  key_array_[0]=pair.first;
  page_id_array_[0]=pair.second;
  ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetNextPageId(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* father_write) -> page_id_t {
  //返回这个页id的物理下标
  auto index= father_write->ValueIndexForPage_id_t(GetPageId());
  if (index==0) {
    return INVALID_PAGE_ID;
  }
  //直接定位到左页id
  return father_write->page_id_array_[index+1];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Find(const KeyComparator& comparator, const KeyType &key) const -> page_id_t {
  auto index=BinarySearch(comparator,key);
  if (index==-1) {
    return -1;
  }
  return page_id_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeletePair(int index) {
  for (int i=index;i<GetSize();i++) {
    key_array_[i]=key_array_[i+1];
    page_id_array_[i]=page_id_array_[i+1];
  }
  ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS//只需要移动下一层的id 因为是链式结构
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(
    B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_internal_page, std::vector<page_id_t> &v) -> KeyType {
  for (int mid=GetMinSize();mid<GetMaxSize();mid++) {
    //由于已经是有序的 新内页所以直接顺序加入
    new_internal_page->key_array_[GetSize()]=key_array_[mid];
    new_internal_page->page_id_array_[GetSize()]=page_id_array_[mid];
    //更新叶子页的father_id为新的内页id
    v.push_back(page_id_array_[mid]);
    new_internal_page->ChangeSizeBy(1);
    //同时更新size 不需要物理删除 直接通过size来逻辑删除
    ChangeSizeBy(-1);
  }
  return key_array_[GetMinSize()];
}


// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
} // namespace bustub