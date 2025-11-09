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
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetFatherPageId(INVALID_PAGE_ID);
  SetPageId(INVALID_PAGE_ID);
  num_tombstones_ = 0;
  pre_page_id_=INVALID_PAGE_ID;
  next_page_id_=INVALID_PAGE_ID;
  is_begin = false;
  is_update_ = false;
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetNumTombstones(size_t num) -> void {num_tombstones_=num;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNumTombstones()const -> size_t {
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPrePageId() const -> page_id_t {
  return pre_page_id_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}


FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPrePageId(page_id_t pre_page_id) {
  pre_page_id_= pre_page_id;
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return rid_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetMinKey() -> KeyType {
  return key_array_[0];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsBegin() -> bool {
  return is_begin;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetBegin(bool set) -> void {
    is_begin=set;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertKeyValue(const KeyComparator &comparator, const KeyType &key,
                                                const ValueType &value) -> bool {
  if (GetSize() == 0) {
    key_array_[0] = key;
    rid_array_[0] = value;
  }else {
    auto index = BinarySearch(comparator, key);
    if (index==0) {
      is_begin=true;
    }
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

FULL_INDEX_TEMPLATE_ARGUMENTS//  用于找到insert的位置
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MatchKey(KeyType key, const KeyComparator &comparator) -> int {
  int begin = 0;
  int end = GetSize() - 1;
  while (begin <= end) {
    int mid = (end - begin) / 2 + begin;
    int res = comparator(key_array_[mid], key);
    if (res > 0) {
      end = mid - 1;
    } else if (res < 0) {
      begin = mid + 1;
    } else {
      if (LEAF_PAGE_TOMB_CNT > 0 && IsTombstone(mid)) {
        return -1;
      }
      return mid;
    }
  }
  return -1;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType key, const KeyComparator &comparator) {
  auto index= MatchKey(key,comparator);
  if (index == -1) {
    return ;
  }
  // 当墓碑数组大小为0时，直接进行物理删除（乐观删除）
  if (LEAF_PAGE_TOMB_CNT == 0) {
    // 物理删除：移动数组元素
    // 保存删除前的大小，避免在循环中重复调用GetSize()
    int current_size = GetSize();
    for (int i = index; i < current_size-1; i++) {
      key_array_[i] = key_array_[i + 1];
      rid_array_[i] = rid_array_[i + 1];
    }
    ChangeSizeBy(-1);
    if (index == 0) {
      if (!is_update_) {
        before_first_key_ = key;
      }
      is_update_ = true;
    }
    return;
  }
  // 乐观删除 标记墓碑 不物理删除
  MarkTomb(index);
  if (index==0) {//TODO is_update重置
    is_update_=true;
  }
  ChangeSizeBy(-1);
}

//吸收函数 将page里的键值对吸收到末尾 但是不做删除处理 外部负责处理
FULL_INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::Absorb(B_PLUS_TREE_LEAF_PAGE_TYPE *page) {
  auto begin_key=page->KeyAt(0);
  for (int i=0;i<page->GetSize();i++) {
    InsertBack(std::make_pair(page->key_array_[i], page->rid_array_[i]));
  }
  //更新大小
  page->ChangeSizeBy(-page->GetSize());
  return begin_key;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MarkTomb(int index) {
    tombstones_[GetNumTombstones()]=index;
    SetNumTombstones(GetNumTombstones()+1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::IsTombstone(int index) const {
  // 检查index是否在tombstones_数组中
  for (size_t i = 0; i < GetNumTombstones(); i++) {
    if (tombstones_[i] == static_cast<size_t>(index)) {
      return true;
    }
  }
  return false;
}


FULL_INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::IsUpdate(){
  return is_update_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::IsEmpty(){
  if (GetSize()==0) {
    return true;
  }else {
    return false;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CleanupTombs() {
  //v
  // 创建临时数组，只存储有效键 跳过墓碑
  KeyType new_key_array[LEAF_PAGE_SLOT_CNT];
  ValueType new_rid_array[LEAF_PAGE_SLOT_CNT];
  int new_size = 0;

  //  遍历所有键 只保留非墓碑的键
  for (int i = 0; i < GetSize(); i++) {
    if (!IsTombstone(i)) {
      new_key_array[new_size] = key_array_[i];
      new_rid_array[new_size] = rid_array_[i];
      new_size++;
    }
  }

  //  将新数组复制回原数组
  for (int i = 0; i < new_size; i++) {
    key_array_[i] = new_key_array[i];
    rid_array_[i] = new_rid_array[i];
  }
  num_tombstones_ = 0;
}

//当查找不到时 直接返回索引0上的值
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::FindAndPush(const KeyComparator &comparator,
                                      const KeyType &key, std::vector<ValueType> *result) const {
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
      result->push_back(rid_array_[mid]);
      begin=mid+1;
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertBegin(std::pair<KeyType, ValueType> pair){
    for (int i=GetSize()-1;i>=0;i--) {
      key_array_[i+1]=key_array_[i];
      rid_array_[i+1]=rid_array_[i];
    }
  key_array_[0]=pair.first;
  rid_array_[0]=pair.second;
  ChangeSizeBy(1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertBack(std::pair<KeyType, ValueType> pair){
  key_array_[GetSize()]=pair.first;
  rid_array_[GetSize()]=pair.second;
  ChangeSizeBy(1);
}

//键值对删除然后返回出来
FULL_INDEX_TEMPLATE_ARGUMENTS
std::pair<KeyType, ValueType> B_PLUS_TREE_LEAF_PAGE_TYPE::PopBack() {
  ChangeSizeBy(-1);
  return std::make_pair(key_array_[GetSize()],rid_array_[GetSize()]);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
std::pair<KeyType, ValueType> B_PLUS_TREE_LEAF_PAGE_TYPE::PopFront() {
  auto pair=std::make_pair(key_array_[0],(rid_array_[0]));
  for (int i=0;i<GetSize();i++) {
    key_array_[i]=key_array_[i+1];
    rid_array_[i]=rid_array_[i+1];
  }
  ChangeSizeBy(-1);
  return pair;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetBeforeFirstKey() const -> KeyType {
  return before_first_key_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Split(B_PLUS_TREE_LEAF_PAGE_TYPE* new_leaf_page) {
  //设置其 next_page_id_ 为原节点的 next_page_id_ 这里更新三个页的前后页关系
  new_leaf_page->next_page_id_=next_page_id_;
  new_leaf_page->pre_page_id_=GetPageId();//1
  next_page_id_=new_leaf_page->GetPageId();//2


  for (int mid=GetMaxSize()/2;mid<GetMaxSize();mid++) {
    //由于已经是有序的 新叶子页所以直接顺序加入
    new_leaf_page->key_array_[new_leaf_page->GetSize()]=key_array_[mid];
    new_leaf_page->rid_array_[new_leaf_page->GetSize()]=rid_array_[mid];
    new_leaf_page->ChangeSizeBy(1);
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