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
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> v;
  for (size_t i = 0; i < num_tombstones_; i++) {
    v.push_back(key_array_[tombstones_[i]]);
  }
  return v;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetNumTombstones(size_t num) -> void {
  num_tombstones_=num;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNumTombstones()const -> size_t {
  return num_tombstones_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetRealSize() const -> int {
  // 逻辑大小 = 有效键数量 = size_ - num_tombstones_
  // size_ 是叶子页物理存储的键总数（包括墓碑标记的键）
  // num_tombstones_ 是墓碑标记的已删除键数量
  // 逻辑大小 = 当前页中未被删除、可正常访问的有效键数量
  return GetSize() - static_cast<int>(num_tombstones_);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNeedUpdate() -> bool {
  return need_deep_update_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetNeedUpdate(bool set) -> void {
  need_deep_update_=set;
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
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
  if (GetSize() == 0) {
    key_array_[0] = key;
    rid_array_[0] = value;
    ChangeSizeBy(1);
    return true;
  }
  
  auto index = BinarySearch(comparator, key);
  if (index==0) {
    is_begin=true;
  }
  if (index==-1) {
    // 键已存在，检查是否是墓碑
    // 需要找到匹配的键的位置
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
        // 找到了匹配的键
        if (LEAF_PAGE_TOMB_CNT > 0 && IsTombstone(mid)) {
          // 键已被墓碑标记，清除墓碑并恢复键
          RemoveTombstone(mid);
          rid_array_[mid] = value;  // 更新值
          return true;
        }
        return false;  // 键已存在且不是墓碑
      }
    }
    return false;
  }
  
  // 插入新键
  int total_size = GetSize();  // size_ 已经是总大小，不需要再加 num_tombstones_
  
  // 先更新墓碑索引：如果墓碑索引 >= index，需要加1（因为要在index位置插入新元素）
  if (LEAF_PAGE_TOMB_CNT > 0) {
    for (size_t j = 0; j < GetNumTombstones(); j++) {
      if (tombstones_[j] >= static_cast<size_t>(index)) {
        tombstones_[j]++;
      }
    }
  }
  
  if (index == total_size) {
    // 插到末尾
    key_array_[total_size] = key;
    rid_array_[total_size] = value;
  } else {
    // 插入到中间位置，需要移动元素
    for (int i = total_size - 1; i >= index; i--) {
      key_array_[i + 1] = key_array_[i];
      rid_array_[i + 1] = rid_array_[i];
    }
    key_array_[index] = key;
    rid_array_[index] = value;
  }
  ChangeSizeBy(1);
  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS//  用于找到insert的位置
auto B_PLUS_TREE_LEAF_PAGE_TYPE::BinarySearch(const KeyComparator &comparator,
                                              const KeyType &key) -> int {
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
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
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
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
    //第一次注册
    if (index == 0) {
      if (!is_update_) {
        before_first_key_ = key;
      }
      is_update_ = true;
    }
    return;
  }
  // 根据文档：墓碑数组满了之后，应该等下一次标记墓碑删除时进行删除最靠前的那个墓碑元素
  // 所以在标记新墓碑之前，如果数组已经满了（有k个墓碑），先处理最早的墓碑
  bool processed_tombstone = false;
  int processed_tomb_index = -1;
  if (LEAF_PAGE_TOMB_CNT > 0 && GetNumTombstones() >= static_cast<size_t>(LEAF_PAGE_TOMB_CNT)) {
    // 保存要处理的墓碑索
    processed_tomb_index = static_cast<int>(tombstones_[0]);

    if (GetNumTombstones() == 0) {
      return;
    }

    // 最早的墓碑是tombstones_[0]（按删除顺序）
    int tomb_index = static_cast<int>(tombstones_[0]);

    // 保存被删除的键（用于更新父节点）
    KeyType deleted_key = key_array_[tomb_index];
    bool is_first_key = (tomb_index == 0);

    // 先调整所有剩余墓碑的索引：如果墓碑索引 > tomb_index，需要减1
    // 注意：必须在物理删除之前调整，因为物理删除会改变数组
    for (size_t i = 1; i < GetNumTombstones(); i++) {
      if (tombstones_[i] > static_cast<size_t>(tomb_index)) {
        tombstones_[i]--;
      }
    }

    // 物理删除：移动数组元素
    // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
    int total_size = GetSize();
    for (int i = tomb_index; i < total_size - 1; i++) {
      key_array_[i] = key_array_[i + 1];
      rid_array_[i] = rid_array_[i + 1];
    }

    // 移除最早的墓碑（tombstones_[0]），将后面的墓碑前移
    for (size_t i = 0; i < GetNumTombstones() - 1; i++) {
      tombstones_[i] = tombstones_[i + 1];
    }
    SetNumTombstones(GetNumTombstones() - 1);

    // 减少size_（物理删除了一个键）
    ChangeSizeBy(-1);

    //这里为处理之前墓碑标记的键被挤出去的情况 即用need_deep_update来辅助更新父页 此时之前的墓碑标记的首键还没有被清除 但是上面已经物理清除了
    //is_first_key为是否挤出去的是首键
    if (is_first_key) {
      //因为只有墓碑清除函数才记录这个key
      before_first_key_ = deleted_key;
      need_deep_update_=true;
    }
    processed_tombstone = true;
  }
  
  // 如果处理了墓碑，并且被删除的墓碑索引小于当前要标记的index，需要调整index
  // 因为物理删除会导致数组前移，index需要减1
  if (processed_tombstone && processed_tomb_index >= 0 && processed_tomb_index < index) {
    index--;
  }
  
  // 乐观删除 标记墓碑 不物理删
  MarkTomb(index);
  //处理首次标记首键的情况
  if (!is_update_&&index==0) {
    is_update_=true;
  }
}

//吸收函数 将page里的键值对吸收到末尾 但是不做删除处理 外部负责处理
FULL_INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::Absorb(B_PLUS_TREE_LEAF_PAGE_TYPE *page) {
  // 找到第一个有效键作为begin_key
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
  int total_size = page->GetSize();
  KeyType begin_key{};

  begin_key = page->key_array_[0];

  
  // 获取当前页的总大小（用于计算合并后的索引）
  int current_total_size = GetSize();
  
  // 第一步：将page的所有元素（包括墓碑）移到当前页
  for (int i = 0; i < total_size; i++) {
    int new_index = current_total_size + i;
    key_array_[new_index] = page->key_array_[i];
    rid_array_[new_index] = page->rid_array_[i];
    // 注意：所有元素（包括墓碑）都需要增加size_，因为size_是物理存储的总数
    ChangeSizeBy(1);
  }
  
  // 第二步：单独循环处理墓碑数组，将墓碑索引更新为新页中的位置，并追加到当前页的墓碑数组
  if (LEAF_PAGE_TOMB_CNT > 0) {
    for (size_t i = 0; i < page->GetNumTombstones(); i++) {
      // 获取原页中的墓碑索引
      size_t old_tomb_index = page->tombstones_[i];
      // 计算在新页中的索引位置
      size_t new_tomb_index = static_cast<size_t>(current_total_size) + old_tomb_index;
      // 追加到当前页的墓碑数组
      tombstones_[GetNumTombstones()] = new_tomb_index;
      SetNumTombstones(GetNumTombstones() + 1);
    }
  }
  
  // 更新page的大小（清空page）
  page->ChangeSizeBy(-page->GetSize());
  page->SetNumTombstones(0);
  
  return begin_key;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MarkTomb(int index) {
    // 根据文档：删除操作不减少size_，仅增加num_tombstones_
    int i=static_cast<int>(GetNumTombstones());
    tombstones_[i]=index;
    SetNumTombstones(GetNumTombstones()+1);
    // 注意：不调用ChangeSizeBy(-1)，保持size_不变
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
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveTombstone(int index) {
  // 从tombstones_数组中移除指定索引的墓碑
  for (size_t i = 0; i < GetNumTombstones(); i++) {
    if (tombstones_[i] == static_cast<size_t>(index)) {
      // 找到要移除的墓碑，将其后的元素前移
      for (size_t j = i; j < GetNumTombstones() - 1; j++) {
        tombstones_[j] = tombstones_[j + 1];
      }
      SetNumTombstones(GetNumTombstones() - 1);
      return;
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ProcessOldestTombstone() {
  // 处理最早的墓碑：物理删除对应键值对，并调整剩余墓碑的索引
  if (GetNumTombstones() == 0) {
    return;
  }

  // 最早的墓碑是tombstones_[0]（按删除顺序）
  int tomb_index = static_cast<int>(tombstones_[0]);

  // 保存被删除的键（用于更新父节点）
  KeyType deleted_key = key_array_[tomb_index];
  bool is_first_key = (tomb_index == 0);

  // 先调整所有剩余墓碑的索引：如果墓碑索引 > tomb_index，需要减1
  // 注意：必须在物理删除之前调整，因为物理删除会改变数组
  for (size_t i = 1; i < GetNumTombstones(); i++) {
    if (tombstones_[i] > static_cast<size_t>(tomb_index)) {
      tombstones_[i]--;
    }
  }

  // 物理删除：移动数组元素
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
  int total_size = GetSize();
  for (int i = tomb_index; i < total_size - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }

  // 移除最早的墓碑（tombstones_[0]），将后面的墓碑前移
  for (size_t i = 0; i < GetNumTombstones() - 1; i++) {
    tombstones_[i] = tombstones_[i + 1];
  }
  SetNumTombstones(GetNumTombstones() - 1);

  // 减少size_（物理删除了一个键）
  ChangeSizeBy(-1);

  // 如果删除的是首键，需要标记更新
  if (is_first_key) {
    // 如果is_update_为false，设置before_first_key_为被删除的键
    // 如果is_update_已经为true，说明之前标记过首键删除，现在实际物理删除了，需要更新before_first_key_
    // 因为这是实际被物理删除的首键，用于后续更新父页
    before_first_key_ = deleted_key;

    //如果首键的第二个键已经被标记
    if (IsTombstone(0)){
      is_update_ = true;
    }else {
      is_update_=false;
    }
    need_deep_update_=true;
  }
}


FULL_INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::IsUpdate(){
  return is_update_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::IsEmpty(){
  // 根据文档：应该基于逻辑大小（有效键数量）来判断是否为空
  // 如果逻辑大小为0，说明所有键都是墓碑，应该被认为是空的
  return GetSize() == 0;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CleanupTombs() {
  // 创建临时数组，只存储有效键 跳过墓碑
  KeyType new_key_array[LEAF_PAGE_SLOT_CNT];
  ValueType new_rid_array[LEAF_PAGE_SLOT_CNT];
  int new_size = 0;

    //记录墓碑清除（包含首键的清除）前的首键 方便下溢clean后的父页信息更新
    //父页更新的更新键可以直接去minkey即可 无需记录
    before_first_key_ = key_array_[0];

  // 遍历所有键 只保留非墓碑的键
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
  for (int i = 0; i < GetSize(); i++) {
    if (!IsTombstone(i)) {
      new_key_array[new_size] = key_array_[i];
      new_rid_array[new_size] = rid_array_[i];
      new_size++;
    }
  }

  // 将新数组复制回原数组
  for (int i = 0; i < new_size; i++) {
    key_array_[i] = new_key_array[i];
    rid_array_[i] = new_rid_array[i];
  }
  
  // 更新size_为新的有效键数量（物理删除后，size_应该等于有效键数量）
  SetSize(new_size);
  num_tombstones_ = 0;
}

//当查找不到时 直接返回索引0上的值
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::FindAndPush(const KeyComparator &comparator,
                                      const KeyType &key, std::vector<ValueType> *result) const {
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
  int begin = 0;
  int end = GetSize() - 1;
  while (begin <= end) {
    int mid = (end - begin) / 2 + begin;
    int res=comparator(key_array_[mid], key);
    if (res > 0) {
      end = mid - 1;
    } else if (res < 0){
      begin = mid + 1;
    } else {
      if (LEAF_PAGE_TOMB_CNT > 0 && IsTombstone(mid)) {
        // 当前命中的键已经是墓碑，继续向右查找其它可能的重复键
        begin = mid + 1;
        continue;
      }
      result->push_back(rid_array_[mid]);
      begin = mid + 1;
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertBegin(std::pair<KeyType, ValueType> pair){
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
  int total_size = GetSize();
  
  // 先更新墓碑索引：所有墓碑索引都需要加1（因为要在开头插入新元素）
  if (LEAF_PAGE_TOMB_CNT > 0) {
    for (size_t i = 0; i < GetNumTombstones(); i++) {
      tombstones_[i]++;
    }
  }
  
  // 移动所有元素
  for (int i = total_size - 1; i >= 0; i--) {
    key_array_[i + 1] = key_array_[i];
    rid_array_[i + 1] = rid_array_[i];
  }
  
  key_array_[0] = pair.first;
  rid_array_[0] = pair.second;
  ChangeSizeBy(1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertBack(std::pair<KeyType, ValueType> pair){
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
  int total_size = GetSize();
  key_array_[total_size] = pair.first;
  rid_array_[total_size] = pair.second;
  ChangeSizeBy(1);
}

//键值对删除然后返回出来（跳过墓碑，只返回有效键）
FULL_INDEX_TEMPLATE_ARGUMENTS
std::pair<KeyType, ValueType> B_PLUS_TREE_LEAF_PAGE_TYPE::PopBack() {
  // 找到最后一个有效键（非墓碑）
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
  int total_size = GetSize();
  int last_valid_index = -1;
  for (int i = total_size - 1; i >= 0; i--) {
    if (!IsTombstone(i)) {
      last_valid_index = i;
      break;
    }
  }
  
  if (last_valid_index == -1) {
    // 没有有效键，返回无效值（这种情况不应该发生）
    return std::make_pair(KeyType{}, ValueType{});
  }
  
  auto pair = std::make_pair(key_array_[last_valid_index], rid_array_[last_valid_index]);
  
  // 物理删除：移动数组元素
  for (int i = last_valid_index; i < total_size - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  
  // 调整墓碑索引：如果墓碑索引 > last_valid_index，需要减1
  if (LEAF_PAGE_TOMB_CNT > 0) {
    for (size_t i = 0; i < GetNumTombstones(); i++) {
      if (tombstones_[i] > static_cast<size_t>(last_valid_index)) {
        tombstones_[i]--;
      }
    }
  }
  
  ChangeSizeBy(-1);
  return pair;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
std::pair<KeyType, ValueType> B_PLUS_TREE_LEAF_PAGE_TYPE::PopFront() {
  // 找到第一个有效键（非墓碑）
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
  int total_size = GetSize();
  int first_valid_index = -1;
  for (int i = 0; i < total_size; i++) {
    if (!IsTombstone(i)) {
      first_valid_index = i;
      break;
    }
  }
  
  if (first_valid_index == -1) {
    // 没有有效键，返回无效值（这种情况不应该发生）
    return std::make_pair(KeyType{}, ValueType{});
  }
  
  auto pair = std::make_pair(key_array_[first_valid_index], rid_array_[first_valid_index]);
  
  // 物理删除：移动数组元素
  for (int i = first_valid_index; i < total_size - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  
  // 调整墓碑索引：如果墓碑索引 > first_valid_index，需要减1
  if (LEAF_PAGE_TOMB_CNT > 0) {
    for (size_t i = 0; i < GetNumTombstones(); i++) {
      if (tombstones_[i] > static_cast<size_t>(first_valid_index)) {
        tombstones_[i]--;
      }
    }
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

  // 根据物理大小计算分裂点（包括墓碑在内的所有键值对）
  // size_ 是物理存储的键总数（包括墓碑），所以总大小就是 GetSize()
  int total_size = GetSize();
  int split_point = total_size / 2;  // 分裂点基于物理大小
  int split_index = split_point;  // 分裂点：从split_index开始移到右页
  
  // 第一步：先收集需要移到新页的墓碑信息（保持原页墓碑数组中的顺序）
  // 创建一个映射：原页索引 -> 新页索引
  std::vector<std::pair<size_t, size_t>> tombstone_mapping;  // (原页索引, 新页索引)
  if (LEAF_PAGE_TOMB_CNT > 0) {
    // 先遍历原页的墓碑数组，找出所有 >= split_index 的墓碑
    // 按照它们在原页墓碑数组中的顺序来收集
    for (size_t i = 0; i < GetNumTombstones(); i++) {
      if (tombstones_[i] >= static_cast<size_t>(split_index)) {
        // 计算这个墓碑在新页中的索引位置
        size_t new_index = tombstones_[i] - static_cast<size_t>(split_index);
        tombstone_mapping.push_back(std::make_pair(tombstones_[i], new_index));
      }
    }
  }
  
  // 第二步：将split_index之后的元素移到新页（包括墓碑在内的所有元素）
  int new_page_pos = 0;  // 新页中的位置
  for (int i = split_index; i < total_size; i++) {
    new_leaf_page->key_array_[new_page_pos] = key_array_[i];
    new_leaf_page->rid_array_[new_page_pos] = rid_array_[i];
    // 所有元素（包括墓碑）都增加新页的物理size
    new_leaf_page->ChangeSizeBy(1);
    new_page_pos++;
  }
  
  // 第三步：按照原页墓碑数组中的顺序，将墓碑添加到新页的墓碑数组
  if (LEAF_PAGE_TOMB_CNT > 0) {
    for (const auto& pair : tombstone_mapping) {
      new_leaf_page->tombstones_[new_leaf_page->GetNumTombstones()] = pair.second;
      new_leaf_page->SetNumTombstones(new_leaf_page->GetNumTombstones() + 1);
    }
  }
  
  // 第四步：从原页移除已移到新页的元素和墓碑
  // 处理墓碑：移除属于右页的墓碑，保留左页的墓碑
  if (LEAF_PAGE_TOMB_CNT > 0) {
    size_t new_num_tombs = 0;
    for (size_t i = 0; i < GetNumTombstones(); i++) {
      if (tombstones_[i] < static_cast<size_t>(split_index)) {
        // 墓碑在左页，保留但索引不变
        tombstones_[new_num_tombs] = tombstones_[i];
        new_num_tombs++;
      }
      // 墓碑在右页的已经在上面移到新页了，这里不需要处理
    }
    SetNumTombstones(new_num_tombs);
  }
  
  // 更新原页的物理size：移除已移到新页的所有元素（包括墓碑）
  int moved_count = total_size - split_index;  // 移动的物理元素数量
  ChangeSizeBy(-moved_count);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetIsUpdate(bool set) {
  is_update_=set;
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