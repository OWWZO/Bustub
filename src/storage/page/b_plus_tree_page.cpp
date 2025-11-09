//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_page.cpp
//
// Identification: src/storage/page/b_plus_tree_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*
获取 / 设置页面类型的辅助方法
页面类型枚举类定义在 b_plus_tree_page.h 中
*/
auto BPlusTreePage::IsLeafPage() const -> bool {
  if (page_type_ == IndexPageType::LEAF_PAGE) {
    return true;
  } else {
    return false;
  }
}

void BPlusTreePage::SetPageType(IndexPageType page_type) {
  page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int {
  return size_;
}

void BPlusTreePage::SetSize(int size) {
  size_ = size;
}

void BPlusTreePage::ChangeSizeBy(int amount) {
  size_ += amount;
}


auto BPlusTreePage::GetMaxSize() const -> int {
  return max_size_;
}

void BPlusTreePage::SetMaxSize(int size) {
  max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * But whether you will take ceil() or floor() depends on your implementation
 */
auto BPlusTreePage::GetMinSize() const -> int { return ceil(static_cast<float>(max_size_) / 2); }

void BPlusTreePage::SetPageId(page_id_t id) {
  page_id_=id;
}

auto BPlusTreePage::GetPageId()const -> page_id_t {
  return page_id_;
}

void BPlusTreePage::SetFatherPageId(page_id_t id) {
  father_page_id=id;
}

auto BPlusTreePage::GetFatherPageId() -> page_id_t {
  return father_page_id;
}
} // namespace bustub