//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_header_page.h
//
// Identification: src/include/storage/page/b_plus_tree_header_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/config.h"

namespace bustub {

/*
头页面仅用于获取根页面，
以防止并发环境下可能出现的竞态条件。
*/
class BPlusTreeHeaderPage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeHeaderPage() = delete;
  BPlusTreeHeaderPage(const BPlusTreeHeaderPage &other) = delete;

  page_id_t root_page_id_;
};

}  // namespace bustub
