//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {
FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
  : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
    index_name_(std::move(name)),
    comparator_(std::move(comparator)),
    leaf_max_size_(leaf_max_size),
    internal_max_size_(internal_max_size),
    header_page_id_(header_page_id) {
  //分配一个header_page_id 来存根页id 存树的全局配置
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
@brief 用于判断当前 B + 树是否为空的辅助函数
@return 若此 B + 树没有键和值，则返回 true。
*/
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  return false;
}


/**
@brief 返回与输入键相关联的唯一值
此方法用于点查询
@param key 输入键
@param [out] result 存储与输入键相关联的唯一值的向量（如果该值存在）
@return ：true 表示键存在
*/
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
@brief 向 B + 树中插入常量键值对
如果当前树为空，则启动新树，更新根页 ID 并插入条目；否则，插入到叶页中。
@param key 要插入的键
@param value 与键相关联的值
@return: 由于我们仅支持唯一键，如果用户尝试插入重复键，则返回 false；否则，返回 true。
*/
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;
  //获取 头页的guard 然后根据头页的guard来获取根页的数据数组
  WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
  auto header_page_read = header_guard.As<BPlusTreeHeaderPage>();
  auto header_page_write = header_guard.AsMut<BPlusTreeHeaderPage>();
  //处理树为空的逻辑
  if (header_page_read->root_page_id_ == INVALID_PAGE_ID) {
    page_id_t page_id = bpm_->NewPage();
    //给全局配置里的根页设置id
    header_page_write->root_page_id_ = page_id;
    //获取根页的write_guard
    WritePageGuard root_page_guard = bpm_->WritePage(page_id);
    //用guard 来获取到页面的数据数组 将内存变为leaf页 进行操作
    auto root_page_write = root_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //初始化叶子页
    root_page_write->Init();
    //插入键值对
    root_page_write->InsertKeyValue(0,key,value);
    return true;
  }

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { UNIMPLEMENTED("TODO(P2): Add implementation."); }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
} // namespace bustub