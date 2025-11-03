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
  return true;
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
  auto header_guard = bpm_->WritePage(header_page_id_);
  auto header_page_read = header_guard.As<BPlusTreeHeaderPage>();
  auto header_page_write = header_guard.AsMut<BPlusTreeHeaderPage>();
  //处理树为空的逻辑
  if (header_page_read->root_page_id_ == INVALID_PAGE_ID) {
    page_id_t page_id = bpm_->NewPage();
    //给全局配置里的根页设置id
    header_page_write->root_page_id_ = page_id;
    //获取根页的write_guard
    auto root_page_guard = bpm_->WritePage(page_id);
    //用guard 来获取到页面的数据数组 将内存变为leaf页 进行操作
    auto root_page_write = root_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //初始化叶子页
    root_page_write->Init();
    root_page_write->SetPageId(page_id);
  }
  auto root_page_guard = bpm_->WritePage(header_page_read->root_page_id_);
  //变成基类查页类型
  auto temp_root_page_write = root_page_guard.As<BPlusTreePage>();
  if (temp_root_page_write->IsLeafPage()) {
    //重新变成叶子页
    auto root_page_write = root_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //插入键值对
    bool is_repeat = root_page_write->InsertKeyValue(comparator_, key, value);
    if (is_repeat) {
      return false;
    }
    //处理叶子页满了的逻辑
    if (root_page_write->GetSize() == root_page_write->GetMaxSize()) {
      //创建新叶子页
      auto new_page_id = bpm_->NewPage();
      auto new_page_guard = bpm_->WritePage(new_page_id);
      auto new_page_write = new_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      new_page_write->Init();
      new_page_write->SetPageId(new_page_id);
      //进行分裂
      root_page_write->Split(new_page_write);
      //如果是根节点 要进行建立internal页
      if (header_page_read->root_page_id_ == root_page_write->GetPageId()) {
        //建立的一些基础操作
        auto internal_page_id = bpm_->NewPage();
        auto internal_page_guard = bpm_->WritePage(internal_page_id);
        auto internal_page_write = internal_page_guard.AsMut<BPlusTreeInternalPage<
          KeyType, page_id_t, KeyComparator>>();
        internal_page_write->Init();
        internal_page_write->SetPageId(internal_page_id);
        //将分裂的两个叶子页的父节点设置成当前internal页
        new_page_write->SetFatherPageId(internal_page_id);
        root_page_write->SetFatherPageId(internal_page_id);
        //然后更新root_page_id
        header_page_write->root_page_id_ = internal_page_id;
        //设置internal页的键值对
        internal_page_write->FirstInsert(new_page_write->GetMinKey(), root_page_write->GetPageId(),
                                         new_page_write->GetPageId());
      }
    }
  } else {
    //注意 此时至少为2层
    //查找当前要插入的键值对 将要被插入到哪里 已实现循环查找 知道查到的页是叶子id
    auto page_id = INVALID_PAGE_ID;
    {
      auto root_page_write = root_page_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      //先查一层 防止用root_page_guard来进行循环 减少bug出现概率
      auto temp_id = root_page_write->Find(comparator_, key);
      //读取这个id的信息
      auto temp_guard = bpm_->WritePage(temp_id);
      auto temp_write_b_page = temp_guard.template AsMut<BPlusTreePage>();
      //发现不是叶子页 就循环find
      while (!temp_write_b_page->IsLeafPage()) {
        //先将第一次查找的那一页 变成internal 页
        auto guard_while = bpm_->WritePage(temp_id);
        auto temp_write_internal_page = guard_while.template AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
        //继续在此内页上查找
        temp_id = temp_write_internal_page->Find(comparator_, key);
        //复用循环外的变量 实现while循环查找
        temp_guard = bpm_->WritePage(temp_id);
        temp_write_b_page = temp_guard.template AsMut<BPlusTreePage>();
      }
      page_id = temp_id;
    }
    //根据位置 插入到指定叶子页
    auto leaf_page_guard = bpm_->WritePage(page_id);
    auto leaf_page_write = leaf_page_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    leaf_page_write->InsertKeyValue(comparator_, key, value);
    //处理叶子页已经满了的情况 触发分裂
    if (leaf_page_write->GetMaxSize() == leaf_page_write->GetSize()) {
      //创建新叶子页
      //PushUp逻辑 向右
      auto new_page_id = bpm_->NewPage();
      auto new_page_guard = bpm_->WritePage(new_page_id);
      auto new_page_write = new_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      new_page_write->Init();
      new_page_write->SetPageId(new_page_id);
      new_page_write->SetFatherPageId(leaf_page_write->GetFatherPageId());
      //进行分裂
      leaf_page_write->Split(new_page_write);
      //分裂后 将新的页的数据 加入到内页
      auto father_page_guard = bpm_->WritePage(leaf_page_write->GetFatherPageId());
      auto father_page_write = father_page_guard.template AsMut<BPlusTreeInternalPage<
        KeyType, page_id_t, KeyComparator>>();
      father_page_write->InsertKeyValue(comparator_, new_page_write->GetMinKey(), new_page_write->GetPageId());
      //处理第一次内页满了的逻辑
      //TODO(wwz): 当然还有第二次内页满了的逻辑
      if (father_page_write->GetMaxSize() == father_page_write->GetSize()) {
        //创建新内页
        auto new_internal_page_id = bpm_->NewPage();
        auto new_internal_guard = bpm_->WritePage(new_internal_page_id);
        auto new_internal_write = new_internal_guard.AsMut<BPlusTreeInternalPage<
          KeyType, page_id_t, KeyComparator>>();
        new_internal_write->Init();
        new_internal_write->SetPageId(new_internal_page_id);
        //内页满了之后的分裂要特化处理 因为要处理地下叶子页的父节点关系
        auto split_key = SplitForInternal(father_page_write, new_internal_write); //实现内页分裂逻辑
        //建立父内页 提高树高度 将其设置成根页 //这是当高度第一次到3的时候 才要这样操作
        //parent>father
        page_id_t parent_internal_page_id = father_page_write->GetFatherPageId();
        //此为2层 但是2层已经满了 要升级为3层逻辑
        if (parent_internal_page_id == INVALID_PAGE_ID) {
          parent_internal_page_id = bpm_->NewPage();
          auto parent_internal_guard = bpm_->WritePage(parent_internal_page_id);
          auto parent_internal_write = parent_internal_guard.AsMut<BPlusTreeInternalPage<
            KeyType, page_id_t, KeyComparator>>();
          parent_internal_write->Init();
          parent_internal_write->SetPageId(parent_internal_page_id);
          //由于父节点的id已经出现 更新两个内页的parent_id
          father_page_write->SetFatherPageId(parent_internal_page_id);
          new_internal_write->SetFatherPageId(parent_internal_page_id);
          //插入刚刚获得的分裂键
          parent_internal_write->FirstInsert(split_key, father_page_write->GetPageId(),
                                             new_internal_write->GetPageId());
          //更新 根页id;
          header_page_write->root_page_id_ = parent_internal_write->GetPageId();
        } else {
          //此为 已经是3层了 然后继续插入的逻辑 也就是
          auto parent_internal_guard = bpm_->WritePage(parent_internal_page_id);
          auto parent_internal_write = parent_internal_guard.AsMut<BPlusTreeInternalPage<
            KeyType, page_id_t, KeyComparator>>();
          //插入刚刚获得的分裂键
          parent_internal_write->InsertKeyValue(comparator_, split_key, new_internal_write->GetPageId());
          //更新新内页的父亲页id //差点忘记 但是看着注释分析出来忘记设置了
          new_internal_write->SetFatherPageId(parent_internal_write->GetPageId());
          //3层模型 当第3层页也插满了
          if (parent_internal_write->GetMaxSize() == parent_internal_write->GetSize()) {
            auto new_est_page_id = bpm_->NewPage();
            auto new_est_guard = bpm_->WritePage(new_est_page_id);
            auto new_est_write = new_est_guard.AsMut<BPlusTreeInternalPage<
              KeyType, page_id_t, KeyComparator>>();
            new_est_write->Init();
            new_est_write->SetPageId(new_est_page_id);
            auto split_key_other = SplitForInternal(parent_internal_write, new_est_write);
            //建立顶层页  //TODO(wwz): 处理已经有父页的逻辑
            auto new_top_page_id = bpm_->NewPage();
            auto new_top_guard = bpm_->WritePage(new_top_page_id);
            auto new_top_write = new_top_guard.AsMut<BPlusTreeInternalPage<
              KeyType, page_id_t, KeyComparator>>();
            new_top_write->Init();
            new_top_write->SetPageId(new_top_page_id);
            //更新父节点id
            parent_internal_write->SetFatherPageId(new_top_write->GetPageId());
            new_est_write->SetFatherPageId(new_top_write->GetPageId());
            //插入分裂键
            new_est_write->FirstInsert(split_key_other,parent_internal_write->GetPageId(),new_est_write->GetPageId());
          }
        }
      }
    }
  }
  return true;
}
//外部执行 基本查找 并且插入逻辑
//每次执行插入操作 就将被插入的叶子id传入
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PushUp(page_id_t id) {
  //伪代码
  //如果当前页满了就 创建新叶子页 然后分裂
  //然后插入父节点（无父节点就建立 属于else分支）
  //然后pushup父节点

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
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto header_guard = bpm_->WritePage(header_page_id_);
  auto header_page_read = header_guard.As<BPlusTreeHeaderPage>();
  return header_page_read->root_page_id_;
}

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