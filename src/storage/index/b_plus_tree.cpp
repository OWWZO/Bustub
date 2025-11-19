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
#include <algorithm>
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

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateFather(KeyType first_key, KeyType second_key, WritePageGuard& write_guard) {
  auto write=write_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  //获取父页write
  auto father_id=write->GetFatherPageId();
  if (father_id==INVALID_PAGE_ID) {
    return ;
  }
  write_guard.Drop();
  auto father_guard=bpm_->WritePage(father_id);
  auto father_write=father_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

  int index=father_write->MatchKey(first_key,comparator_);
  if (index == -1) {
    // 回退：通过子页ID定位对应条目
    index = father_write->ValueIndexForPage_id_t(write->GetPageId());
  }
  if (index == -1) {
    return;
  }
  father_write->UpdateKey(index,second_key);
  if (index==0) {
    UpdateFather(first_key,second_key,father_guard);
  }
}

/**
 * @brief 递归向上更新所有父节点的键信息（用于重分配操作）
 * @param old_key 旧的键值，用于在父页中查找要更新的条目
 * @param new_pair 新的键值对（键和页面ID）
 * @param father_write 当前父页的指针
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RecursiveUpdateKeyForRedistribute(
    KeyType old_key, 
    std::pair<KeyType, page_id_t> new_pair,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* father_write) {
  // 如果父页为空，直接返回
  if (father_write == nullptr) {
    return;
  }
  
  // 在父页中查找要更新的键的索引
  auto index = father_write->MatchKey(old_key, comparator_);
  if (index == -1) {
    return;  // 找不到对应的键，直接返回
  }
  
  // 用于递归时记录父页自身在祖父中的旧首键
  auto father_old_min_key = father_write->KeyAt(0);

  // 更新父页中的键值对
  father_write->UpdateKey(old_key, new_pair, comparator_);
  
  // 如果更新的是父页的第一个键（index==0），需要继续向上递归更新
  // 否则不需要递归（因为只有首位键的改变会影响上层）
  if (index == 0) {
    auto grandfather_id = father_write->GetFatherPageId();
    if (grandfather_id != INVALID_PAGE_ID) {
      auto grandfather_guard = bpm_->WritePage(grandfather_id);
      auto grandfather_write = grandfather_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      // 递归向上更新：使用新的键值继续向上查找和更新
      auto father_new_min_key = father_write->KeyAt(0);
      RecursiveUpdateKeyForRedistribute(father_old_min_key,
                                        std::make_pair(father_new_min_key, father_write->GetPageId()),
                                        grandfather_write);
    }
  }
}

//调用之前 需要确保根页存在
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LocateKey(const KeyType &key, const BPlusTreeHeaderPage* header_page) -> page_id_t {
  // 获取根页
  auto root_guard = bpm_->ReadPage(header_page->root_page_id_);
  auto root_page = root_guard.As<BPlusTreePage>();
  // 如果根页是叶子页，直接查找
  if (!root_page->IsLeafPage()) {
    {
      auto root_page_write = root_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      //先查一层 防止用root_page_guard来进行循环 减少bug出现概率
      auto temp_id = root_page_write->Find(comparator_, key);
      if (temp_id==INVALID_PAGE_ID) {
        return -1;
      }
      //读取这个id的信息
      auto temp_guard = bpm_->ReadPage(temp_id);
      auto temp_write_b_page = temp_guard.template As<BPlusTreePage>();
      //发现不是叶子页 就循环find
      // 注意：不要在Drop guard后继续使用指针，会导致悬空指针问题
      bool is_leaf = temp_write_b_page->IsLeafPage();
      temp_guard.Drop();
      //TODO 这里以前产生了未定义行为
      while (!is_leaf) {
        //先将第一次查找的那一页 变成internal 页
        auto guard_while = bpm_->ReadPage(temp_id);
        auto temp_check_page = guard_while.template As<BPlusTreePage>();
        // 安全检查：确保页面确实是内部页，避免类型转换错误
        if (temp_check_page->IsLeafPage()) {
          // 如果实际上是叶子页，说明数据不一致或逻辑错误
          return -1;
        }
        // 额外检查：验证 page_id 是否有效
        page_id_t check_page_id = temp_check_page->GetPageId();
        if (check_page_id != temp_id) {
          // 页面数据不一致：页面中存储的 page_id 与实际 page_id 不匹配
          // 这可能是页面数据损坏或类型转换错误的标志
          return -1;
        }
        auto temp_write_internal_page = guard_while.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        //继续在此内页上查找
        temp_id = temp_write_internal_page->Find(comparator_, key);
        if (temp_id == INVALID_PAGE_ID) {
          return -1;
        }
        //复用循环外的变量 实现while循环查找
        guard_while.Drop();
        temp_guard = bpm_->ReadPage(temp_id);
        temp_write_b_page = temp_guard.template As<BPlusTreePage>();
        // 在Drop之前先检查是否是叶子页
        is_leaf = temp_write_b_page->IsLeafPage();
        temp_guard.Drop();
      }
      //获取到内页的id
      return temp_id;
    }
  }else {
    return header_page->root_page_id_;
  }
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
  Context ctx;
  // 获取头页，读取根页ID
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  // 如果树为空，返回false
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  auto page_id=LocateKey(key,header_page);
  if (page_id==INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();
  auto leaf_guard=bpm_->ReadPage(page_id);
  auto leaf_read=leaf_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  leaf_read->FindAndPush(comparator_, key, result);
  if (result->empty()) {
    return false;
  }
  return true;
}


//适用于当上层是内页 下面是叶子页的情况
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitForInternal(
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *first_internal_write,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *second_internal_write,
    std::vector<page_id_t> &moved_children) -> KeyType {
  moved_children.clear();
  auto key = first_internal_write->Split(second_internal_write, moved_children);
  return key;
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
FULL_INDEX_TEMPLATE_ARGUMENTS//TODO 读取下/上 一页之前 先释放当前页的guard 因为可能上/下 一页阻塞在访问当前页的过程里
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;
  //获取 头页的guard 然后根据头页的guard来获取根页的数据数组
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page_read = header_guard.As<BPlusTreeHeaderPage>();
  //处理树为空的逻辑 建立新树
  if (header_page_read->root_page_id_ == INVALID_PAGE_ID) {
    header_guard.Drop();
    auto header_guard_w=bpm_->WritePage(header_page_id_);
    auto header_page_write = header_guard_w.AsMut<BPlusTreeHeaderPage>();
    page_id_t page_id = bpm_->NewPage();
    if (page_id == INVALID_PAGE_ID) {
      // 缓冲池满或无可淘汰帧，无法创建新根
      return false;
    }
    //给全局配置里的根页设置id
    header_page_write->root_page_id_ = page_id;
    //获取根页的write_guard
    auto root_page_guard = bpm_->WritePage(page_id);
    //用guard 来获取到页面的数据数组 将内存变为leaf页 进行操作
    auto root_page_write = root_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //初始化叶子页
    root_page_write->Init(GetLeafMaxSize());
    root_page_write->SetPageId(page_id);
  }
  auto  find_guard = bpm_->ReadPage(header_page_read->root_page_id_);
  //变成基类查页类型
  auto temp_root_page_write =  find_guard.As<BPlusTreePage>();
  if (temp_root_page_write->IsLeafPage()) {
    find_guard.Drop();
    auto root_guard= bpm_->WritePage(header_page_read->root_page_id_);
    //重新变成叶子页
    auto root_page_write =  root_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //插入键值对
    bool is_repeat = root_page_write->InsertKeyValue(comparator_, key, value);
    if (!is_repeat) {
      header_guard.Drop();
      root_guard.Drop();
      return false;
    }
    header_guard.Drop();
    PushUp(root_guard);
  } else{
    //注意 此时至少为2层
    //查找当前要插入的键值对 将要被插入到哪里 已实现循环查找 知道查到的页是叶子id
    auto page_id = INVALID_PAGE_ID;
    {
      auto root_page_write =  find_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      //先查一层 防止用root_page_guard来进行循环 减少bug出现概率
      auto temp_id = root_page_write->AccurateFind(comparator_, key);
      //读取这个id的信息
      auto temp_guard = bpm_->ReadPage(temp_id);
      auto temp_write_b_page = temp_guard.template As<BPlusTreePage>();
      //发现不是叶子页 就循环find
      // 注意：不要在Drop guard后继续使用指针，会导致悬空指针问题
      bool is_leaf = temp_write_b_page->IsLeafPage();
      temp_guard.Drop();
      
      while (!is_leaf) {
        //先将第一次查找的那一页 变成internal 页
        auto guard_while = bpm_->ReadPage(temp_id);
        auto temp_write_internal_page = guard_while.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        //继续在此内页上查找
        temp_id = temp_write_internal_page->AccurateFind(comparator_, key);
        if (temp_id == INVALID_PAGE_ID) {
          header_guard.Drop();
          find_guard.Drop();
          guard_while.Drop();
          return false;
        }
        //复用循环外的变量 实现while循环查找
        guard_while.Drop();
        temp_guard = bpm_->ReadPage(temp_id);
        temp_write_b_page = temp_guard.template As<BPlusTreePage>();
        // 在Drop之前先检查是否是叶子页
        is_leaf = temp_write_b_page->IsLeafPage();
        temp_guard.Drop();
      }
      page_id = temp_id;
    }
    //根据位置 插入到指定叶子页
    auto leaf_page_guard = bpm_->WritePage(page_id);
    auto leaf_page_write = leaf_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    int pre_size = leaf_page_write->GetSize();
    KeyType begin_key{};
    if (pre_size > 0) {
      begin_key = leaf_page_write->GetMinKey();
    } else {
      begin_key = leaf_page_write->GetBeforeFirstKey();
    }
    bool inserted = leaf_page_write->InsertKeyValue(comparator_, key, value);
    if (!inserted) {
      header_guard.Drop();
      find_guard.Drop();
      leaf_page_guard.Drop();
      return false;
    }
    header_guard.Drop();
    find_guard.Drop();
    if (leaf_page_write->IsBegin()) {
      UpdateFather(begin_key, leaf_page_write->GetMinKey(), leaf_page_guard);
      auto leaf_page_guard1 = bpm_->WritePage(page_id);
      auto leaf_page_write1 = leaf_page_guard1.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      leaf_page_write1->SetBegin(false);
      PushUp(leaf_page_guard1);
    } else {
      PushUp(leaf_page_guard);
    }
  }
  return true;
}

//传入被插入页的guard 1.如果是叶子页 满了就建立新页然后进行split （1）此时如果无父页就进行建立内页 然后注册信息  （2）如果有父页就进行pushup递归向上检测
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PushUp(WritePageGuard& write_guard) {
  //如果当前页满了就 创建新叶子页 然后分裂
  auto temp_write = write_guard.As<BPlusTreePage>();
  if (temp_write->IsLeafPage()) {
    auto page_write=write_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //一层模型 如果第一个叶子页满了 就先分裂 再判断有无父页
    // 分裂判断依据为物理大小（GetSize()），包括墓碑在内的所有键值对
    if (page_write->GetSize() >= page_write->GetMaxSize()) {
      auto new_page_id = bpm_->NewPage();
      if (new_page_id == INVALID_PAGE_ID) {
        return;
      }
      auto new_page_guard = bpm_->WritePage(new_page_id);
      auto new_page_write = new_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      new_page_write->Init(GetLeafMaxSize());
      new_page_write->SetPageId(new_page_id);
      //进行分裂
      page_write->Split(new_page_write);
      if (new_page_write->GetNextPageId()!=INVALID_PAGE_ID) {
        auto next_guard=bpm_->WritePage(new_page_write->GetNextPageId());
        auto next_write=next_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
        next_write->SetPrePageId(new_page_write->GetPageId());//3
      }
      //进行父页判断 无就创立一个内页当父页
      if (page_write->GetFatherPageId()==INVALID_PAGE_ID) {
        //建立的一些基础操作
        auto internal_page_id = bpm_->NewPage();
        if (internal_page_id == INVALID_PAGE_ID) {
          return;
        }
        auto internal_page_guard = bpm_->WritePage(internal_page_id);
        auto internal_page_write = internal_page_guard.AsMut<BPlusTreeInternalPage<
          KeyType, page_id_t, KeyComparator>>();
        internal_page_write->Init(GetInternalMaxSize());
        internal_page_write->SetPageId(internal_page_id);
        //将分裂的两个叶子页的父节点设置成当前internal页
        new_page_write->SetFatherPageId(internal_page_id);
        page_write->SetFatherPageId(internal_page_id);

        auto page_write_min_key=page_write->GetMinKey();
        auto page_write_page_id=page_write->GetPageId();
        write_guard.Drop();

        //然后更新root_page_id
        auto head_guard=bpm_->WritePage(header_page_id_);
        auto head_write=head_guard.AsMut<BPlusTreeHeaderPage>();
        head_write->root_page_id_ = internal_page_id;
        //设置internal页的键值对
        internal_page_write->FirstInsert(page_write_min_key,new_page_write->GetMinKey(), page_write_page_id,new_page_write->GetPageId());
      }else{
        // 在释放 guard 之前先缓存新页的必要信息，避免使用悬空指针
        KeyType right_min_key = new_page_write->GetMinKey();
        page_id_t right_page_id = new_page_write->GetPageId();

        write_guard.Drop();
        new_page_guard.Drop();

        // 分裂后将新页信息插入父页
        auto father_page_guard = bpm_->WritePage(page_write->GetFatherPageId());
        auto father_page_write = father_page_guard.template AsMut<BPlusTreeInternalPage<
          KeyType, page_id_t, KeyComparator>>();
        father_page_write->InsertKeyValue(comparator_, right_min_key, right_page_id);

        // 设置新页的父亲id
        auto right_guard = bpm_->WritePage(right_page_id);
        auto right_leaf = right_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
        right_leaf->SetFatherPageId(father_page_write->GetPageId());
        right_guard.Drop();

        // pushup 父页
        PushUp(father_page_guard);
      }
    }
  }else {
    auto page_write=write_guard.AsMut<BPlusTreeInternalPage<
            KeyType, page_id_t, KeyComparator>>();
    if (GetInternalMaxSize() == page_write->GetSize()) {
      auto new_internal_page_id = bpm_->NewPage();
      if (new_internal_page_id == INVALID_PAGE_ID) {
        return;
      }
      auto new_internal_guard = bpm_->WritePage(new_internal_page_id);
      auto new_internal_write = new_internal_guard.AsMut<BPlusTreeInternalPage<
        KeyType, page_id_t, KeyComparator>>();
      new_internal_write->Init(GetInternalMaxSize());
      new_internal_write->SetPageId(new_internal_page_id);
      std::vector<page_id_t> moved_children;
      SplitForInternal(page_write, new_internal_write, moved_children);
      // 记录必要信息以便在释放锁后安全更新
      auto first_internal_id = page_write->GetPageId();
      auto second_internal_id = new_internal_page_id;

      page_id_t parent_internal_page_id = page_write->GetFatherPageId();
      // 先释放当前两个内页锁，避免父→子锁嵌套
      write_guard.Drop();
      new_internal_guard.Drop();

      // 统一按升序更新迁移子页的父指针
      if (!moved_children.empty()) {
        std::sort(moved_children.begin(), moved_children.end());
        for (auto child_id : moved_children) {
          auto child_guard = bpm_->WritePage(child_id);
          auto child_page = child_guard.AsMut<BPlusTreePage>();
          child_page->SetFatherPageId(second_internal_id);
        }
      }

      // 建立/更新父内页结构
      if (parent_internal_page_id == INVALID_PAGE_ID) {
        parent_internal_page_id = bpm_->NewPage();
        if (parent_internal_page_id == INVALID_PAGE_ID) {
          return;
        }
        auto parent_internal_guard = bpm_->WritePage(parent_internal_page_id);
        auto parent_internal_write = parent_internal_guard.AsMut<BPlusTreeInternalPage<
          KeyType, page_id_t, KeyComparator>>();
        parent_internal_write->Init(GetInternalMaxSize());
        parent_internal_write->SetPageId(parent_internal_page_id);
        KeyType left_min_key{};
        KeyType right_min_key{};
        {
          auto first_internal_guard = bpm_->WritePage(first_internal_id);
          auto first_internal_write = first_internal_guard.template AsMut<BPlusTreeInternalPage<
            KeyType, page_id_t, KeyComparator>>();
          first_internal_write->SetFatherPageId(parent_internal_page_id);
          left_min_key = first_internal_write->GetMinKey();
        }
        {
          auto second_internal_guard = bpm_->WritePage(second_internal_id);
          auto second_internal_write2 = second_internal_guard.AsMut<BPlusTreeInternalPage<
            KeyType, page_id_t, KeyComparator>>();
          second_internal_write2->SetFatherPageId(parent_internal_page_id);
          right_min_key = second_internal_write2->GetMinKey();
        }
        parent_internal_write->FirstInsert(left_min_key, right_min_key,
                                           first_internal_id, second_internal_id);
        auto head_guard=bpm_->WritePage(header_page_id_);
        auto head_write=head_guard.AsMut<BPlusTreeHeaderPage>();
        head_write->root_page_id_ = parent_internal_write->GetPageId();
      } else {
        auto parent_internal_guard = bpm_->WritePage(parent_internal_page_id);
        auto parent_internal_write = parent_internal_guard.AsMut<BPlusTreeInternalPage<
          KeyType, page_id_t, KeyComparator>>();
        KeyType right_min_key{};
        {
          auto second_internal_guard = bpm_->WritePage(second_internal_id);
          auto second_internal_write2 = second_internal_guard.AsMut<BPlusTreeInternalPage<
            KeyType, page_id_t, KeyComparator>>();
          second_internal_write2->SetFatherPageId(parent_internal_write->GetPageId());
          right_min_key = second_internal_write2->GetMinKey();
        }
        parent_internal_write->InsertKeyValue(comparator_, right_min_key, second_internal_id);
        PushUp(parent_internal_guard);
      }
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafMaxSize() -> int {
  return leaf_max_size_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetInternalMaxSize() -> int {
  return internal_max_size_;
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/


FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsDistributeForLeaf(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_write) -> page_id_t {
  //获取左右叶子页
  // 根据文档：借调判断应该基于物理大小（GetSize()），因为需要判断是否有足够的物理空间可以借调
  auto left_id =leaf_write->GetPrePageId();
  auto right_id=leaf_write->GetNextPageId();
  auto need_num=leaf_write->GetMinSize()-leaf_write->GetSize();
 if (left_id!=INVALID_PAGE_ID){
    auto left_guard=bpm_->ReadPage(left_id);
    auto left_write=left_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    if (left_write->GetSize() > leaf_write->GetMinSize()+need_num-1) {
        return left_id;
    }
  }
  if (right_id!=INVALID_PAGE_ID) {
    auto right_guard=bpm_->ReadPage(right_id);
    auto right_write=right_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    if (right_write->GetSize() > leaf_write->GetMinSize()+need_num-1) {
      return right_id;
    }
  }
  return INVALID_PAGE_ID;
}

FULL_INDEX_TEMPLATE_ARGUMENTS//可以加个需要分配多少的参数 然后加在判断条件上
auto BPLUSTREE_TYPE::IsDistributeForInternal(BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>* internal_write, int to_size) -> page_id_t {
  if (internal_write->GetFatherPageId()==INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  auto father_guard=bpm_->ReadPage(internal_write->GetFatherPageId());
  auto father_write = father_guard.template As<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
  //获取左右叶子页id
  auto left_id = internal_write->GetPrePageId(father_write);
  auto right_id=internal_write->GetNextPageId(father_write);
  if (left_id!=INVALID_PAGE_ID){
    auto left_guard=bpm_->ReadPage(left_id);
    auto left_write=left_guard.template As<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
    if (left_write->GetSize()-left_write->GetMinSize()>=to_size) {
      return left_id;
    }
  }
  if (right_id!=INVALID_PAGE_ID) {
    auto right_guard=bpm_->ReadPage(right_id);
    auto right_write=right_guard.template As<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
    if (right_write->GetSize()-right_write->GetMinSize()>to_size) {
      return right_id;
    }
  }
  return INVALID_PAGE_ID;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeForLeaf(page_id_t page_id, B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_write) {
  //如果是左边的 leaf和left要分清楚
    if (page_id==leaf_write->GetPrePageId()) {
      auto left_guard=bpm_->WritePage(page_id);
      auto left_write=left_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      //保存当前页的旧第一个键，用于在父页中查找
      auto old_first_key = leaf_write->KeyAt(0);
      //将左页最后一个元素抛出 放到当前页
      auto back= left_write->PopBack();
      leaf_write->InsertBegin(back);
      //然后更新分裂键，处理无父页的情况
      auto father_id = leaf_write->GetFatherPageId();
      if (father_id != INVALID_PAGE_ID) {
        auto new_first_key = leaf_write->KeyAt(0);
        if (comparator_(new_first_key, old_first_key) != 0) {
          auto father_guard=bpm_->WritePage(father_id);
          auto father_write=father_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          //将当前叶子页的第一个键传入 然后更新分裂键，并递归向上更新
          RecursiveUpdateKeyForRedistribute(old_first_key, std::make_pair(new_first_key, leaf_write->GetPageId()), father_write);
        }
      }
    }else {
      //右边处理逻辑
      auto right_guard=bpm_->WritePage(page_id);
      auto right_write=right_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      //保存右页的旧第一个键，用于在父页中查找
      auto old_right_first_key = right_write->KeyAt(0);
      //保存当前页在重分配前的首键（若为空页，则使用before_first_key_）
      bool has_first_key = leaf_write->GetSize() > 0;
      KeyType receiver_old_first_key{};
      if (has_first_key) {
        receiver_old_first_key = leaf_write->KeyAt(0);
      }
      //将右边页的第一个元素抛出 放到当前页末尾
      auto front=right_write->PopFront();
      leaf_write->InsertBack(front);
      //更新分裂键，处理无父页的情况
      auto father_id = right_write->GetFatherPageId();
      if (father_id != INVALID_PAGE_ID) {
        auto father_guard=bpm_->WritePage(father_id);
        auto father_write=father_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        auto right_new_first_key = right_write->KeyAt(0);
        //更新父页中指向右页的键，并递归向上更新
        RecursiveUpdateKeyForRedistribute(old_right_first_key,
                                          std::make_pair(right_new_first_key, right_write->GetPageId()),
                                          father_write);
      }
      //若当前页的首键发生变化（或者原本为空，现在有了首键），也需要更新父页指向当前页的键
      auto receiver_father_id = leaf_write->GetFatherPageId();
      if (receiver_father_id != INVALID_PAGE_ID) {
        auto new_first_key = leaf_write->KeyAt(0);
        bool need_update = false;
        KeyType receiver_old_key{};
        if (has_first_key) {
          if (comparator_(new_first_key, receiver_old_first_key) != 0) {
            receiver_old_key = receiver_old_first_key;
            need_update = true;
          }
        } else {
          receiver_old_key = leaf_write->GetBeforeFirstKey();
          need_update = true;
        }
        if (need_update) {
          auto receiver_father_guard = bpm_->WritePage(receiver_father_id);
          auto receiver_father_write =
              receiver_father_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          RecursiveUpdateKeyForRedistribute(receiver_old_key,
                                            std::make_pair(new_first_key, leaf_write->GetPageId()),
                                            receiver_father_write);
        }
      }
    }
}


FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeForInternal(page_id_t page_id, BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>* internal_write) {
  //如果是左边的 leaf和left要分清楚
  auto father_id=internal_write->GetFatherPageId();
  if (father_id == INVALID_PAGE_ID) {
    return;  // 无父页，直接返回
  }
  auto father_guard=bpm_->WritePage(father_id);
  auto father_write=father_guard.template AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();

  if (page_id==internal_write->GetPrePageId(father_write)) {
    auto left_guard=bpm_->WritePage(page_id);
    auto left_write=left_guard.AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
    //保存当前内页的旧第一个键（重分配前的第一个键）
    auto old_first_key = internal_write->KeyAt(0);
    //将左页最后一个元素抛出 放到当前页
    auto back= left_write->PopBack();
    internal_write->InsertBegin(back);
    //然后更新分裂键，并递归向上更新
    //将当前内页的新第一个键传入 然后更新分裂键（如果更新的不是首位键，递归函数会自动停止递归）
    RecursiveUpdateKeyForRedistribute(old_first_key, std::make_pair(internal_write->KeyAt(0), internal_write->GetPageId()), father_write);
  }else {
    //右边处理逻辑
    auto right_guard=bpm_->WritePage(page_id);
    auto right_write=right_guard.AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
    //保存右内页的旧第一个键（重分配前的第一个键）
    auto old_right_first_key = right_write->KeyAt(0);
    //将右边页的第一个元素抛出 放到当前页末尾
    auto front=right_write->PopFront();
    internal_write->InsertBack(front);
    //更新分裂键，并递归向上更新
    //将右内页的新第一个键传入 然后更新分裂键（如果更新的不是首位键，递归函数会自动停止递归）
    RecursiveUpdateKeyForRedistribute(old_right_first_key, std::make_pair(right_write->KeyAt(0), right_write->GetPageId()), father_write);
  }
}


//叶子节点合并 //优先和左页合并 前提是父亲页相同
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeForLeaf(WritePageGuard &leaf_guard) {
  auto leaf_write = leaf_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //获取左页信息
  auto left_id=leaf_write->GetPrePageId();
  if (left_id!=INVALID_PAGE_ID) {
    auto left_guard=bpm_->WritePage(left_id);
    auto left_write=left_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //如果父亲相同 说明可行 视为可合并对象
    if (left_write->GetFatherPageId()==leaf_write->GetFatherPageId()) {

      leaf_write->SetPrePageId(left_write->GetPrePageId());
      //将左页的pre页 的nextid 改成当前页id
      if (left_write->GetPrePageId()!=INVALID_PAGE_ID) {
        auto pre_guard=bpm_->WritePage(left_write->GetPrePageId());
        auto pre_write=pre_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
        pre_write->SetNextPageId(leaf_write->GetPageId());
        pre_guard.Drop();
      }

      auto pre_size=leaf_write->GetSize();
      std::optional<KeyType> pre_absorb_first_key=std::nullopt;
      if (pre_size!=0) {
        pre_absorb_first_key =leaf_write->GetMinKey();
      }
      //将左叶融入当前页
      leaf_write->Absorb(left_write);
      
      // 保存需要的信息，然后删除被合并的页面
      KeyType left_min_key = left_write->GetMinKey();
      page_id_t left_page_id_to_delete = left_write->GetPageId();
      auto father_id=leaf_write->GetFatherPageId();
      left_guard.Drop();  // 先 drop left_guard，然后才能删除页面
      
      auto father_page=bpm_->WritePage(father_id);
      auto father_write=father_page.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

      //虽然size为0 但是还是能访问到第一个key
      auto index= father_write->MatchKey(left_min_key,comparator_);

      //直接夺舍
      if (pre_size==0) {
        father_write->UpdateKey(index,leaf_write->GetPageId());
      }else {
          //删除本页在父页的信息
          auto tem_index=father_write->MatchKey(pre_absorb_first_key.value(),comparator_);
          father_write->DeletePair(tem_index);

          father_write->UpdateKey(index,leaf_write->GetPageId());
      }
      // 删除被合并的页面（left_write）
      bpm_->DeletePage(left_page_id_to_delete);
      return;
    }
  }

  auto right_id=leaf_write->GetNextPageId();
  if (right_id!=INVALID_PAGE_ID){
  //选右页为目标来合并
  auto right_guard=bpm_->WritePage(right_id);
  auto right_write=right_guard. template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (leaf_write->GetFatherPageId()==right_write->GetFatherPageId()) {
    //将当前页的nextid 设置成rightwrite的nextid
    leaf_write->SetNextPageId(right_write->GetNextPageId());
    //将right页的next页的preid 设置成当前页
    if (right_write->GetNextPageId()!=INVALID_PAGE_ID) {
      auto rnext_guard=bpm_->WritePage(right_write->GetNextPageId());
      auto rnext_write=rnext_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      rnext_write->SetPrePageId(leaf_write->GetPageId());
      rnext_guard.Drop();
    }

    auto pre_is_empty=leaf_write->GetSize();
    //将右页融入当前页
    auto begin_key= leaf_write->Absorb(right_write);
    
    // 保存需要的信息，然后删除被合并的页面
    page_id_t right_page_id_to_delete = right_write->GetPageId();
    auto father_id=leaf_write->GetFatherPageId();
    right_guard.Drop();  // 先 drop right_guard，然后才能删除页面

    auto father_page=bpm_->WritePage(father_id);
    auto father_write=father_page.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (pre_is_empty!=0) {
      //说明有节点在 无需换右页的节点
    auto index= father_write->MatchKey(begin_key,comparator_);
    father_write->DeletePair(index);
    }else {
      auto index= father_write->MatchKey(begin_key,comparator_);
      father_write->UpdateKey(index,leaf_write->GetPageId());
    }
    // 删除被合并的页面（right_write）
    bpm_->DeletePage(right_page_id_to_delete);
  }
  }
}


FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeForInternal(WritePageGuard &internal_guard) {
  auto internal_write = internal_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  auto father_page_id=internal_write->GetFatherPageId();
  if (father_page_id==INVALID_PAGE_ID) {
    return;
  }
  auto father_guard=bpm_->WritePage(father_page_id);
  auto father_write=father_guard.template AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
  //获取左页信息
  auto left_id=internal_write->GetPrePageId(father_write);
  if (left_id!=INVALID_PAGE_ID) {
    auto left_guard=bpm_->WritePage(left_id);
    auto left_write=left_guard.template AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
    //如果父亲相同 说明可行 视为可合并对象
    if (left_write->GetFatherPageId()==internal_write->GetFatherPageId()) {
      std::vector<page_id_t> v;
      //将当前页融入左页
      auto begin_key= left_write->Absorb(internal_write, v);
      //进行父id更新操作
      for (int i : v) {
        auto temp_guard = bpm_->WritePage(i);
        auto temp_write = temp_guard.AsMut<BPlusTreePage>();
        //写成internal_write了
        temp_write->SetFatherPageId(left_write->GetPageId());
      }
      //然后删除父页里的键
      auto index= father_write->MatchKey(begin_key,comparator_);
      father_write->DeletePair(index);
      // 删除被合并的页面（internal_write）
      page_id_t page_id_to_delete = internal_write->GetPageId();
      internal_guard.Drop();
      bpm_->DeletePage(page_id_to_delete);
      return;
    }
  }
  auto right_id=internal_write->GetNextPageId(father_write);
  if (right_id==INVALID_PAGE_ID) {
    return;
  }
  //选右页为目标来合并
  auto right_guard=bpm_->WritePage(right_id);
  auto right_write=right_guard. template AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
  if (internal_write->GetFatherPageId()==right_write->GetFatherPageId()) {
    //将右页融入当前页 同时将右页的叶子页加入vector
    std::vector<page_id_t> v;
    auto begin_key= internal_write->Absorb(right_write,v);
    //进行父id更新操作
    for(int i : v) {
      auto temp_guard=bpm_->WritePage(i);
      auto temp_write=temp_guard.AsMut<BPlusTreePage>();
      temp_write->SetFatherPageId(internal_write->GetPageId());
    }
    //然后删除父页里的键
    auto index= father_write->MatchKey(begin_key,comparator_);
    father_write->DeletePair(index);
    // 删除被合并的页面（right_write）
    page_id_t page_id_to_delete = right_write->GetPageId();
    right_guard.Drop();
    bpm_->DeletePage(page_id_to_delete);
  }
}


FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CheckForInternal(WritePageGuard &internal_guard) {
  auto internal_write = internal_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  //如果少了 就分配
  if (internal_write->GetSize()<internal_write->GetMinSize()) {
    //优先分配
    int to_distribute_size=internal_write->GetMinSize()-internal_write->GetSize();
    auto page_id=IsDistributeForInternal(internal_write,to_distribute_size);
    if (page_id!=INVALID_PAGE_ID) {
      RedistributeForInternal(page_id,internal_write);
    }else {
      //如果size为0 还找不到分配 就直接删除
      if (internal_write->GetSize()==0) {
        auto father_id=internal_write->GetFatherPageId();
        page_id_t page_id_to_delete = internal_write->GetPageId();
        if (father_id==INVALID_PAGE_ID) {
          //说明是顶层内页 执行root_page_id重置操作
          // 先 drop internal_guard，然后才能删除页面
          internal_guard.Drop();
          auto header_guard = bpm_->WritePage(header_page_id_);
          auto header_page_write = header_guard.AsMut<BPlusTreeHeaderPage>();
          header_page_write->root_page_id_=INVALID_PAGE_ID;
          bpm_->DeletePage(page_id_to_delete);
          return;
        }
        // 先 drop internal_guard，然后才能删除页面
        internal_guard.Drop();
        auto father_guard=bpm_->WritePage(father_id);
        bpm_->DeletePage(page_id_to_delete);
        CheckForInternal(father_guard);
        return;
      }
      //会导致上层父页少个键值对 使用check检测
      MergeForInternal(internal_guard);
      auto father_id=internal_write->GetFatherPageId();
      if (father_id==INVALID_PAGE_ID) {
        return;
      }
      auto father_guard=bpm_->WritePage(father_id);
      internal_guard.Drop();
      CheckForInternal(father_guard);
    }
  }
}


//可以递归循环向上检查 因为内页也有最小限制
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CheckForLeaf(WritePageGuard &leaf_guard) {
    auto leaf_write = leaf_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //如果少了 就分配（基于物理大小判断）
    // 分裂判断和下溢判断都基于物理大小（GetSize()），包括墓碑在内的所有键值对
    if (leaf_write->GetSize() < leaf_write->GetMinSize()) {
      //优先分配
      auto page_id=IsDistributeForLeaf(leaf_write);
      if (page_id!=INVALID_PAGE_ID) {
        RedistributeForLeaf(page_id, leaf_write);
      }else {
        MergeForLeaf(leaf_guard);
        auto father_id=leaf_write->GetFatherPageId();
        if (father_id==INVALID_PAGE_ID) {
          return;
        }
        auto father_guard=bpm_->WritePage(father_id);
        leaf_guard.Drop();
        CheckForInternal(father_guard);
      }
    }
}

/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 * @param update_key
 * @param is_update
 */
//传入的是父亲的write 进入至少是到三层了
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeepDeleteOrUpdate(const KeyType &key, std::optional<KeyType> update_key, BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>* internal_write, bool
                                        is_update) {

  //删除传入write的父页相关的信息
  auto father_id=internal_write->GetFatherPageId();
  if (father_id==INVALID_PAGE_ID) {
    //说明是顶层内页
    return;
  }
  auto father_guard=bpm_->WritePage(father_id);
  auto father_write=father_guard.template AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
  //匹配到 然后更新
  auto father_index=father_write->MatchKey(key,comparator_);
  if (father_index==-1) {
    return ;
  }
  if (is_update) {
    father_write->UpdateKey(father_index,update_key);
    if (father_index==0) {
      DeepDeleteOrUpdate(key,update_key, father_write,true);
    }
  }else {
    father_write->DeletePair(father_index);
    if (father_index==0){
      if (father_write->GetSize()!=0) {
        //找到更新键了
        DeepDeleteOrUpdate(key,father_write->GetMinKey(), father_write,true);
      }else {
        //如果还找不出更新键 就继续删
        DeepDeleteOrUpdate(key,update_key,father_write,false);
      }
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeepUpdate(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_write, KeyType temp_key)
{
  //进行递归更新父页信息  size为空或者没有删首键不需要更新
  if (leaf_write->GetFatherPageId()!=INVALID_PAGE_ID) {
    //检查是否需要更新处理 同时要不为空
    if (leaf_write->IsUpdate()&&!leaf_write->IsEmpty()){
      auto father_guard_w=bpm_->WritePage(leaf_write->GetFatherPageId());
      auto father_write=father_guard_w . template AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
      //父页更新
      auto father_index=father_write->MatchKey(temp_key,comparator_);

      if (father_index==0) {
        //更新已经删除的首键信息
        father_write->UpdateKey(father_index,leaf_write->GetMinKey());
        //递归删除逻辑 一次性把头上的页的信息全部删除 这样当内页删除的时候 就无需进行父页的信息删除了
        DeepDeleteOrUpdate(temp_key,leaf_write->GetMinKey(),father_write,true);
      }else {
        //更新已经删除的首键信息
        father_write->UpdateKey(father_index,leaf_write->GetMinKey());
      }
      leaf_write->SetIsUpdate(false);
    }
    //如果为空 就直接删
    if (leaf_write->IsEmpty()) {
      auto father_guard_w=bpm_->WritePage(leaf_write->GetFatherPageId());
      auto father_write=father_guard_w . template AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
      auto father_index=father_write->MatchKey(temp_key,comparator_);
      if (father_index==0) {
        father_write->DeletePair(father_index);
        if (father_write->GetSize()!=0) {
          //找到更新键了
          DeepDeleteOrUpdate(temp_key,father_write->GetMinKey(), father_write,true);
        }else {
          //如果还找不出更新键 就继续删
          DeepDeleteOrUpdate(temp_key,std::nullopt,father_write,false);
        }
      }else {
        father_write->DeletePair(father_index);
      }
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::DistributionClean(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_write) {
  if (leaf_write->GetSize()<leaf_write->GetMinSize()) {
    auto id=IsDistributeForLeaf(leaf_write);
    if (id!=INVALID_PAGE_ID) {
        leaf_write->CleanupTombs();

        if (leaf_write->IsUpdate() && !leaf_write->IsEmpty()) {
          auto father_guard_w = bpm_->WritePage(leaf_write->GetFatherPageId());
          auto father_write = father_guard_w.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

          auto father_index = father_write->MatchKey(leaf_write->GetBeforeFirstKey(), comparator_);

          //说明是父页的首键 就要递归向上更新信息
          if (father_index==0) {
            //先将当前的还在生效的首键 更新到父页
            father_write->UpdateKey(father_index, leaf_write->GetMinKey());

             //然后更新父亲的父亲页
            DeepDeleteOrUpdate(leaf_write->GetBeforeFirstKey(), leaf_write->GetMinKey(), father_write, true);
          }else {
            //删除的不是父亲的首键 无需递归更新
            father_write->UpdateKey(father_index, leaf_write->GetMinKey());
          }
        leaf_write->SetIsUpdate(false);
      }else if (leaf_write->IsEmpty()) {
          auto father_guard_w = bpm_->WritePage(leaf_write->GetFatherPageId());
          auto father_write = father_guard_w.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

          auto father_index = father_write->MatchKey(leaf_write->GetBeforeFirstKey(), comparator_);

          if (father_index == 0) {
            //删除 有关删除键的信息
            father_write->DeletePair(father_index);
            //如果父亲页有第二人选
            if (father_write->GetSize() != 0) {
              DeepDeleteOrUpdate(leaf_write->GetBeforeFirstKey(), father_write->GetMinKey(), father_write, true);
            } else {
              //如果父亲页没有第二人选
              DeepDeleteOrUpdate(leaf_write->GetBeforeFirstKey(), std::nullopt, father_write, false);
            }
          }else {
            //删除的不是父亲的首键 无需递归更新
            father_write->DeletePair(father_index);
          }
        leaf_write->SetIsUpdate(false);
      }
      return true;
    }else {
      return false;
    }
  }
  return false;
}

//是否是右页并入当前页
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsRightMerge(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_write) ->std::optional<bool>{
  auto left_id=leaf_write->GetPrePageId();
  if (left_id!=INVALID_PAGE_ID) {
    auto left_guard=bpm_->WritePage(left_id);
    auto left_write=left_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //如果父亲相同 说明可行 视为可合并对象
    if (left_write->GetFatherPageId()==leaf_write->GetFatherPageId()) {
      return false;
    }
  }

  auto right_id=leaf_write->GetNextPageId();
  if (right_id!=INVALID_PAGE_ID) {
    auto right_guard=bpm_->WritePage(right_id);
    auto right_write=right_guard. template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    if (leaf_write->GetFatherPageId()==right_write->GetFatherPageId()) {
      return true;
    }
  }
  return std::nullopt;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
std::optional<bool> BPLUSTREE_TYPE::MergeClean(WritePageGuard &leaf_guard) {
  auto leaf_write = leaf_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  //如果下溢出了
  if (leaf_write->GetSize()<leaf_write->GetMinSize()) {
    //是否是当前页吸收右页
    auto result=IsRightMerge(leaf_write);

      leaf_write->CleanupTombs();

     //进行父页的更新 如果isupdate为1 而且上面已经墓碑清除了 说明已经将被标记的首键给清除了 此时用beforekey来更新父页
      if (leaf_write->IsUpdate() && !leaf_write->IsEmpty()) {

        leaf_write->SetIsUpdate(false);

        auto father_guard_w = bpm_->WritePage(leaf_write->GetFatherPageId());
        auto father_write = father_guard_w.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();


        auto father_index = father_write->MatchKey
        (leaf_write->GetBeforeFirstKey(), comparator_);

        //说明是父页的首键 就要递归向上更新信息
        if (father_index==0) {
          //先将当前的还在生效的首键 更新到父页
          father_write->UpdateKey(father_index, leaf_write->GetMinKey());

          //然后更新父亲的父亲页
          DeepDeleteOrUpdate(leaf_write->GetBeforeFirstKey(), leaf_write->GetMinKey(), father_write, true);
        }else {
          //删除的不是父亲的首键 无需递归更新
          father_write->UpdateKey(father_index, leaf_write->GetMinKey());
        }
        leaf_write->SetIsUpdate(false);

      }else if (leaf_write->IsEmpty()) {
        //默认清除了首键

        leaf_write->SetIsUpdate(false);

        auto father_guard_w = bpm_->WritePage(leaf_write->GetFatherPageId());
        auto father_write = father_guard_w.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

        auto father_index = father_write->MatchKey(leaf_write->GetBeforeFirstKey(), comparator_);

        if (father_index == 0) {
          //删除 有关删除键的信息
          father_write->DeletePair(father_index);
          //如果父亲页有第二人选
          if (father_write->GetSize() != 0) {
            DeepDeleteOrUpdate(leaf_write->GetBeforeFirstKey(), father_write->GetMinKey(), father_write, true);
          } else {
            //如果父亲页没有第二人选
            DeepDeleteOrUpdate(leaf_write->GetBeforeFirstKey(), std::nullopt, father_write, false);
      }
    }else {
          //删除的不是父亲的首键 无需递归更新
          father_write->DeletePair(father_index);
        }
        leaf_write->SetIsUpdate(false);
      }

    //无法合并
    if (!result.has_value()) {
      //执行页删除工作
      // 在 drop guard 之前先保存所有需要的信息
      page_id_t next_page_id = leaf_write->GetNextPageId();
      page_id_t pre_page_id = leaf_write->GetPrePageId();
      page_id_t father_id = leaf_write->GetFatherPageId();
      page_id_t page_id_to_delete = leaf_write->GetPageId();
      
      // 先 drop leaf_guard，然后才能删除页面和更新邻居
      leaf_guard.Drop();
      
      // 处理左右叶子页的preid 和nextid的关系
      if (next_page_id != INVALID_PAGE_ID) {
        auto right_guard = bpm_->WritePage(next_page_id);
        auto right_write = right_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
        right_write->SetPrePageId(pre_page_id);
      }

      if (pre_page_id != INVALID_PAGE_ID) {
        auto left_guard = bpm_->WritePage(pre_page_id);
        auto left_write = left_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
        left_write->SetNextPageId(next_page_id);
      }

      bpm_->DeletePage(page_id_to_delete);
      if (father_id == INVALID_PAGE_ID) {
        return false;
      }
      auto father_guard = bpm_->WritePage(father_id);
      CheckForInternal(father_guard);
      return false;
    }
    return true;
  }


  return std::nullopt;
}


FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  // 获取头页，读取根页ID
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  // 如果树为空，返回false
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return ;
  }
  //找到要删除的键的页
  auto page_id=LocateKey(key,header_page);
  //先释放根页 防止根页为叶子页的情况
  header_guard.Drop();
  //执行叶子页的delete函数 标记里面的键值对
  auto leaf_guard=bpm_->WritePage(page_id);
  auto leaf_write=leaf_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  // 乐观删除 标记为墓碑或直接物理删除
  leaf_write->Delete(key, comparator_);


  if (LEAF_PAGE_TOMB_CNT != 0&&leaf_write->GetNeedUpdate()) {

    leaf_write->SetNeedUpdate(false);
    //说明此页未下溢出 无需check
    if (leaf_write->IsUpdate() && !leaf_write->IsEmpty()) {
      DeepUpdate(leaf_write, leaf_write->GetBeforeFirstKey());
    } else if (leaf_write->IsUpdate() && leaf_write->IsEmpty()) {
      // 如果页为空，需要删除父页中的条目
      auto father_guard_w = bpm_->WritePage(leaf_write->GetFatherPageId());
      auto father_write = father_guard_w.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

      auto father_index = father_write->MatchKey(leaf_write->GetBeforeFirstKey(), comparator_);

      if (father_index == 0) {
        father_write->DeletePair(father_index);
        if (father_write->GetSize() != 0) {
          DeepDeleteOrUpdate(leaf_write->GetBeforeFirstKey(), father_write->GetMinKey(), father_write, true);
        } else {
          DeepDeleteOrUpdate(leaf_write->GetBeforeFirstKey(), std::nullopt, father_write, false);
        }
      } else {
        father_write->DeletePair(father_index);
      }
      leaf_write->SetIsUpdate(false);
    }

    if (leaf_write->IsTombstone(0)) {
      leaf_write->SetIsUpdate(true);
    }
  }
  //如果能进行重分配 进行清除墓碑
  if (!DistributionClean(leaf_write)) {
    //是右则清除 否则不清除
    auto result= MergeClean(leaf_guard);

    if (result.has_value() && !result.value()) {
      //说明页已被删除，直接返回
      return;
    }
  }
  // 检查是否需要合并或重分配（基于逻辑大小）
  CheckForLeaf(leaf_guard);
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
    //返回一个迭代器类
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page_read = header_guard.As<BPlusTreeHeaderPage>();
  auto  find_guard = bpm_->ReadPage(header_page_read->root_page_id_);
  auto temp_root_page_write =  find_guard.As<BPlusTreePage>();
  auto page_id = INVALID_PAGE_ID;

  if (temp_root_page_write->IsLeafPage()) {
    return IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>(bpm_, temp_root_page_write->GetPageId());
  }else{
    auto root_page_r =  find_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    //先查一层 防止用root_page_guard来进行循环 减少bug出现概率
    auto temp_id =root_page_r->ValueAt(0);
    //读取这个id的信息
    auto temp_guard = bpm_->ReadPage(temp_id);
    auto temp_write_b_page = temp_guard.template As<BPlusTreePage>();
    //发现不是叶子页 就循环find
    // 注意：不要在Drop guard后继续使用指针，会导致悬空指针问题
    bool is_leaf = temp_write_b_page->IsLeafPage();
    temp_guard.Drop();
    
    while (!is_leaf) {
      auto guard_while = bpm_->ReadPage(temp_id);
      auto temp_check_page = guard_while.template As<BPlusTreePage>();
      // 安全检查：确保页面确实是内部页，避免类型转换错误
      if (temp_check_page->IsLeafPage()) {
        // 如果实际上是叶子页，说明数据不一致或逻辑错误
        return IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>(bpm_, INVALID_PAGE_ID);
      }
      // 额外检查：验证 page_id 是否有效
      page_id_t check_page_id = temp_check_page->GetPageId();
      if (check_page_id != temp_id) {
        // 页面数据不一致：页面中存储的 page_id 与实际 page_id 不匹配
        // 这可能是页面数据损坏或类型转换错误的标志
        return IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>(bpm_, INVALID_PAGE_ID);
      }
      auto temp_write_internal_page = guard_while.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      //继续在此内页上查找
      temp_id = temp_write_internal_page->ValueAt(0);
      //复用循环外的变量 实现while循环查找
      guard_while.Drop();
      temp_guard = bpm_->ReadPage(temp_id);
      temp_write_b_page = temp_guard.template As<BPlusTreePage>();
      // 在Drop之前先检查是否是叶子页
      is_leaf = temp_write_b_page->IsLeafPage();
      temp_guard.Drop();
    }
    page_id = temp_id;
  }
  return IndexIterator<KeyType,ValueType,KeyComparator,NumTombs>(bpm_,page_id);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  //返回一个迭代器类
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page_read = header_guard.As<BPlusTreeHeaderPage>();
  auto  find_guard = bpm_->ReadPage(header_page_read->root_page_id_);
  auto temp_root_page_write =  find_guard.As<BPlusTreePage>();
  auto page_id = INVALID_PAGE_ID;

  if (temp_root_page_write->IsLeafPage()) {
    return IndexIterator<KeyType,ValueType,KeyComparator,NumTombs>(bpm_,temp_root_page_write->GetPageId());
  }else{
    auto root_page_write =  find_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    //先查一层 防止用root_page_guard来进行循环 减少bug出现概率
    auto temp_id = root_page_write->AccurateFind(comparator_, key);
    //读取这个id的信息
    auto temp_guard = bpm_->ReadPage(temp_id);
    auto temp_write_b_page = temp_guard.template As<BPlusTreePage>();
    //发现不是叶子页 就循环find
    // 注意：不要在Drop guard后继续使用指针，会导致悬空指针问题
    bool is_leaf = temp_write_b_page->IsLeafPage();
    temp_guard.Drop();
    
    while (!is_leaf) {
      //先将第一次查找的那一页 变成internal 页
      auto guard_while = bpm_->ReadPage(temp_id);
      auto temp_check_page = guard_while.template As<BPlusTreePage>();
      // 安全检查：确保页面确实是内部页，避免类型转换错误
      if (temp_check_page->IsLeafPage()) {
        // 如果实际上是叶子页，说明数据不一致或逻辑错误
        return IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>(bpm_, INVALID_PAGE_ID);
      }
      // 额外检查：验证 page_id 是否有效
      page_id_t check_page_id = temp_check_page->GetPageId();
      if (check_page_id != temp_id) {
        // 页面数据不一致：页面中存储的 page_id 与实际 page_id 不匹配
        // 这可能是页面数据损坏或类型转换错误的标志
        return IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>(bpm_, INVALID_PAGE_ID);
      }
      auto temp_write_internal_page = guard_while.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      //继续在此内页上查找
      temp_id = temp_write_internal_page->AccurateFind(comparator_, key);
      if (temp_id == INVALID_PAGE_ID) {
        return IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>(bpm_, INVALID_PAGE_ID);
      }
      //复用循环外的变量 实现while循环查找
      guard_while.Drop();
      temp_guard = bpm_->ReadPage(temp_id);
      temp_write_b_page = temp_guard.template As<BPlusTreePage>();
      // 在Drop之前先检查是否是叶子页
      is_leaf = temp_write_b_page->IsLeafPage();
      temp_guard.Drop();
    }
    page_id = temp_id;
  }
  return IndexIterator<KeyType,ValueType,KeyComparator,NumTombs>(bpm_,page_id);
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>(bpm_, -1);
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto header_guard = bpm_->ReadPage(header_page_id_);
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