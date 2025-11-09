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

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateFather(KeyType first_key, KeyType second_key, WritePageGuard& write_guard) {
  auto write=write_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  //获取父页write
  auto father_id=write->GetFatherPageId();
  if (father_id==INVALID_PAGE_ID) {
    return ;
  }
  auto father_guard=bpm_->WritePage(father_id);
  auto father_write=father_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

  auto index=father_write->MatchKey(first_key,comparator_);
  father_write->UpdateKey(index,second_key);
  if (index==0) {
    UpdateFather(first_key,second_key,father_guard);
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
      temp_guard.Drop();
      while (!temp_write_b_page->IsLeafPage()) {
        //先将第一次查找的那一页 变成internal 页
        auto guard_while = bpm_->ReadPage(temp_id);
        auto temp_write_internal_page = guard_while.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        //继续在此内页上查找
        temp_id = temp_write_internal_page->Find(comparator_, key);
        //复用循环外的变量 实现while循环查找
        temp_guard = bpm_->ReadPage(temp_id);
        temp_write_b_page = temp_guard.template As<BPlusTreePage>();
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
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *second_internal_write) -> KeyType {
  std::vector<page_id_t> v;
  auto key= first_internal_write->Split(second_internal_write,v);
  //取出来 将新内页的元素 father_id 全部改了
  for (auto &item:v) {
    auto temp_guard=bpm_->WritePage(item);
    auto write=temp_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    write->SetFatherPageId(second_internal_write->GetPageId());
  }
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
FULL_INDEX_TEMPLATE_ARGUMENTS
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
      return false;
    }
    header_guard.Drop();
    PushUp(root_page_write->GetPageId(),  root_guard);
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
      temp_guard.Drop();
      while (!temp_write_b_page->IsLeafPage()) {
        //先将第一次查找的那一页 变成internal 页
        auto guard_while = bpm_->ReadPage(temp_id);
        auto temp_write_internal_page = guard_while.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        //继续在此内页上查找
        temp_id = temp_write_internal_page->AccurateFind(comparator_, key);
        //复用循环外的变量 实现while循环查找
        temp_guard = bpm_->ReadPage(temp_id);
        temp_write_b_page = temp_guard.template As<BPlusTreePage>();
        temp_guard.Drop();
      }
      page_id = temp_id;
    }
    //根据位置 插入到指定叶子页
    auto leaf_page_guard = bpm_->WritePage(page_id);
    auto leaf_page_write = leaf_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    auto begin_key=leaf_page_write->GetMinKey();
    leaf_page_write->InsertKeyValue(comparator_, key, value);
    //进入前资源释放 带guard的都要进行释放
    header_guard.Drop();
     find_guard.Drop();
    //递归更新父页的信息
    if (leaf_page_write->IsBegin()) {
        UpdateFather(begin_key,leaf_page_write->GetMinKey(),leaf_page_guard);
      leaf_page_write->SetBegin(false);
    }
    PushUp(leaf_page_write->GetPageId(), leaf_page_guard);
  }
  return true;
}

//外部执行 基本查找 并且插入逻辑
//每次执行插入操作 就将被插入的叶子id传入
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PushUp(page_id_t id, WritePageGuard& write_guard) {
  //如果当前页满了就 创建新叶子页 然后分裂
  auto temp_write = write_guard.As<BPlusTreePage>();
  if (temp_write->IsLeafPage()) {
    auto page_write=write_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    //一层模型 如果第一个叶子页满了 就先分裂 再判断有无父页
    if (page_write->GetMaxSize() == page_write->GetSize()) {
      auto new_page_id = bpm_->NewPage();
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
        auto internal_page_guard = bpm_->WritePage(internal_page_id);
        auto internal_page_write = internal_page_guard.AsMut<BPlusTreeInternalPage<
          KeyType, page_id_t, KeyComparator>>();
        internal_page_write->Init(GetInternalMaxSize());
        internal_page_write->SetPageId(internal_page_id);
        //将分裂的两个叶子页的父节点设置成当前internal页
        new_page_write->SetFatherPageId(internal_page_id);
        page_write->SetFatherPageId(internal_page_id);
        //然后更新root_page_id
        auto head_guard=bpm_->WritePage(header_page_id_);
        auto head_write=head_guard.AsMut<BPlusTreeHeaderPage>();
        head_write->root_page_id_ = internal_page_id;
        //设置internal页的键值对
        internal_page_write->FirstInsert(page_write->GetMinKey(),new_page_write->GetMinKey(), page_write->GetPageId(),new_page_write->GetPageId());
      }else{
        //分裂后 将新的页的数据 加入到内页
        auto father_page_guard = bpm_->WritePage(page_write->GetFatherPageId());
        auto father_page_write = father_page_guard.template AsMut<BPlusTreeInternalPage<
          KeyType, page_id_t, KeyComparator>>();
        father_page_write->InsertKeyValue(comparator_, new_page_write->GetMinKey(), new_page_write->GetPageId());
        //设置新页的父亲id
        new_page_write->SetFatherPageId(father_page_write->GetPageId());
        //pushup 父页
        write_guard.Drop();
        new_page_guard.Drop();
        PushUp(father_page_write->GetPageId(), father_page_guard);
      }
    }
  }else {
    auto page_write=write_guard.AsMut<BPlusTreeInternalPage<
            KeyType, page_id_t, KeyComparator>>();
    if (GetInternalMaxSize() == page_write->GetSize()) {
      //创建新内页
      auto new_internal_page_id = bpm_->NewPage();
      auto new_internal_guard = bpm_->WritePage(new_internal_page_id);
      auto new_internal_write = new_internal_guard.AsMut<BPlusTreeInternalPage<
        KeyType, page_id_t, KeyComparator>>();
      new_internal_write->Init(GetInternalMaxSize());
      new_internal_write->SetPageId(new_internal_page_id);
      //内页满了之后的分裂要特化处理 因为要处理地下叶子页的父节点关系
      auto split_key = SplitForInternal(page_write, new_internal_write); //实现内页分裂逻辑
      //建立父内页 提高树高度 将其设置成根页 //这是当高度第一次到3的时候 才要这样操作
      //parent>father
      page_id_t parent_internal_page_id = page_write->GetFatherPageId();
      //此为2层 但是2层已经满了 要升级为3层逻辑
      if (parent_internal_page_id == INVALID_PAGE_ID) {
        parent_internal_page_id = bpm_->NewPage();
        auto parent_internal_guard = bpm_->WritePage(parent_internal_page_id);
        auto parent_internal_write = parent_internal_guard.AsMut<BPlusTreeInternalPage<
          KeyType, page_id_t, KeyComparator>>();
        parent_internal_write->Init(GetInternalMaxSize());
        parent_internal_write->SetPageId(parent_internal_page_id);
        //由于父节点的id已经出现 更新两个内页的parent_id
        page_write->SetFatherPageId(parent_internal_page_id);
        new_internal_write->SetFatherPageId(parent_internal_page_id);
        //插入刚刚获得的分裂键
        parent_internal_write->FirstInsert(page_write->GetMinKey(), split_key,
                                           page_write->GetPageId(), new_internal_write->GetPageId());
        //更新 根页id;
        auto head_guard=bpm_->WritePage(header_page_id_);
        auto head_write=head_guard.AsMut<BPlusTreeHeaderPage>();
        head_write->root_page_id_ = parent_internal_write->GetPageId();
      }else {
        //此为 已经是3层了 然后继续插入的逻辑 也就是
        auto parent_internal_guard = bpm_->WritePage(parent_internal_page_id);
        auto parent_internal_write = parent_internal_guard.AsMut<BPlusTreeInternalPage<
          KeyType, page_id_t, KeyComparator>>();
        //插入刚刚获得的分裂键
        parent_internal_write->InsertKeyValue(comparator_, split_key, new_internal_write->GetPageId());
        //更新新内页的父亲页id //差点忘记 但是看着注释分析出来忘记设置了
        new_internal_write->SetFatherPageId(parent_internal_write->GetPageId());
        //3层模型 当第3层页也插满了
        write_guard.Drop();
        new_internal_guard.Drop();
        PushUp(parent_internal_write->GetPageId(), parent_internal_guard);
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
  auto left_id =leaf_write->GetPrePageId();
  auto right_id=leaf_write->GetNextPageId();
 if (left_id!=INVALID_PAGE_ID){
    auto left_guard=bpm_->ReadPage(left_id);
    auto left_write=left_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    if (left_write->GetSize()>left_write->GetMinSize()) {
        return left_id;
    }
  }
  if (right_id!=INVALID_PAGE_ID) {
    auto right_guard=bpm_->ReadPage(right_id);
    auto right_write=right_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    if (right_write->GetSize()>right_write->GetMinSize()) {
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
      //将左页最后一个元素抛出 放到当前页
      auto back= left_write->PopBack();
      leaf_write->InsertBegin(back);
      //然后更新分裂键 TODO(wwz): 需处理无父页的情况
      auto father_guard=bpm_->WritePage(leaf_write->GetFatherPageId());
      auto father_write=father_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      //将当前叶子页的第二个键传入 然后更新分裂键
      father_write->UpdateKey(leaf_write->KeyAt(1), std::make_pair(leaf_write->KeyAt(0), leaf_write->GetPageId()), comparator_);
    }else {
      //右边处理逻辑
      auto right_guard=bpm_->WritePage(page_id);
      auto right_write=right_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      //将右边页的第一个元素抛出 放到当前页末尾
      auto front=right_write->PopFront();
      leaf_write->InsertBack(front);
      //更新分裂键 //TODO(wwz) 可能无父页
      auto father_guard=bpm_->WritePage(leaf_write->GetFatherPageId());
      auto father_write=father_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      father_write->UpdateKey(front.first, std::make_pair(right_write->KeyAt(0), right_write->GetPageId()), comparator_);
    }
}


FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeForInternal(page_id_t page_id, BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>* internal_write) {
  //如果是左边的 leaf和left要分清楚
  auto father_id=internal_write->GetFatherPageId();
  auto father_guard=bpm_->WritePage(father_id);
  auto father_write=father_guard.template AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();

  if (page_id==internal_write->GetPrePageId(father_write)) {
    auto left_guard=bpm_->WritePage(page_id);
    auto left_write=left_guard.AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
    //将左页最后一个元素抛出 放到当前页
    auto back= left_write->PopBack();
    internal_write->InsertBegin(back);
    //然后更新分裂键 TODO(wwz): 需处理无父页的情况
    //将当前叶子页的第二个键传入 然后更新分裂键
    father_write->UpdateKey(internal_write->KeyAt(1),back,comparator_);
  }else {
    //右边处理逻辑
    auto right_guard=bpm_->WritePage(page_id);
    auto right_write=right_guard.AsMut<BPlusTreeInternalPage<KeyType,page_id_t, KeyComparator>>();
    //将右边页的第一个元素抛出 放到当前页末尾
    auto front=right_write->PopFront();
    internal_write->InsertBack(front);
    //更新分裂键 //TODO(wwz) 可能无父页
    father_write->UpdateKey(front.first,std::make_pair(right_write->KeyAt(0),right_write->ValueAt(0)),comparator_);
  }
}


//叶子节点合并 //优先和左页合并 前提是父亲页相同
//TODO(wwz): 处理合并失败的情况
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
      //将当前页的nextid 赋值给leftwrite的nextid
      left_write->SetNextPageId(leaf_write->GetNextPageId());
      //将当前页的next页 的preid 改成left页id
      auto right_guard=bpm_->WritePage(leaf_write->GetNextPageId());
      auto right_write=right_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      right_write->SetPrePageId(left_write->GetPageId());
      right_guard.Drop();

      //将当前页融入左页
      auto begin_key= left_write->Absorb(leaf_write);
      //然后删除父页里的键
      auto father_id=leaf_write->GetFatherPageId();
      auto father_page=bpm_->WritePage(father_id);
      auto father_write=father_page.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      auto index= father_write->MatchKey(begin_key,comparator_);
      father_write->DeletePair(index);
      return;
    }
  }
  auto right_id=leaf_write->GetNextPageId();
  if (right_id==INVALID_PAGE_ID) {
    return;
  }
  //选右页为目标来合并
  auto right_guard=bpm_->WritePage(right_id);
  auto right_write=right_guard. template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (leaf_write->GetFatherPageId()==right_write->GetFatherPageId()) {
    //将当前页的nextid 设置成rightwrite的nextid
    leaf_write->SetNextPageId(right_write->GetNextPageId());
    //将right页的next页的preid 设置成当前页
    auto rnext_guard=bpm_->WritePage(right_write->GetNextPageId());
    auto rnext_write=rnext_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    rnext_write->SetPrePageId(leaf_write->GetPageId());
    rnext_guard.Drop();

    //将右页融入当前页
    auto begin_key= leaf_write->Absorb(right_write);
    //然后删除父页里的键
    auto father_id=leaf_write->GetFatherPageId();
    auto father_page=bpm_->WritePage(father_id);
    auto father_write=father_page.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto index= father_write->MatchKey(begin_key,comparator_);
    father_write->DeletePair(index);
  }
}

//TODO(wwz): 处理合并失败的情况
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
  if (right_write->GetFatherPageId()==right_write->GetFatherPageId()) {
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
        if (father_id==INVALID_PAGE_ID) {
          //说明是顶层内页 执行root_page_id重置操作
          auto header_guard = bpm_->WritePage(header_page_id_);
          auto header_page_write = header_guard.AsMut<BPlusTreeHeaderPage>();
          header_page_write->root_page_id_=INVALID_PAGE_ID;
          bpm_->DeletePage(internal_write->GetPageId());
          return;
        }
        auto father_guard=bpm_->WritePage(father_id);
        bpm_->DeletePage(internal_write->GetPageId());
        internal_guard.Drop();
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
    //如果少了 就分配
    if (leaf_write->GetSize()<leaf_write->GetMinSize()) {
      //优先分配
      auto page_id=IsDistributeForLeaf(leaf_write);
      if (page_id!=INVALID_PAGE_ID) {
        RedistributeForLeaf(page_id, leaf_write);
      }else {
        //如果当前页size为0 而且还找不到分配 就直接删除
        if (leaf_write->GetSize()==0) {
          // 处理左右叶子也的preid 和nextid的关系
          if (leaf_write->GetNextPageId()!=INVALID_PAGE_ID) {
            auto right_guard=bpm_->WritePage(leaf_write->GetNextPageId());
            auto right_write=right_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
            right_write->SetPrePageId(leaf_write->GetPrePageId());
          }

          if (leaf_write->GetPrePageId()!=INVALID_PAGE_ID) {
            auto left_guard=bpm_->WritePage(leaf_write->GetPrePageId());
            auto left_write=left_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
            left_write->SetNextPageId(leaf_write->GetNextPageId());
          }

          auto father_id=leaf_write->GetFatherPageId();
          if (father_id==INVALID_PAGE_ID) {
            return;
          }
          bpm_->DeletePage(leaf_write->GetPageId());
          auto father_guard=bpm_->WritePage(father_id);
          leaf_guard.Drop();
          CheckForInternal(father_guard);
          return ;
        }
        //会导致上层父页少个键值对 使用check检测
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
  
  // 当墓碑数组大小为0时，已经进行了物理删除
  if (LEAF_PAGE_TOMB_CNT == 0) {
    // 乐观删除：如果删除后节点大小仍然大于等于最小大小，不触发合并或重新分配，也不更新父页
    // 只有当删除后节点大小小于最小大小时，才触发合并或重新分配，并更新父页
    if (leaf_write->GetSize() < leaf_write->GetMinSize()) {
      // 需要合并或重新分配时，才更新父页信息
      if (leaf_write->GetFatherPageId() != INVALID_PAGE_ID) {
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
        }
        //如果删了之后 为空的情况
        if (leaf_write->IsEmpty()) {
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
        }
      }
      //检测是否能合并或者重分配
      CheckForLeaf(leaf_guard);
    }
    return;
  }
  
  //判断是否要进行清除墓碑
  if (leaf_write->GetSize()<leaf_write->GetMinSize()||leaf_write->GetNumTombstones()>=LEAF_PAGE_TOMB_CNT||leaf_write->GetSize() + leaf_write->GetNumTombstones() >= static_cast<size_t>(leaf_write->GetMaxSize())) {

    //如果isupdate为true 就要用这个变量
    auto temp_key=leaf_write->GetMinKey();

     leaf_write->CleanupTombs();  // 物理删除所有墓碑

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
    temp_guard.Drop();//TODO 这个会导致短暂的guard缺失 然后指针还能用的情况
    while (!temp_write_b_page->IsLeafPage()) {
      auto guard_while = bpm_->ReadPage(temp_id);
      auto temp_write_internal_page = guard_while.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      //继续在此内页上查找
      temp_id = temp_write_internal_page->ValueAt(0);
      //复用循环外的变量 实现while循环查找
      temp_guard = bpm_->ReadPage(temp_id);
      temp_write_b_page = temp_guard.template As<BPlusTreePage>();
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
    temp_guard.Drop();
    while (!temp_write_b_page->IsLeafPage()) {
      //先将第一次查找的那一页 变成internal 页
      auto guard_while = bpm_->ReadPage(temp_id);
      auto temp_write_internal_page = guard_while.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      //继续在此内页上查找
      temp_id = temp_write_internal_page->AccurateFind(comparator_, key);
      //复用循环外的变量 实现while循环查找
      temp_guard = bpm_->ReadPage(temp_id);
      temp_write_b_page = temp_guard.template As<BPlusTreePage>();
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