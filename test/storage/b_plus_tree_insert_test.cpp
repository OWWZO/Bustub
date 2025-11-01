//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_utils.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

// 单元测试：测试B+树的基础插入功能
// 类比：这相当于在"图书馆图书索引系统"刚搭建好后，测试能否成功插入第一本图书的索引信息
TEST(BPlusTreeTests, BasicInsertTest) {
  // 1. 创建键比较器（KeyComparator）和索引结构描述（index schema）
  // 类比：
  // - "a bigint" 定义了索引键的类型是"大整数"，好比规定图书馆索引的键是"图书编号"（必须是数字且范围较大）
  // - key_schema 就是这份"图书编号规则说明文档"，明确键的格式、类型等信息
  // - comparator 是"编号比较工具"，用于判断两个图书编号的大小关系（比如判断编号42和50哪个更大）
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  // 2. 创建磁盘管理器和缓冲池管理器
  // 类比：
  // - DiskManagerUnlimitedMemory：相当于"无限容量的图书馆仓库"，可以存放所有图书索引数据，不用担心空间不足
  // - BufferPoolManager(50, ...)：相当于"图书馆前台的临时货架"，最多能放50本常用索引册，避免每次都去仓库取（提高效率）
  // - bpm（缓冲池管理器指针）：是管理这个临时货架的"前台管理员"，负责从仓库取索引、放回仓库、管理货架空间
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // 3. 分配B+树的根页面（header_page）
  // 类比：向"前台管理员"申请一个新的"空白索引册"（page），作为整个图书馆索引系统的第一本册子（根节点）
  // page_id 是这本空白索引册的唯一编号，方便后续查找
  page_id_t page_id = bpm->NewPage();

  // 4. 创建B+树实例
  // 类比：根据以下参数搭建"图书馆图书索引系统"（B+树）：
  // - "foo_pk"：索引名称，好比给这个索引系统起名为"foo图书主键索引"
  // - page_id：根页面编号，指定系统的第一本索引册是刚才申请的那本（编号为page_id）
  // - bpm：指定用哪个"前台管理员"来管理索引册的存取（即缓冲池管理器）
  // - comparator：指定用哪个"编号比较工具"来排序索引（即之前创建的GenericComparator）
  // - 2：最小度数（min degree），规定每个非叶节点最少有2个子节点（类比：每本非叶索引册最少记录2个下级索引册的位置）
  // - 3：每个页面最多能存储的键值对数量（类比：每本索引册最多能记录3个图书编号和对应的位置信息）
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);

  // 5. 准备要插入的键（key）和记录标识符（RID）
  // 类比：准备一本具体图书的索引信息，用于插入到索引系统中
  GenericKey<8> index_key;  // 索引键的变量，好比"图书编号"的存储容器
  RID rid;                  // 记录标识符，好比"图书在仓库中的具体位置"（哪个货架+哪个位置）

  int64_t key = 42;               // 具体的键值：图书编号为42
  int64_t value = key & 0xFFFFFFFF; // 计算RID的部分值（这里是取键的低32位，仅为示例计算方式）
  rid.Set(static_cast<int32_t>(key), value); // 构造RID：好比将"图书编号42"和"计算出的位置值"组合，确定图书在仓库的具体位置
  index_key.SetFromInteger(key);  // 将整数42转换成B+树能识别的键格式，好比将"42"这个数字按"图书编号规则"（key_schema）格式化

  // 6. 向B+树插入键值对
  // 类比：将"图书编号42"和对应的"仓库位置（RID）"插入到"图书馆索引系统"中
  tree.Insert(index_key, rid);

  // 7. 验证插入结果：获取B+树的根页面并检查
  // 类比：从索引系统中找到第一本索引册（根页面），检查插入是否成功

  // 7.1 获取根页面的编号，好比查到"系统第一本索引册"的编号
  auto root_page_id = tree.GetRootPageId();
  // 7.2 让"前台管理员"读取根页面到临时货架，并用"页面守卫"（root_page_guard）管理（避免被意外修改或删除）
  auto root_page_guard = bpm->ReadPage(root_page_id);
  // 7.3 将读取到的页面转换成B+树页面格式，好比将"空白索引册"转换成"能识别索引内容的格式"
  auto root_page = root_page_guard.As<BPlusTreePage>();

  // 断言1：根页面不为空，好比确认"系统第一本索引册"确实存在（没丢失）
  ASSERT_NE(root_page, nullptr);
  // 断言2：根页面是叶节点，好比确认"系统第一本索引册"是直接记录图书信息的册子（而非记录下级索引册位置的册子）
  // 原因：刚插入1个数据，根节点无需分裂，直接作为叶节点存储数据
  ASSERT_TRUE(root_page->IsLeafPage());

  // 7.4 将根页面进一步转换成叶节点页面格式，好比将"通用索引册"转换成"专门记录图书信息的叶册"
  auto root_as_leaf = root_page_guard.As<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>>();

  // 断言3：叶节点中的数据量为1，好比确认"第一本索引册"里只记录了1条图书信息（就是我们刚插入的那条）
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  // 断言4：叶节点中第0个位置的键，与我们插入的键相等（比较结果为0表示相等）
  // 类比：确认"第一本索引册"里第1条记录的图书编号，就是我们插入的42
  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);

  // 8. 释放缓冲池管理器的内存，好比"关闭图书馆前台"，回收管理员占用的资源（避免内存泄漏）
  delete bpm;
}

TEST(BPlusTreeTests, DISABLED_OptimisticInsertTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 4, 3);
  GenericKey<8> index_key;
  RID rid;

  size_t num_keys = 25;
  for (size_t i = 0; i < num_keys; i++) {
    int64_t value = i & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(i >> 32), value);
    index_key.SetFromInteger(2 * i);
    tree.Insert(index_key, rid);
  }

  size_t to_insert = num_keys + 1;
  auto leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>>(tree.GetRootPageId(), bpm);
  while (leaf.Valid()) {
    if (((*leaf)->GetSize() + 1) < (*leaf)->GetMaxSize()) {
      to_insert = (*leaf)->KeyAt(0).GetAsInteger() + 1;
    }
    ++leaf;
  }
  EXPECT_NE(to_insert, num_keys + 1);

  auto base_reads = tree.bpm_->GetReads();
  auto base_writes = tree.bpm_->GetWrites();

  index_key.SetFromInteger(to_insert);
  int64_t value = to_insert & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(to_insert >> 32), value);
  tree.Insert(index_key, rid);

  auto new_reads = tree.bpm_->GetReads();
  auto new_writes = tree.bpm_->GetWrites();

  EXPECT_GT(new_reads - base_reads, 0);
  EXPECT_EQ(new_writes - base_writes, 1);

  delete bpm;
}

TEST(BPlusTreeTests, DISABLED_InsertTest1NoIterator) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }

  bool is_present;
  std::vector<RID> rids;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  delete bpm;
}

TEST(BPlusTreeTests, DISABLED_InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    auto pair = *iter;
    auto location = pair.second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }
  delete bpm;
}
}  // namespace bustub
