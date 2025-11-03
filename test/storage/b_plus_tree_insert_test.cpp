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

// 单元测试：测试B+树的乐观插入功能
// 类比：这相当于在"图书馆书籍索引系统"中，测试一种高效的新书上架（插入）策略，
// 先检查是否有书架（叶子节点）有空闲位置，再插入以减少索引结构调整的开销
TEST(BPlusTreeTests, OptimisticInsertTest) {
  // 1. 定义索引键的结构：解析"a bigint"语句，创建一个8字节大小的bigint类型键 schema
  // 类比：制定"书籍索引卡"的格式——规定索引卡上只记录"书籍编号"（bigint类型，8字节），作为查找依据
  auto key_schema = ParseCreateStatement("a bigint");

  // 2. 创建比较器：用于比较两个8字节索引键的大小（决定键在B+树中的排序位置）
  // 类比：制作一把"尺子"，专门用来比较两张"书籍索引卡"上的编号大小，确定哪本排在前面
  GenericComparator<8> comparator(key_schema.get());

  // 3. 创建内存磁盘管理器：模拟磁盘存储，但实际数据都存在内存中（无需真实读写硬盘）
  // 类比：搭建一个"临时仓库"，所有书籍和索引卡都暂时存这里，不用搬去真实仓库（硬盘），操作更快
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();

  // 4. 创建缓冲池管理器：管理50个内存页，负责将"磁盘页"（实际是内存模拟的）加载到内存/写回
  // 类比：设置一个"工作台"，最多同时放50本"书"（内存页），需要操作某本书时从临时仓库搬到工作台，用完后放回
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // 5. 分配B+树的根页面：向缓冲池申请一个新的空白页面，作为B+树的"根节点"（最顶层索引节点）
  // 类比：在工作台上拿一张空白的"总索引页"，这张页是整个图书馆索引的起点（所有查找从这里开始）
  page_id_t page_id = bpm->NewPage();

  // 6. 创建B+树实例：
  // - 名称"foo_pk"（类似索引的标识）
  // - 根节点使用刚分配的page_id
  // - 依赖缓冲池bpm管理页面
  // - 使用前面创建的comparator比较键
  // - 非叶子节点最大能存4个键（对应5个子节点指针），叶子节点最大能存3个键（对应3个数据指针）
  // 类比：搭建完整的"图书馆索引系统"，规定：
  // - 总索引页（根节点）最多记4个编号范围（比如1-10,11-20等），每个范围指向更细的分索引页
  // - 最底层的分索引页（叶子节点）最多记3个书籍编号，每个编号直接指向书籍存放位置
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 4, 3);

  // 7. 定义临时变量：index_key用于存储要插入的索引键，rid用于存储数据的物理位置（类似指针）
  // 类比：准备一张空白的"书籍索引卡"（index_key）和一个"书籍位置标签"（rid），后续插入时填写
  GenericKey<8> index_key;
  RID rid;

  // 8. 批量插入25条数据：模拟向B+树中插入大量初始数据
  // 类比：向图书馆索引系统中，批量录入25本书的索引信息
  size_t num_keys = 25;
  for (size_t i = 0; i < num_keys; i++) {
    // 计算RID的数值：将循环变量i拆分为两部分（高32位和低32位），存入RID（表示数据在磁盘上的位置）
    // 类比：生成"书籍位置标签"——把书籍编号i拆成"仓库分区号"（i>>32）和"分区内货架号"（i&0xFFFFFFFF）
    int64_t value = i & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(i >> 32), value);

    // 设置索引键：将2*i作为索引键值（确保键是偶数，后续插入奇数键测试）
    // 类比：在"书籍索引卡"上填写书籍编号为2*i（比如i=0填0，i=1填2，…，i=24填48）
    index_key.SetFromInteger(2 * i);

    // 插入B+树：将索引键和对应的RID存入B+树
    // 类比：把填好的"索引卡"和"位置标签"一起加入图书馆索引系统，系统自动按编号排序存放
    tree.Insert(index_key, rid);
  }

  // 9. 乐观插入前的准备：找到一个能直接插入的叶子节点（避免节点分裂）
  // 类比：在插入新书前，先检查所有底层分索引页（叶子节点），找一张还没装满的（能直接加索引卡的）
  size_t to_insert = num_keys + 1; // 初始化要插入的键值（默认值，后续会修改）

  // 创建叶子节点迭代器：从B+树的根节点出发，遍历所有叶子节点
  // 类比：拿一个"叶子页查找器"，从总索引页开始，依次找到所有底层的分索引页
  auto leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>>(tree.GetRootPageId(), bpm);

  // 遍历所有叶子节点
  while (leaf.Valid()) {
    // 检查当前叶子节点：如果插入1个键后仍未达到最大容量（3），说明能直接插入
    // 类比：看当前分索引页是否还有空位——如果当前有2张索引卡，插入1张后变成3张（没超上限），就选这页
    if (((*leaf)->GetSize() + 1) < (*leaf)->GetMaxSize()) {
      // 确定要插入的键值：取当前叶子节点的第一个键，加1（确保是未存在的奇数键，比如第一个键是0，加1得1）
      // 类比：新书编号设为当前分索引页最小编号加1（比如当前页最小是0，新书编号就是1，保证插入后仍有序）
      to_insert = (*leaf)->KeyAt(0).GetAsInteger() + 1;
    }
    ++leaf; // 迭代器移动到下一个叶子节点（检查下一张分索引页）
  }

  // 断言：确保找到了可插入的叶子节点（to_insert已被修改，不是初始值）
  // 类比：确认一定找到了有空位的分索引页（不会出现所有页都满的情况）
  EXPECT_NE(to_insert, num_keys + 1);

  // 10. 记录插入前的IO统计：获取缓冲池的读次数和写次数（用于后续对比）
  // 类比：记录插入新书前，从临时仓库搬书到工作台的次数（读）和放回仓库的次数（写）
  auto base_reads = tree.bpm_->GetReads();
  auto base_writes = tree.bpm_->GetWrites();

  // 11. 准备要插入的数据：设置索引键和对应的RID
  // 类比：填写新书的索引卡和位置标签
  index_key.SetFromInteger(to_insert); // 索引键设为前面确定的to_insert（比如1）
  int64_t value = to_insert & 0xFFFFFFFF; // 拆分to_insert为RID的低32位
  rid.Set(static_cast<int32_t>(to_insert >> 32), value); // 拆分to_insert为RID的高32位（位置标签）

  // 执行乐观插入：将数据插入B+树
  // 类比：把新书的索引卡插入到之前找到的分索引页（有空位的页），并记录位置标签
  tree.Insert(index_key, rid);

  // 12. 记录插入后的IO统计：获取插入后的读次数和写次数
  // 类比：记录插入新书后，总的搬书次数（读）和放回次数（写）
  auto new_reads = tree.bpm_->GetReads();
  auto new_writes = tree.bpm_->GetWrites();

  // 13. 断言插入的IO开销符合预期：
  // - 读次数增加（需要读取叶子节点到内存）
  // - 写次数只增加1（只需将修改后的叶子节点写回，无需分裂节点）
  // 类比：
  // - 搬书次数增加（必须把要插入的分索引页搬到工作台，所以读次数加1）
  // - 放回次数只加1（修改后的分索引页放回仓库，无需处理其他页，所以写次数加1）
  EXPECT_GT(new_reads - base_reads, 0);
  EXPECT_EQ(new_writes - base_writes, 1);

  // 14. 释放缓冲池管理器内存：避免内存泄漏
  // 类比：用完工作台后，清理工作台，释放占用的空间
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
