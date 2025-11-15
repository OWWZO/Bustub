
#include <algorithm>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_utils.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/generic_key.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;
//TODO draw偏移量的改变是否和声明位置相关？
TEST(BPlusTreeTests, TombstoneBasicTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManagerUnlimitedMemory();
  auto *bpm = new BufferPoolManager(50, disk_manager);

  // create and fetch header_page
  page_id_t page_id = bpm->NewPage();

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2> tree("foo_pk", page_id, bpm, comparator, 4, 4);
  GenericKey<8> index_key;
  RID rid;

  size_t num_keys = 17;
  std::vector<size_t> expected;
  for (size_t i = 0; i < num_keys; i++) {
    int64_t value = i & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(i >> 32), value);
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid);
    tree.Draw(bpm, "b_plus_tree.dot");
    expected.push_back(i);
  }

  // Test tombstones are being used / affect the index iterator correctly
  tree.Draw(bpm, "b_plus_tree.dot");
  std::vector<int64_t> to_delete = {1, 5, 9};
  for (auto i : to_delete) {
    index_key.SetFromInteger(i);
    tree.Remove(index_key);
    tree.Draw(bpm, "b_plus_tree.dot");
    expected.erase(std::remove(expected.begin(), expected.end(), i), expected.end());
  }

  tree.Draw(bpm, "b_plus_tree.dot");
  size_t i = 0;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    ASSERT_EQ((*iter).first.GetAsInteger(), expected[i]);
    i++;
  }

  std::vector<int64_t> tombstones;
  auto leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>, 2>(tree.GetRootPageId(), bpm);
  while (leaf.Valid()) {
    for (auto t : (*leaf)->GetTombstones()) {
      tombstones.push_back(t.GetAsInteger());
    }
    ++leaf;
  }

  /*. 墓碑（Tombstone）相关问题
处理时机与空间获取：
Raunaq 提问：墓碑何时处理？插入时是否通过num_tombstones_和size_计算当前大小以获取插入空间？是否后台持续处理墓碑？
kirillk 依据文档回复：按规范，墓碑处理应在删除操作时进行，仅当叶子页的删除缓冲区（墓碑）存满k条记录时，才将最早的缓冲删除应用到键 / 值数组。
墓碑索引调整：
kirillk 疑问：若墓碑存储键索引（如[3,5,7,9]），删除低索引项（如索引 3）后，墓碑应变为[5,7,9]还是调整为[4,6,8]？
其认为需调整：因删除靠前键会导致后续值移位，索引需同步更新。
墓碑与分裂 / 插入规则（Miketud04 观点）：
分裂判断：基于逻辑大小（GetSize() - tombstone.size()），若逻辑大小≥MaxSize()则分裂，否则即使索引超MaxSize()仍直接插入。
墓碑满时处理：仅处理最早的墓碑，更新键 / 值数组，并将剩余墓碑中大于 “已处理索引” 的项减 1。
分裂时墓碑处理：按逻辑大小确定分裂点，同步拆分墓碑数组；合并逻辑较直观。*/
  EXPECT_EQ(tombstones.size(), to_delete.size());
  for (size_t size = 0; size < tombstones.size(); size++) {
    EXPECT_EQ(tombstones[size], to_delete[size]);
  }

  // Test insertions interact correctly with tombstones

  for (auto key : to_delete) {
    int64_t value = (2 * key) & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }
  tree.Draw(bpm, "b_plus_tree.dot");
  leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>, 2>(tree.GetRootPageId(), bpm);
  while (leaf.Valid()) {
    EXPECT_EQ((*leaf)->GetTombstones().size(), 0);
    ++leaf;
  }

  for (auto key : to_delete) {
    index_key.SetFromInteger(key);
    std::vector<RID> rids;
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetSlotNum(), (2 * key) & 0xFFFFFFFF);
  }

  // Test tombstones are processed in the correct order

  to_delete.clear();
  {
    auto leaf_page = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>, 2>(tree.GetRootPageId(), bpm);
    while (leaf_page.Valid()) {
      EXPECT_EQ(2, (*leaf_page)->GetMinSize());
      if ((*leaf_page)->GetSize() > (*leaf_page)->GetMinSize()) {
        for (int index = 0; index < (*leaf_page)->GetMinSize() + 1; index++) {
          to_delete.push_back((*leaf_page)->KeyAt(index).GetAsInteger());
        }
        break;
      }
      ++leaf_page;
    }
  }
  tree.Draw(bpm, "b_plus_tree.dot");
  for (auto key : to_delete) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key);
    tree.Draw(bpm, "b_plus_tree.dot");
  }
  tree.Draw(bpm, "b_plus_tree.dot");
  tombstones.clear();
  leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>, 2>(tree.GetRootPageId(), bpm);
  while (leaf.Valid()) {
    for (auto t : (*leaf)->GetTombstones()) {
      tombstones.push_back(t.GetAsInteger());
    }
    ++leaf;
  }
  EXPECT_EQ(tombstones.size(), to_delete.size() - 1);
  for (size_t size = 0; size < tombstones.size(); size++) {
    EXPECT_EQ(tombstones[size], to_delete[size + 1]);
  }

  std::vector<RID> rids;
  index_key.SetFromInteger(to_delete[0]);
  tree.GetValue(index_key, &rids);
  EXPECT_EQ(rids.size(), 0);
  tree.Draw(bpm, "b_plus_tree.dot");
  // 测试索引迭代器在 “空” 树中保持有效（且树未被完全物理删除）
  for (size_t key = 0; key < num_keys; key++) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key);
    tree.Draw(bpm, "b_plus_tree.dot");
  }

  leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>, 2>(tree.GetRootPageId(), bpm);
  size_t tot_tombs = 0;
  while (leaf.Valid()) {
    tot_tombs += (*leaf)->GetTombstones().size();
    ++leaf;
  }

  // 最坏情况：所有键都在完整的叶节点中，因此每个叶节点中只有 2 个条目被标记删除。
  //第一个参数的值是否大于第二个参数的值
  EXPECT_GT(tot_tombs, ((num_keys - 1) / 4) * 2);
  EXPECT_LT(tot_tombs, num_keys);
  EXPECT_EQ(tree.Begin().IsEnd(), true);

  delete bpm;
  delete disk_manager;
}

//得出信息 标记墓碑删除之后 不减少size大小 但是当size大于maxsize之后就要分裂 同时墓碑也要分裂过去（TODO move优化） 然后将墓碑的值统一减去一个值
TEST(BPlusTreeTests, TombstoneSplitTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManagerUnlimitedMemory();
  auto *bpm = new BufferPoolManager(50, disk_manager);

  // create and fetch header_page
  page_id_t page_id = bpm->NewPage();

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3> tree("foo_pk", page_id, bpm, comparator, 5, 4);
  GenericKey<8> index_key;
  RID rid;

  for (size_t i = 0; i < 4; i++) {
    int64_t value = i & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(i >> 32), value);
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid);
  }
  index_key.SetFromInteger(3);
  tree.Remove(index_key);

  //打印不出墓碑 是draw的问题 实际调试我能看到
  tree.Draw(bpm, "b_plus_tree.dot");
  index_key.SetFromInteger(2);

  tree.Remove(index_key);

  index_key.SetFromInteger(0);
  tree.Remove(index_key);

  tree.Draw(bpm, "b_plus_tree.dot");
  size_t i = 4;
  while (GetNumLeaves(tree, bpm) < 2 && i < 6) {
    int64_t value = i & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(i >> 32), value);
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid);
    i++;
  }

  tree.Draw(bpm, "b_plus_tree.dot");
  auto leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>, 3>(tree.GetRootPageId(), bpm);
  while (leaf.Valid()) {
    std::vector<size_t> expected;
    for (int index = 0; index < (*leaf)->GetSize(); index++) {
      auto key = (*leaf)->KeyAt(index).GetAsInteger();
      if (key == 0 || key == 2 || key == 3) {
        expected.push_back(key);
      }
    }
    std::sort(expected.rbegin(), expected.rend());
    auto tombstones = (*leaf)->GetTombstones();
    EXPECT_EQ(tombstones.size(), expected.size());
    for (size_t size = 0; size < tombstones.size(); size++) {
      EXPECT_EQ(tombstones[size].GetAsInteger(), expected[size]);
    }
    ++leaf;
  }

  delete bpm;
  delete disk_manager;
}

TEST(BPlusTreeTests, TombstoneBorrowTest) {
  using LeafPage = BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManagerUnlimitedMemory();
  auto *bpm = new BufferPoolManager(50, disk_manager);

  // create and fetch header_page
  page_id_t page_id = bpm->NewPage();

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1> tree("foo_pk", page_id, bpm, comparator, 4, 4);
  GenericKey<8> index_key;
  RID rid;

  size_t num_keys = 5;
  for (size_t i = 0; i < num_keys; i++) {
    int64_t value = i & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(i >> 32), value);
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid);
  }

  auto left_pid = GetLeftMostLeafPageId<GenericKey<8>, RID, GenericComparator<8>>(tree.GetRootPageId(), bpm);
  auto left_page = bpm->ReadPage(left_pid).As<LeafPage>();
  EXPECT_NE(left_page->GetNextPageId(), INVALID_PAGE_ID);
  auto right_page = bpm->ReadPage(left_page->GetNextPageId()).As<LeafPage>();

  std::vector<GenericKey<8>> to_remove;
  if (left_page->GetSize() == left_page->GetMinSize()) {
    to_remove.push_back(right_page->KeyAt(0));
    to_remove.push_back(left_page->KeyAt(1));
    to_remove.push_back(left_page->KeyAt(0));
  } else {
    to_remove.push_back(left_page->KeyAt(0));
    to_remove.push_back(right_page->KeyAt(1));
    to_remove.push_back(right_page->KeyAt(0));
  }
  tree.Draw(bpm, "b_plus_tree.dot");
  for (auto k : to_remove) {
    tree.Remove(k);
    tree.Draw(bpm, "b_plus_tree.dot");
  }

  std::vector<int64_t> tombstones;
  auto leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>, 1>(tree.GetRootPageId(), bpm);
  while (leaf.Valid()) {
    EXPECT_GE((*leaf)->GetSize(), (*leaf)->GetMinSize());
    for (auto t : (*leaf)->GetTombstones()) {
      tombstones.push_back(t.GetAsInteger());
    }
    ++leaf;
  }

  EXPECT_EQ(tombstones.size(), 1);
  EXPECT_EQ(tombstones[0], to_remove[0].GetAsInteger());

  delete bpm;
  delete disk_manager;
}

TEST(BPlusTreeTests, TombstoneCoalesceTest) {
  using LeafPage = BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManagerUnlimitedMemory();
  auto *bpm = new BufferPoolManager(50, disk_manager);

  // create and fetch header_page
  page_id_t page_id = bpm->NewPage();

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2> tree("foo_pk", page_id, bpm, comparator, 6, 6);
  GenericKey<8> index_key;
  RID rid;

  size_t num_keys = 7;
  for (size_t i = 0; i < num_keys; i++) {
    int64_t value = i & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(i >> 32), value);
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid);
  }

  tree.Draw(bpm, "b_plus_tree.dot");
  page_id_t larger_pid;
  page_id_t smaller_pid;
  auto leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>, 2>(tree.GetRootPageId(), bpm);
  while (leaf.Valid()) {
    if ((*leaf)->GetSize() == 4) {
      larger_pid = leaf.guard_->GetPageId();
    } else {
      smaller_pid = leaf.guard_->GetPageId();
    }
    ++leaf;
  }

  std::vector<GenericKey<8>> to_delete;
  {
    auto larger_page = bpm->ReadPage(larger_pid).As<LeafPage>();
    auto smaller_page = bpm->ReadPage(smaller_pid).As<LeafPage>();
    for (size_t i = 0; i < 2; i++) {
      to_delete.push_back(larger_page->KeyAt(2 + i));
      to_delete.push_back(smaller_page->KeyAt(i));
    }
    to_delete.push_back(larger_page->KeyAt(0));
    to_delete.push_back(smaller_page->KeyAt(2));
  }
  //5 0 6 1 3 2
  tree.Draw(bpm, "b_plus_tree.dot");
  for (auto k : to_delete) {
    tree.Remove(k);
    tree.Draw(bpm, "b_plus_tree.dot");
  }

  size_t num_leaves = 0;
  page_id_t remaining_pid;
  leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>, 2>(tree.GetRootPageId(), bpm);
  while (leaf.Valid()) {
    remaining_pid = leaf.guard_->GetPageId();
    num_leaves++;
    ++leaf;
  }

  EXPECT_EQ(num_leaves, 1);
  auto page = bpm->ReadPage(remaining_pid).As<LeafPage>();
  auto tombstones = page->GetTombstones();
  EXPECT_EQ(tombstones.size(), 2);
  if (remaining_pid == smaller_pid) {
    EXPECT_EQ(tombstones[0].GetAsInteger(), to_delete[2].GetAsInteger());
    EXPECT_EQ(tombstones[1].GetAsInteger(), to_delete[4].GetAsInteger());
    index_key.SetFromInteger(7);
    int64_t value = 7 & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(7L >> 32), value);
    tree.Insert(index_key, rid);
    EXPECT_EQ(tombstones[0].GetAsInteger(), to_delete[2].GetAsInteger());
    EXPECT_EQ(tombstones[1].GetAsInteger(), to_delete[4].GetAsInteger());
  } else {
    EXPECT_EQ(tombstones[0].GetAsInteger(), to_delete[3].GetAsInteger());
    EXPECT_EQ(tombstones[1].GetAsInteger(), to_delete[5].GetAsInteger());
  }

  delete bpm;
  delete disk_manager;
}
}  // namespace bustub
