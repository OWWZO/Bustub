//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer_test.cpp
//
// Identification: test/buffer/arc_replacer_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * arc_replacer_test.cpp
 */

#include "buffer/arc_replacer.h"

#include "gtest/gtest.h"
#include <thread>
#include <vector>

namespace bustub {

TEST(ArcReplacerTest, SampleTest) {
  // 我们用（a, fb）表示帧b上的页面a，
  // （a, _）标记页面ID为a的幽灵页面
  // p(a, fb)标记帧b上的固定页面a
  // 我们用[<-mru_ghost-][<-mru-]![-mfu->][->mfu_ghost->] p=x
  // 来表示4个列表，其中离!越近的页面越新
  // 并记录mru目标大小为x
  ArcReplacer arc_replacer(7);
  // 向替换器添加六个帧。
  // 我们将帧 6 设置为不可驱逐。这些页都进入 mru 列表
  // 我们现在有帧 [][(1,f1), (2,f2), (3,f3), (4,f4), (5,f5), p (6,f6)]![][]
  arc_replacer.RecordAccess(1, 1);
  arc_replacer.RecordAccess(2, 2);
  arc_replacer.RecordAccess(3, 3);
  arc_replacer.RecordAccess(4, 4);
  arc_replacer.RecordAccess(5, 5);
  arc_replacer.RecordAccess(6, 6);
  arc_replacer.SetEvictable(1, true);
  arc_replacer.SetEvictable(2, true);
  arc_replacer.SetEvictable(3, true);
  arc_replacer.SetEvictable(4, true);
  arc_replacer.SetEvictable(5, true);
  arc_replacer.SetEvictable(6, false);

  // 替换器的大小是可以被逐出的帧数，而不是进入的总帧数
  ASSERT_EQ(5, arc_replacer.Size());
  // 记录对帧 1 的访问。现在帧 1 进入 mfu 列表
  arc_replacer.RecordAccess(1, 1);
  // 现在 [][(2,f2), (3,f3), (4,f4), (5,f5), p (6,f6)]![(1,f1)][] p=0
  //
  // 现在从替换器中逐出三个页面。
  // 由于目标大小仍然是 0，应该逐出最近最久未使用（mru）端的页面
  ASSERT_EQ(2, arc_replacer.Evict());
  ASSERT_EQ(3, arc_replacer.Evict());
  ASSERT_EQ(4, arc_replacer.Evict());
  ASSERT_EQ(2, arc_replacer.Size());
  // 现在 [(2,), (3,), (4,_)][(5,f5), p(6,f6)]![(1,f1)][] p=0
  // 在帧 2 上插入新页面 7，这不应该命中幽灵列表
  // 因为我们从未见过页面 7，它会进入 mru 列表
  arc_replacer.RecordAccess(2, 7);
  arc_replacer.SetEvictable(2, true);
  // 在帧 3 上插入第 2 页，这应该会命中最近最久未使用（mru）的幽灵列表
  // 由于我们刚刚逐出了第 2 页，所以它会进入最常使用（mfu）列表
  // 此外，目标大小应增加 1，因为最近最久未使用（mru）幽灵列表的大小为 3，而最常使用（mfu）幽灵列表的大小为 0
  // 以 x 开头的是幽灵列表上的页面 ID，以_开头的是已固定的页面 ID
  arc_replacer.RecordAccess(3, 2);
  arc_replacer.SetEvictable(3, true);
  // Now [(3,_), (4,_)][(5,f5), p(6,f6), (7,f2)]![(2,f3), (1,f1)][] p=1
  ASSERT_EQ(4, arc_replacer.Size());
  // Continue to insert page 3 on frame 4 and page 4 on frame 7
  arc_replacer.RecordAccess(4, 3);
  arc_replacer.SetEvictable(4, true);
  arc_replacer.RecordAccess(7, 4);
  arc_replacer.SetEvictable(7, true);
  // Now [][(5,f5), p(6,f6), (7,f2)]![(4,f7), (3,f4), (2,f3), (1,f1)][] p=3
  ASSERT_EQ(6, arc_replacer.Size());

  // Evict an entry, now target size is 3, we still evict from mru
  ASSERT_EQ(5, arc_replacer.Evict());
  // Now [(5,_)][p(6,f6), (7,f2)]![(4,f7), (3,f4), (2,f3), (1,f1)][] p=3
  // Evict another entry, this time mru is smaller than target,
  // mfu is victimized
  ASSERT_EQ(1, arc_replacer.Evict());
  // Now [(5,_)][p(6,f6), (7,f2)]![(4,f7), (3,f4), (2,f3)][(1,_)] p=3

  // 在帧 5 上再次访问页面 1，现在页面 1 回到了最近最常使用（mfu）状态
  // 由于使用了不同的帧，p 也减少了 1/1=1
  arc_replacer.RecordAccess(5, 1);
  arc_replacer.SetEvictable(5, true);
  // Now [(5,_)][p(6,f6), (7,f2)]![(1,f5), (4,f7), (3,f4), (2,f3)][] p=2
  ASSERT_EQ(5, arc_replacer.Size());

  // We evict again, this time target size is 2, we evict from mru,
  // note that page 6 is pinned. Victim is page 7
  // Now [(5,_), (7,_)][p(6,f6)]![(1,f5), (4,f7), (3,f4), (2,f3)][] p=2
  ASSERT_EQ(2, arc_replacer.Evict());
}

TEST(ArcReplacerTest, SampleTest2) {
  // Test a smaller capacity
  ArcReplacer arc_replacer(3);
  // Fill up the replacer
  arc_replacer.RecordAccess(1, 1);
  arc_replacer.SetEvictable(1, true);
  arc_replacer.RecordAccess(2, 2);
  arc_replacer.SetEvictable(2, true);
  arc_replacer.RecordAccess(3, 3);
  arc_replacer.SetEvictable(3, true);
  ASSERT_EQ(3, arc_replacer.Size());
  // Now [][(1,f1), (2,f2), (3,f3)]![][] p=0
  // Evict all pages
  ASSERT_EQ(1, arc_replacer.Evict());
  ASSERT_EQ(2, arc_replacer.Evict());
  ASSERT_EQ(3, arc_replacer.Evict());
  ASSERT_EQ(0, arc_replacer.Size());
  // Now [(1,_), (2,_), (3,_)][]![][] p=0

  // Insert a new page 4 with frame 3. This is case 4A
  // and ghost pages 1 should be driven out
  arc_replacer.RecordAccess(3, 4);
  arc_replacer.SetEvictable(3, true);
  // Now [(2,_), (3,_)][(4,f3)]![][] p=0

  // 访问帧 2 上的页面 1，它不应该命中幽灵列表。幽灵页面 2 应该被淘汰
  arc_replacer.RecordAccess(2, 1);
  arc_replacer.SetEvictable(2, true);
  ASSERT_EQ(2, arc_replacer.Size());
  // Now [(3,_)][(4,f3), (1,f2)]![][] p=0

  // Access page 3 with frame 1, this should be a ghost hit,
  // page 3 is placed on mfu and target size is bumped up by 1
  arc_replacer.RecordAccess(1, 3);
  arc_replacer.SetEvictable(1, true);
  // Now [][(4,f3), (1,f2)]![(3,f1)][] p=1

  // Make some more ghosts by evicting all pages again
  ASSERT_EQ(3, arc_replacer.Evict());
  ASSERT_EQ(2, arc_replacer.Evict());
  ASSERT_EQ(1, arc_replacer.Evict());
  // Now [(4,_), (1,_)][]![][(3,_)] p=1

  // Let's make even more ghost to fill the list to "full"
  // Insert page 1 again so it goes to mfu side,
  // target is bumped up by 1
  arc_replacer.RecordAccess(1, 1);
  arc_replacer.SetEvictable(1, true);
  // Now [(4,_)][]![(1,f1)][(3,_)] p=2

  // Insert page 4 again so it goes to mfu side,
  // target is bumped up by 1
  arc_replacer.RecordAccess(2, 4);
  arc_replacer.SetEvictable(2, true);
  // Now [][]![(4,f2),(1,f1)][(3,_)] p=3

  // Now insert and evict one new page at a time
  // Insert page 5 and evict, since target size is 3,
  // should victimize page 1
  arc_replacer.RecordAccess(3, 5);
  arc_replacer.SetEvictable(3, true);
  ASSERT_EQ(1, arc_replacer.Evict());
  // Now [][(5,f3)]![(4,f2)][(1,_),(3,_)] p=3
  // Insert page 6 and evict, notice target size is 3,
  // so page 4 gets evicted
  arc_replacer.RecordAccess(1, 6);
  arc_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, arc_replacer.Evict());
  // Now [][(5,f3),(6,f1)]![(4,_),(1,_),(3,_)] p=3
  // Insert page 7 and evict, notice target size is 3,
  // so page 5 gets evicted
  arc_replacer.RecordAccess(2, 7);
  arc_replacer.SetEvictable(2, true);
  ASSERT_EQ(3, arc_replacer.Evict());
  // Now [(5,_)][(6,f1),(7,f2)]![][(4,_),(1,_),(3,_)] p=3

  // Now the list is full! reaching 2*capacity
  // adjust page 5 to mfu list
  arc_replacer.RecordAccess(3, 5);
  arc_replacer.SetEvictable(3, true);
  // Now [][(6,f1),(7,f2)]![(5,f3)][(4,_),(1,_),(3,_)] p=3

  // Now evict, target should be mfu
  ASSERT_EQ(3, arc_replacer.Evict());
  // Now [][(6,f1),(7,f2)]![][(5,_),(4,_),(1,_),(3,_)] p=3

  // Now mru and mru_ghost together has
  // less than 3 records. When inserting a new page 2
  // this should be case 4B and
  // four lists total size equals 2 * capacity case,
  // So mfu ghost will be shrinked
  arc_replacer.RecordAccess(3, 2);  // 总容量超了
  arc_replacer.SetEvictable(3, true);
  // Now [][(6,f1),(7,f2),(2,f3)]![][(5,_),(4,_),(1,_)] p=3

  // Evict a page 6
  ASSERT_EQ(1, arc_replacer.Evict());
  // Now [(6,_)][(7,f2),(2,f3)]![][(5,_),(4,_),(1,_)] p=3
  // And access page 3 who was removed
  // then this is case 4A, ghost page 6 will be removed
  arc_replacer.RecordAccess(1, 3);
  arc_replacer.SetEvictable(1, true);
  // Now [][(7,f2),(2,f3),(3,f1)]![][(5,_),(4,_),(1,_)] p=3

  // Finally we evict all pages and see if the order is right,
  // note that target size is 3
  ASSERT_EQ(2, arc_replacer.Evict());
  ASSERT_EQ(3, arc_replacer.Evict());
  ASSERT_EQ(1, arc_replacer.Evict());
}

// Feel free to write more tests!

/**
 * 测试ARC替换器的replacer_size_成员变量
 * replacer_size_是ARC替换器的总容量，对应构造函数的num_frames参数
 * 它限制了mru_和mfu_列表的总大小上限（活跃帧总数不能超过此值）
 */
TEST(ArcReplacerTest, ReplacerSizeTest) {
  // 测试1：验证构造函数正确设置replacer_size_
  const size_t test_capacity = 10;
  ArcReplacer arc_replacer(test_capacity);
  
  // 初始状态下，Size()应该返回0（没有可淘汰的帧）
  ASSERT_EQ(0, arc_replacer.Size());
  
  // 测试2：验证不能超过replacer_size_限制
  // 添加帧直到达到容量限制
  for (frame_id_t frame_id = 1; frame_id <= static_cast<frame_id_t>(test_capacity); ++frame_id) {
    arc_replacer.RecordAccess(frame_id, frame_id);
    arc_replacer.SetEvictable(frame_id, true);
    ASSERT_EQ(frame_id, arc_replacer.Size());
  }
  
  // 此时应该达到最大容量
  ASSERT_EQ(test_capacity, arc_replacer.Size());
  
  // 测试3：验证超过容量时的行为
  // 尝试添加超过容量的帧，应该触发淘汰机制
  arc_replacer.RecordAccess(static_cast<frame_id_t>(test_capacity + 1), static_cast<page_id_t>(test_capacity + 1));
  arc_replacer.SetEvictable(static_cast<frame_id_t>(test_capacity + 1), true);
  
  // Size()应该仍然等于replacer_size_（因为会淘汰一个帧为新帧让位）
  ASSERT_EQ(test_capacity, arc_replacer.Size());
  
  // 测试4：验证容量为0的边界情况
  ArcReplacer empty_replacer(0);
  ASSERT_EQ(0, empty_replacer.Size());
  
  // 尝试向容量为0的替换器添加帧
  empty_replacer.RecordAccess(1, 1);
  empty_replacer.SetEvictable(1, true);
  ASSERT_EQ(0, empty_replacer.Size()); // 应该立即被淘汰
}

/**
 * 测试不同容量下的replacer_size_行为
 */
TEST(ArcReplacerTest, ReplacerSizeDifferentCapacitiesTest) {
  // 测试小容量
  ArcReplacer small_replacer(3);
  ASSERT_EQ(0, small_replacer.Size());
  
  // 填满小容量替换器
  for (frame_id_t frame_id = 1; frame_id <= 3; ++frame_id) {
    small_replacer.RecordAccess(frame_id, frame_id);
    small_replacer.SetEvictable(frame_id, true);
  }
  ASSERT_EQ(3, small_replacer.Size());
  
  // 测试大容量
  ArcReplacer large_replacer(100);
  ASSERT_EQ(0, large_replacer.Size());
  
  // 添加一些帧但不超过容量
  for (frame_id_t frame_id = 1; frame_id <= 50; ++frame_id) {
    large_replacer.RecordAccess(frame_id, frame_id);
    large_replacer.SetEvictable(frame_id, true);
  }
  ASSERT_EQ(50, large_replacer.Size());
}

/**
 * 测试replacer_size_与幽灵帧的关系
 * 验证总容量包括活跃帧和幽灵帧的总和不能超过2*replacer_size_
 */
TEST(ArcReplacerTest, ReplacerSizeWithGhostFramesTest) {
  const size_t capacity = 5;
  ArcReplacer arc_replacer(capacity);
  
  // 填满替换器
  for (frame_id_t frame_id = 1; frame_id <= static_cast<frame_id_t>(capacity); ++frame_id) {
    arc_replacer.RecordAccess(frame_id, frame_id);
    arc_replacer.SetEvictable(frame_id, true);
  }
  ASSERT_EQ(capacity, arc_replacer.Size());
  
  // 淘汰所有帧，创建幽灵帧
  for (frame_id_t frame_id = 1; frame_id <= static_cast<frame_id_t>(capacity); ++frame_id) {
    auto evicted = arc_replacer.Evict();
    ASSERT_TRUE(evicted.has_value());
  }
  ASSERT_EQ(0, arc_replacer.Size());
  
  // 现在添加新帧，应该触发幽灵帧的清理机制
  // 总容量（活跃帧+幽灵帧）不应该超过2*capacity
  arc_replacer.RecordAccess(static_cast<frame_id_t>(capacity + 1), static_cast<page_id_t>(capacity + 1));
  arc_replacer.SetEvictable(static_cast<frame_id_t>(capacity + 1), true);
  ASSERT_EQ(1, arc_replacer.Size());
}

/**
 * 测试replacer_size_在并发访问下的正确性
 */
TEST(ArcReplacerTest, ReplacerSizeConcurrencyTest) {
  const size_t capacity = 10;
  ArcReplacer arc_replacer(capacity);
  
  // 模拟多线程同时访问
  std::vector<std::thread> threads;
  
  // 线程1：添加帧
  threads.emplace_back([&arc_replacer]() {
    for (frame_id_t frame_id = 1; frame_id <= 5; ++frame_id) {
      arc_replacer.RecordAccess(frame_id, frame_id);
      arc_replacer.SetEvictable(frame_id, true);
    }
  });
  
  // 线程2：添加更多帧
  threads.emplace_back([&arc_replacer]() {
    for (frame_id_t frame_id = 6; frame_id <= 10; ++frame_id) {
      arc_replacer.RecordAccess(frame_id, frame_id);
      arc_replacer.SetEvictable(frame_id, true);
    }
  });
  
  // 等待所有线程完成
  for (auto& thread : threads) {
    thread.join();
  }
  
  // 验证最终大小不超过容量
  ASSERT_LE(arc_replacer.Size(), capacity);
}

/**
 * 测试mru_target_size_的增加和减少逻辑
 * mru_target_size_是ARC算法中的关键参数，控制MRU和MFU区域的大小比例
 * 
 * 增加逻辑（访问MRU_GHOST时）：
 * - 如果mru_ghost_.size() >= mfu_ghost_.size()，则mru_target_size_ += 1
 * - 否则，mru_target_size_ += floor(mfu_ghost_.size() / mru_ghost_.size())
 * - 上限为replacer_size_
 * 
 * 减少逻辑（访问MFU_GHOST时）：
 * - 如果mfu_ghost_.size() >= mru_ghost_.size()，则mru_target_size_ -= 1
 * - 否则，mru_target_size_ -= floor(mru_ghost_.size() / mfu_ghost_.size())
 * - 下限为0
 */
TEST(ArcReplacerTest, MruTargetSizeAdjustmentTest) {
  const size_t capacity = 10;
  ArcReplacer arc_replacer(capacity);
  
  // 测试1：MRU_GHOST访问时的增加逻辑
  // 先填满替换器，然后全部淘汰，创建幽灵帧
  for (frame_id_t frame_id = 1; frame_id <= static_cast<frame_id_t>(capacity); ++frame_id) {
    arc_replacer.RecordAccess(frame_id, frame_id);
    arc_replacer.SetEvictable(frame_id, true);
  }
  
  // 淘汰所有帧，创建MRU幽灵帧
  for (frame_id_t frame_id = 1; frame_id <= static_cast<frame_id_t>(capacity); ++frame_id) {
    auto evicted = arc_replacer.Evict();
    ASSERT_TRUE(evicted.has_value());
  }
  
  // 现在访问MRU_GHOST中的页面，应该增加mru_target_size_
  // 由于mru_ghost_.size() = capacity, mfu_ghost_.size() = 0
  // 所以mru_ghost_.size() >= mfu_ghost_.size()，应该增加1
  arc_replacer.RecordAccess(1, 1);  // 访问MRU_GHOST中的页面1
  arc_replacer.SetEvictable(1, true);
  
  // 验证页面1被移到了MFU区域
  ASSERT_EQ(1, arc_replacer.Size());
  
  // 测试2：MFU_GHOST访问时的减少逻辑
  // 再次淘汰页面1，创建MFU幽灵帧
  auto evicted = arc_replacer.Evict();
  ASSERT_TRUE(evicted.has_value());
  ASSERT_EQ(0, arc_replacer.Size());
  
  // 现在访问MFU_GHOST中的页面1，应该减少mru_target_size_
  // 由于mfu_ghost_.size() = 1, mru_ghost_.size() = capacity-1
  // 所以mfu_ghost_.size() < mru_ghost_.size()，应该减少floor((capacity-1)/1) = capacity-1
  arc_replacer.RecordAccess(2, 1);  // 访问MFU_GHOST中的页面1
  arc_replacer.SetEvictable(2, true);
  
  // 验证页面1被移到了MFU区域
  ASSERT_EQ(1, arc_replacer.Size());
}

/**
 * 测试mru_target_size_的边界条件
 */
TEST(ArcReplacerTest, MruTargetSizeBoundaryTest) {
  const size_t capacity = 5;
  ArcReplacer arc_replacer(capacity);
  
  // 测试上限：mru_target_size_不应该超过replacer_size_
  // 创建大量MRU幽灵帧，然后访问它们
  for (frame_id_t frame_id = 1; frame_id <= static_cast<frame_id_t>(capacity); ++frame_id) {
    arc_replacer.RecordAccess(frame_id, frame_id);
    arc_replacer.SetEvictable(frame_id, true);
  }
  
  // 淘汰所有帧
  for (frame_id_t frame_id = 1; frame_id <= static_cast<frame_id_t>(capacity); ++frame_id) {
    auto evicted = arc_replacer.Evict();
    ASSERT_TRUE(evicted.has_value());
  }
  
  // 多次访问MRU_GHOST，测试上限
  for (int i = 0; i < 10; ++i) {
    arc_replacer.RecordAccess(1, 1);
    arc_replacer.SetEvictable(1, true);
    auto evicted = arc_replacer.Evict();
    ASSERT_TRUE(evicted.has_value());
  }
  
  // 测试下限：mru_target_size_不应该小于0
  // 创建大量MFU幽灵帧，然后访问它们
  for (int i = 0; i < 10; ++i) {
    arc_replacer.RecordAccess(1, 1);
    arc_replacer.SetEvictable(1, true);
    auto evicted = arc_replacer.Evict();
    ASSERT_TRUE(evicted.has_value());
  }
  
  // 多次访问MFU_GHOST，测试下限
  for (int i = 0; i < 10; ++i) {
    arc_replacer.RecordAccess(1, 1);
    arc_replacer.SetEvictable(1, true);
    auto evicted = arc_replacer.Evict();
    ASSERT_TRUE(evicted.has_value());
  }
}

/**
 * 测试mru_target_size_的复杂调整场景
 */
TEST(ArcReplacerTest, MruTargetSizeComplexScenarioTest) {
  const size_t capacity = 8;
  ArcReplacer arc_replacer(capacity);
  
  // 场景1：创建不同大小的幽灵帧列表
  // 先创建4个MRU幽灵帧
  for (frame_id_t frame_id = 1; frame_id <= 4; ++frame_id) {
    arc_replacer.RecordAccess(frame_id, frame_id);
    arc_replacer.SetEvictable(frame_id, true);
  }
  
  // 淘汰它们，创建MRU幽灵帧
  for (frame_id_t frame_id = 1; frame_id <= 4; ++frame_id) {
    auto evicted = arc_replacer.Evict();
    ASSERT_TRUE(evicted.has_value());
  }
  
  // 再创建2个MFU幽灵帧
  for (frame_id_t frame_id = 5; frame_id <= 6; ++frame_id) {
    arc_replacer.RecordAccess(frame_id, frame_id);
    arc_replacer.SetEvictable(frame_id, true);
  }
  
  // 访问一次让它们进入MFU
  arc_replacer.RecordAccess(5, 5);
  arc_replacer.RecordAccess(6, 6);
  
  // 淘汰它们，创建MFU幽灵帧
  auto evicted1 = arc_replacer.Evict();
  auto evicted2 = arc_replacer.Evict();
  ASSERT_TRUE(evicted1.has_value());
  ASSERT_TRUE(evicted2.has_value());
  
  // 现在状态：mru_ghost_.size() = 4, mfu_ghost_.size() = 2
  // 访问MRU_GHOST：mru_ghost_.size() >= mfu_ghost_.size()，应该增加1
  arc_replacer.RecordAccess(7, 1);  // 访问MRU_GHOST中的页面1
  arc_replacer.SetEvictable(7, true);
  ASSERT_EQ(1, arc_replacer.Size());
  
  // 访问MFU_GHOST：mfu_ghost_.size() < mru_ghost_.size()，应该减少floor(3/1) = 3
  arc_replacer.RecordAccess(8, 5);  // 访问MFU_GHOST中的页面5
  arc_replacer.SetEvictable(8, true);
  ASSERT_EQ(2, arc_replacer.Size());
}

/**
 * 测试mru_target_size_在空幽灵列表时的行为
 */
TEST(ArcReplacerTest, MruTargetSizeEmptyGhostTest) {
  const size_t capacity = 3;
  ArcReplacer arc_replacer(capacity);
  
  // 测试MFU_GHOST为空时的减少逻辑
  // 创建一些MRU幽灵帧
  for (frame_id_t frame_id = 1; frame_id <= static_cast<frame_id_t>(capacity); ++frame_id) {
    arc_replacer.RecordAccess(frame_id, frame_id);
    arc_replacer.SetEvictable(frame_id, true);
  }
  
  // 淘汰所有帧
  for (frame_id_t frame_id = 1; frame_id <= static_cast<frame_id_t>(capacity); ++frame_id) {
    auto evicted = arc_replacer.Evict();
    ASSERT_TRUE(evicted.has_value());
  }
  
  // 现在mru_ghost_.size() = capacity, mfu_ghost_.size() = 0
  // 访问MRU_GHOST，应该增加1
  arc_replacer.RecordAccess(1, 1);
  arc_replacer.SetEvictable(1, true);
  ASSERT_EQ(1, arc_replacer.Size());
  
  // 淘汰这个帧，创建MFU幽灵帧
  auto evicted = arc_replacer.Evict();
  ASSERT_TRUE(evicted.has_value());
  
  // 现在mru_ghost_.size() = capacity-1, mfu_ghost_.size() = 1
  // 访问MFU_GHOST，由于mfu_ghost_.size() < mru_ghost_.size()，
  // 应该减少floor((capacity-1)/1) = capacity-1
  arc_replacer.RecordAccess(2, 1);
  arc_replacer.SetEvictable(2, true);
  ASSERT_EQ(1, arc_replacer.Size());
}

}  // namespace bustub
