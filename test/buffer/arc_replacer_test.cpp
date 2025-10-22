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

}  // namespace bustub
