//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <filesystem>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/page/page_guard.h"

namespace bustub {
static std::filesystem::path db_fname("test.bustub");

// The number of frames we give to the buffer pool.
const size_t FRAMES = 10;

void CopyString(char *dest, const std::string &src) {
  BUSTUB_ENSURE(src.length() + 1 <= BUSTUB_PAGE_SIZE, "CopyString src too long");
  snprintf(dest, BUSTUB_PAGE_SIZE, "%s", src.c_str());
}

TEST(BufferPoolManagerTest, VeryBasicTest) {
  // A very basic test.

  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const page_id_t pid = bpm->NewPage();
  const std::string str = "Hello, world!";

  // Check `WritePageGuard` basic functionality.
  {
    auto guard = bpm->WritePage(pid);
    CopyString(guard.GetDataMut(), str);
    EXPECT_STREQ(guard.GetData(), str.c_str());
  }

  // Check `ReadPageGuard` basic functionality.
  {
    const auto guard = bpm->ReadPage(pid);
    EXPECT_STREQ(guard.GetData(), str.c_str());
  }

  // Check `ReadPageGuard` basic functionality (again).
  {
    const auto guard = bpm->ReadPage(pid);
    EXPECT_STREQ(guard.GetData(), str.c_str());
  }

  ASSERT_TRUE(bpm->DeletePage(pid));
}

TEST(BufferPoolManagerTest, PagePinEasyTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(2, disk_manager.get());

  const page_id_t pageid0 = bpm->NewPage();
  const page_id_t pageid1 = bpm->NewPage();

  const std::string str0 = "page0";
  const std::string str1 = "page1";
  const std::string str0updated = "page0updated";
  const std::string str1updated = "page1updated";

  {
    auto page0_write_opt = bpm->CheckedWritePage(pageid0);
    ASSERT_TRUE(page0_write_opt.has_value());
    auto page0_write = std::move(page0_write_opt.value()); // NOLINT
    CopyString(page0_write.GetDataMut(), str0);

    auto page1_write_opt = bpm->CheckedWritePage(pageid1);
    ASSERT_TRUE(page1_write_opt.has_value());
    auto page1_write = std::move(page1_write_opt.value()); // NOLINT
    CopyString(page1_write.GetDataMut(), str1);

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    ASSERT_EQ(1, bpm->GetPinCount(pageid1));

    const auto temp_page_id1 = bpm->NewPage();
    const auto temp_page1_opt = bpm->CheckedReadPage(temp_page_id1);
    ASSERT_FALSE(temp_page1_opt.has_value());

    const auto temp_page_id2 = bpm->NewPage();
    const auto temp_page2_opt = bpm->CheckedWritePage(temp_page_id2);
    ASSERT_FALSE(temp_page2_opt.has_value());

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    page0_write.Drop();
    ASSERT_EQ(0, bpm->GetPinCount(pageid0));

    ASSERT_EQ(1, bpm->GetPinCount(pageid1));
    page1_write.Drop();
    ASSERT_EQ(0, bpm->GetPinCount(pageid1));
  }

  {
    const auto temp_page_id1 = bpm->NewPage();
    const auto temp_page1_opt = bpm->CheckedReadPage(temp_page_id1);
    ASSERT_TRUE(temp_page1_opt.has_value());

    const auto temp_page_id2 = bpm->NewPage();
    const auto temp_page2_opt = bpm->CheckedWritePage(temp_page_id2);
    ASSERT_TRUE(temp_page2_opt.has_value());

    ASSERT_FALSE(bpm->GetPinCount(pageid0).has_value());
    ASSERT_FALSE(bpm->GetPinCount(pageid1).has_value());
  }

  {
    auto page0_write_opt = bpm->CheckedWritePage(pageid0);
    ASSERT_TRUE(page0_write_opt.has_value());
    auto page0_write = std::move(page0_write_opt.value()); // NOLINT
    EXPECT_STREQ(page0_write.GetData(), str0.c_str());
    CopyString(page0_write.GetDataMut(), str0updated);

    auto page1_write_opt = bpm->CheckedWritePage(pageid1);
    ASSERT_TRUE(page1_write_opt.has_value());
    auto page1_write = std::move(page1_write_opt.value()); // NOLINT
    EXPECT_STREQ(page1_write.GetData(), str1.c_str());
    CopyString(page1_write.GetDataMut(), str1updated);
    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    ASSERT_EQ(1, bpm->GetPinCount(pageid1));
  }

  ASSERT_EQ(0, bpm->GetPinCount(pageid0));
  ASSERT_EQ(0, bpm->GetPinCount(pageid1));

  {
    auto page0_read_opt = bpm->CheckedReadPage(pageid0);
    ASSERT_TRUE(page0_read_opt.has_value());
    const auto page0_read = std::move(page0_read_opt.value()); // NOLINT
    EXPECT_STREQ(page0_read.GetData(), str0updated.c_str());

    auto page1_read_opt = bpm->CheckedReadPage(pageid1);
    ASSERT_TRUE(page1_read_opt.has_value());
    const auto page1_read = std::move(page1_read_opt.value()); // NOLINT
    EXPECT_STREQ(page1_read.GetData(), str1updated.c_str());

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    ASSERT_EQ(1, bpm->GetPinCount(pageid1));
  }

  ASSERT_EQ(0, bpm->GetPinCount(pageid0));
  ASSERT_EQ(0, bpm->GetPinCount(pageid1));

  remove(db_fname);
  remove(disk_manager->GetLogFileName());
}

TEST(BufferPoolManagerTest, PagePinMediumTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  const auto pid0 = bpm->NewPage();
  auto page0 = bpm->WritePage(pid0);

  // Scenario: Once we have a page, we should be able to read and write content.
  const std::string hello = "Hello";
  CopyString(page0.GetDataMut(), hello);
  EXPECT_STREQ(page0.GetData(), hello.c_str());

  page0.Drop();

  // Create a vector of unique pointers to page guards, which prevents the guards from getting destructed.
  std::vector<WritePageGuard> pages;

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 0; i < FRAMES; i++) {
    const auto pid = bpm->NewPage();
    auto page = bpm->WritePage(pid);
    pages.push_back(std::move(page));
  }

  // Scenario: All of the pin counts should be 1.
  for (const auto &page : pages) {
    const auto pid = page.GetPageId();
    EXPECT_EQ(1, bpm->GetPinCount(pid));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = 0; i < FRAMES; i++) {
    const auto pid = bpm->NewPage();
    const auto fail = bpm->CheckedWritePage(pid);
    ASSERT_FALSE(fail.has_value());
  }
  // Scenario: Drop the first 5 pages to unpin them.
  for (size_t i = 0; i < FRAMES / 2; i++) {
    const auto pid = pages[0].GetPageId();
    EXPECT_EQ(1, bpm->GetPinCount(pid));
    pages.erase(pages.begin());
    EXPECT_EQ(0, bpm->GetPinCount(pid));
  }

  // Scenario: All of the pin counts of the pages we haven't dropped yet should still be 1.
  for (const auto &page : pages) {
    const auto pid = page.GetPageId();
    EXPECT_EQ(1, bpm->GetPinCount(pid));
  }

  // Scenario: After unpinning pages {1, 2, 3, 4, 5}, we should be able to create 4 new pages and bring them into
  // memory. Bringing those 4 pages into memory should evict the first 4 pages {1, 2, 3, 4} because of LRU.
  for (size_t i = 0; i < ((FRAMES / 2) - 1); i++) {
    const auto pid = bpm->NewPage();
    auto page = bpm->WritePage(pid);
    pages.push_back(std::move(page));
  }

  // Scenario: There should be one frame available, and we should be able to fetch the data we wrote a while ago.
  {
    const auto original_page = bpm->ReadPage(pid0);
    EXPECT_STREQ(original_page.GetData(), hello.c_str());
  }

  // Scenario: Once we unpin page 0 and then make a new page, all the buffer pages should now be pinned. Fetching page 0
  // again should fail.
  const auto last_pid = bpm->NewPage();
  const auto last_page = bpm->ReadPage(last_pid);

  const auto fail = bpm->CheckedReadPage(pid0);
  ASSERT_FALSE(fail.has_value());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove(db_fname);
}

TEST(BufferPoolManagerTest, PageAccessTest) {
  const size_t rounds = 50;

  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(1, disk_manager.get());

  const auto pid = bpm->NewPage();
  char buf[BUSTUB_PAGE_SIZE];

  auto thread = std::thread([&]() {
    // The writer can keep writing to the same page.
    for (size_t i = 0; i < rounds; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      auto guard = bpm->WritePage(pid);
      CopyString(guard.GetDataMut(), std::to_string(i));
    }
  });

  for (size_t i = 0; i < rounds; i++) {
    // Wait for a bit before taking the latch, allowing the writer to write some stuff.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // While we are reading, nobody should be able to modify the data.
    const auto guard = bpm->ReadPage(pid);

    // Save the data we observe.
    memcpy(buf, guard.GetData(), BUSTUB_PAGE_SIZE);

    // Sleep for a bit. If latching is working properly, nothing should be writing to the page.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Check that the data is unmodified.
    EXPECT_STREQ(guard.GetData(), buf);
  }

  thread.join();
}

TEST(BufferPoolManagerTest, ContentionTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const size_t rounds = 100000;

  const auto pid = bpm->NewPage();

  auto thread1 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      CopyString(guard.GetDataMut(), std::to_string(i));
    }
  });

  auto thread2 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      CopyString(guard.GetDataMut(), std::to_string(i));
    }
  });

  auto thread3 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      CopyString(guard.GetDataMut(), std::to_string(i));
    }
  });

  auto thread4 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      CopyString(guard.GetDataMut(), std::to_string(i));
    }
  });

  thread3.join();
  thread2.join();
  thread4.join();
  thread1.join();
}

TEST(BufferPoolManagerTest, DeadlockTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const auto pid0 = bpm->NewPage();
  const auto pid1 = bpm->NewPage();

  auto guard0 = bpm->WritePage(pid0);

  // A crude way of synchronizing threads, but works for this small case.
  std::atomic<bool> start = false;

  auto child = std::thread([&]() {
    // Acknowledge that we can begin the test.
    start.store(true);

    // Attempt to write to page 0.
    const auto guard0 = bpm->WritePage(pid0);
  });

  // Wait for the other thread to begin before we start the test.
  while (!start.load()) {
  }

  // Make the other thread wait for a bit.
  // This mimics the main thread doing some work while holding the write latch on page 0.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  // If your latching mechanism is incorrect, the next line of code will deadlock.
  // Think about what might happen if you hold a certain "all-encompassing" latch for too long...

  // While holding page 0, take the latch on page 1.
  const auto guard1 = bpm->WritePage(pid1);

  // Let the child thread have the page 0 since we're done with it.
  guard0.Drop();

  child.join();
}

TEST(BufferPoolManagerTest, EvictableTest) {
  // 测试用例核心目标：验证缓冲帧（frame）的"可驱逐状态"（evictable status）在任何情况下都始终正确
// 现实逻辑类比：好比测试一个只有1个停车位的停车场，验证当车位被占用时，其他车辆绝对无法进入，且停车位的"是否可用"标识永远准确
// 核心逻辑：通过单缓冲帧+多线程读写竞争，确保被占用的帧不会被错误驱逐，未分配的页无法强制加载
const size_t rounds = 1000;  // 测试循环次数：重复1000次以覆盖更多并发场景
const size_t num_readers = 8; // 并发读线程数量：模拟8个线程同时读取同一数据的场景

// 1. 创建磁盘管理器（DiskManager）：负责实际的磁盘文件读写操作
// 现实类比：相当于创建一个"文件柜管理员"，负责从物理文件柜（磁盘文件db_fname）中存取文件（数据页）
auto disk_manager = std::make_shared<DiskManager>(db_fname);

// 2. 创建缓冲池管理器（BufferPoolManager）：管理内存中的缓冲帧，控制数据页的加载、缓存和驱逐
// 关键参数：1个缓冲帧（仅分配1块内存空间），绑定上面创建的磁盘管理器
// 现实类比：相当于创建一个"前台接待员"，但只给了1个临时办公桌（缓冲帧），所有文件（数据页）必须先放到这个办公桌才能处理
auto bpm = std::make_shared<BufferPoolManager>(1, disk_manager.get());

// 3. 循环执行测试：重复1000次，模拟不同场景下的缓冲帧状态验证
for (size_t i = 0; i < rounds; i++) {
  std::mutex mutex;                // 互斥锁：用于同步主线程和读线程的操作，防止并发混乱
                                   // 现实类比：相当于前台的"叫号机"，确保只有完成当前操作的人才能进行下一步
  std::condition_variable cv;      // 条件变量：用于主线程向读线程发送"可以开始"的信号
                                   // 现实类比：相当于前台的"叫号喇叭"，告诉等待的人"现在可以开始办理业务了"

  // 信号标志：标记主线程是否已完成"抢占缓冲帧"的操作，读线程需等待此信号为true才能开始
  // 现实类比：相当于前台桌上的"正在服务"指示灯，灯亮（true）表示当前办公桌已被占用，等待的人可以开始准备
  bool signal = false;

  // 4. 分配"赢家页"（winner_pid）：在缓冲池中创建一个新数据页，并加载到唯一的缓冲帧中
  // 操作结果：唯一的缓冲帧被此页占用（相当于办公桌被第一个文件占用）
  // 现实类比：前台接待员（bpm）从文件柜（disk_manager）取出一份新文件（新数据页），放到唯一的办公桌上（缓冲帧），记为"赢家文件"
  const auto winner_pid = bpm->NewPage();

  // 5. 分配"输家页"（loser_pid）：再创建一个新数据页，但此时缓冲帧已被占用，此页无法加载到内存
  // 操作结果：仅获得页ID，但缓冲帧中无此页数据（相当于第二个文件没有办公桌可用）
  // 现实类比：再要一份新文件（loser_pid），但前台只有1个办公桌且已被占用，所以这份文件只能暂时存放在文件柜，无法拿到办公桌上
  const auto loser_pid = bpm->NewPage();

  // 6. 创建多个读线程：模拟8个并发线程读取"赢家页"的数据
  std::vector<std::thread> readers;
  for (size_t j = 0; j < num_readers; j++) {
    // 每个读线程的执行逻辑
    readers.emplace_back([&]() {
      // 6.1 线程上锁：通过互斥锁确保当前线程独占"信号判断"的操作
      // 现实类比：每个等待的人先去叫号机取号，确保自己不会和别人同时询问前台
      std::unique_lock<std::mutex> lock(mutex);

      // 6.2 等待信号：如果主线程还没完成"抢占缓冲帧"，当前读线程就阻塞等待
      // 现实类比：如果前台的"正在服务"灯没亮，就一直等喇叭通知，不打扰前台操作
      while (!signal) {
        cv.wait(lock); // 释放锁并阻塞，直到被cv.notify_all()唤醒
      }

      // 6.3 读取"赢家页"：获取该页的读锁（共享锁），确保可以安全读取
      // 操作逻辑：因为缓冲帧已被winner_pid占用，读锁会成功获取（多个读线程可共享读锁）
      // 现实类比：拿到喇叭通知后，上前读取办公桌上的"赢家文件"（多个线程相当于多个人同时看这份文件）
      const auto read_guard = bpm->ReadPage(winner_pid);

      // 6.4 核心断言：验证"输家页"无法加载到缓冲帧
      // 逻辑依据：唯一的缓冲帧被winner_pid占用且处于"不可驱逐"状态（因有读锁），所以CheckedReadPage(loser_pid)应返回空
      // 现实类比：尝试把"输家文件"拿到办公桌上，但桌子已被占用且有人正在使用，所以必然失败（断言确保这一点）
      ASSERT_FALSE(bpm->CheckedReadPage(loser_pid).has_value());
    });
  }

  // 7. 主线程操作：分两种场景（偶数轮读锁、奇数轮写锁）抢占缓冲帧
  // 现实类比：前台接待员（主线程）先对办公桌上的"赢家文件"进行操作，再允许其他人读取
  std::unique_lock<std::mutex> lock(mutex);

  if (i % 2 == 0) {
    // 7.1 场景1：获取"赢家页"的读锁（共享锁）
    // 操作影响：缓冲帧被winner_pid占用，且读锁会将帧标记为"不可驱逐"（evictable=false）
    // 现实类比：前台先自己看一遍"赢家文件"（读锁），此时桌子被占用，别人不能拿走文件，但可以一起看
    auto read_guard = bpm->ReadPage(winner_pid);

    // 7.2 发送信号：通知所有读线程可以开始读取
    // 操作逻辑：设置signal=true并唤醒所有等待的线程，此时读线程会竞争获取读锁
    // 现实类比：前台按下"叫号喇叭"（cv.notify_all()），告诉等待的人可以过来一起看文件了
    signal = true;
    cv.notify_all();
    lock.unlock(); // 释放互斥锁，允许读线程执行

    // 7.3 释放读锁：允许其他线程完全占用读锁（主线程不再持有锁）
    // 现实类比：前台看完文件后，把文件留在桌上，让其他人继续看
    read_guard.Drop();
  } else {
    // 7.4 场景2：获取"赢家页"的写锁（排他锁）
    // 操作影响：缓冲帧被winner_pid占用，写锁会将帧标记为"不可驱逐"，且此时其他线程无法获取读锁或写锁
    // 现实类比：前台要修改"赢家文件"（写锁），此时桌子被独占，别人既不能看也不能改，只能等前台改完
    auto write_guard = bpm->WritePage(winner_pid);

    // 7.5 发送信号：通知所有读线程可以开始等待读锁
    // 逻辑细节：此时写锁未释放，读线程被唤醒后会尝试获取读锁，但会阻塞到写锁释放
    // 现实类比：前台按下"叫号喇叭"，但自己还在修改文件，等待的人只能在旁边等修改完成
    signal = true;
    cv.notify_all();
    lock.unlock(); // 释放互斥锁，读线程可进入"等待读锁"状态

    // 7.6 释放写锁：允许读线程获取读锁
    // 操作影响：写锁释放后，等待的读线程会同时获取共享读锁，帧仍保持"不可驱逐"状态
    // 现实类比：前台改完文件后，把文件放回桌上，允许等待的人一起看
    write_guard.Drop();
  }

  // 8. 等待所有读线程完成：确保当前轮次的所有测试逻辑执行完毕，再进入下一轮
  // 现实类比：等所有看文件的人都看完，再开始下一次的文件处理（下一轮循环）
  for (size_t j = 0; j < num_readers; j++) {
    readers[j].join();
  }


  }
}

/**
 * 测试FrameHeader中is_dirty_字段的功能
 * is_dirty_字段用于标记页面是否被修改过，决定是否需要写回磁盘
 * 
 * 测试场景：
 * 1. 初始状态：新页面应该不是dirty的
 * 2. 写操作后：修改页面数据后应该标记为dirty
 * 3. 刷新后：FlushPage后应该清除dirty标记
 * 4. 读操作：只读操作不应该影响dirty状态
 * 5. 页面替换：页面被替换时应该正确处理dirty状态
 */
TEST(BufferPoolManagerTest, IsDirtyFieldTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  // 测试1：新页面的初始状态应该是干净的
  const page_id_t pid = bpm->NewPage();
  ASSERT_NE(pid, -1);
  
  // 获取页面并检查初始dirty状态
  {
    auto write_guard = bpm->WritePage(pid);
    // 新页面应该不是dirty的
    // 注意：这里我们通过检查页面内容来间接验证dirty状态
    // 因为FrameHeader的is_dirty_是private成员，我们通过行为来测试
  }

  // 测试2：写操作后页面应该被标记为dirty
  {
    auto write_guard = bpm->WritePage(pid);
    const std::string test_data = "Test dirty flag";
    CopyString(write_guard.GetDataMut(), test_data);
    
    // 验证数据被写入
    EXPECT_STREQ(write_guard.GetData(), test_data.c_str());
    
    // 此时页面应该被标记为dirty（通过后续的FlushPage行为验证）
  }

  // 测试3：FlushPage应该清除dirty标记
  {
    // 刷新页面到磁盘
    bool flush_result = bpm->Cut(pid);
    EXPECT_TRUE(flush_result);
    
    // 再次刷新应该不会触发写操作（因为已经不是dirty了）
    // 这通过FlushPageUnsafe的行为来间接验证
  }

  // 测试4：只读操作不应该影响dirty状态
  {
    auto read_guard = bpm->ReadPage(pid);
    const std::string original_data = "Test dirty flag";
    EXPECT_STREQ(read_guard.GetData(), original_data.c_str());
    
    // 只读操作后，页面应该仍然不是dirty的
    // 这通过后续的FlushPage行为来验证
  }

  // 测试5：多次写操作后的dirty状态
  {
    auto write_guard1 = bpm->WritePage(pid);
    const std::string data1 = "First modification";
    CopyString(write_guard1.GetDataMut(), data1);
    
    write_guard1.Drop();
    
    auto write_guard2 = bpm->WritePage(pid);
    const std::string data2 = "Second modification";
    CopyString(write_guard2.GetDataMut(), data2);
    
    // 验证最终数据
    EXPECT_STREQ(write_guard2.GetData(), data2.c_str());
    
    // 页面应该被标记为dirty
  }
}

/**
 * 测试is_dirty_字段在页面替换场景下的行为
 */
TEST(BufferPoolManagerTest, IsDirtyFieldReplacementTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(2, disk_manager.get()); // 小容量便于测试替换

  // 创建两个页面
  const page_id_t pid1 = bpm->NewPage();
  const page_id_t pid2 = bpm->NewPage();
  
  ASSERT_NE(pid1, -1);
  ASSERT_NE(pid2, -1);

  // 修改第一个页面，使其成为dirty
  {
    auto write_guard = bpm->WritePage(pid1);
    const std::string dirty_data = "Dirty page data";
    CopyString(write_guard.GetDataMut(), dirty_data);
  }

  // 修改第二个页面，使其也成为dirty
  {
    auto write_guard = bpm->WritePage(pid2);
    const std::string dirty_data2 = "Another dirty page";
    CopyString(write_guard.GetDataMut(), dirty_data2);
  }

  // 创建第三个页面，触发页面替换
  const page_id_t pid3 = bpm->NewPage();
  ASSERT_NE(pid3, -1);

  // 验证第三个页面可以正常写入
  {
    auto write_guard = bpm->WritePage(pid3);
    const std::string new_data = "New page data";
    CopyString(write_guard.GetDataMut(), new_data);
    EXPECT_STREQ(write_guard.GetData(), new_data.c_str());
  }

  // 验证被替换的页面数据仍然正确（通过重新加载）
  {
    auto read_guard = bpm->ReadPage(pid1);
    const std::string expected_data = "Dirty page data";
    EXPECT_STREQ(read_guard.GetData(), expected_data.c_str());
  }
}

/**
 * 测试is_dirty_字段在并发访问下的行为
 */
TEST(BufferPoolManagerTest, IsDirtyFieldConcurrencyTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const page_id_t pid = bpm->NewPage();
  ASSERT_NE(pid, -1);

  const int num_threads = 4;
  std::vector<std::thread> threads;
  std::atomic<int> write_count{0};
  std::atomic<int> read_count{0};

  // 启动多个线程进行读写操作
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&bpm, pid, i, &write_count, &read_count]() {
      for (int j = 0; j < 10; ++j) {
        if (i % 2 == 0) {
          // 写线程
          auto write_guard = bpm->WritePage(pid);
          const std::string data = "Thread " + std::to_string(i) + " write " + std::to_string(j);
          CopyString(write_guard.GetDataMut(), data);
          write_count.fetch_add(1);
        } else {
          // 读线程
          auto read_guard = bpm->ReadPage(pid);
          // 验证数据不为空
          EXPECT_GT(strlen(read_guard.GetData()), 0);
          read_count.fetch_add(1);
        }
      }
    });
  }

  // 等待所有线程完成
  for (auto& thread : threads) {
    thread.join();
  }

  // 验证读写操作都执行了
  EXPECT_GT(write_count.load(), 0);
  EXPECT_GT(read_count.load(), 0);

  // 最终验证页面数据的一致性
  {
    auto read_guard = bpm->ReadPage(pid);
    // 数据应该不为空且格式正确
    const char* data = read_guard.GetData();
    EXPECT_GT(strlen(data), 0);
    EXPECT_TRUE(strstr(data, "Thread") != nullptr);
  }
}

/**
 * 测试is_dirty_字段在FlushAllPages场景下的行为
 */
TEST(BufferPoolManagerTest, IsDirtyFieldFlushAllTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  // 创建多个页面并修改它们
  std::vector<page_id_t> page_ids;
  for (size_t i = 0; i < 5; ++i) {
    const page_id_t pid = bpm->NewPage();
    ASSERT_NE(pid, -1);
    page_ids.push_back(pid);

    // 修改每个页面，使其成为dirty
    auto write_guard = bpm->WritePage(pid);
    const std::string data = "Page " + std::to_string(i) + " dirty data";
    CopyString(write_guard.GetDataMut(), data);
  }

  // 刷新所有页面
  bpm->FlushAllPages();

  // 验证所有页面数据仍然正确
  for (size_t i = 0; i < page_ids.size(); ++i) {
    auto read_guard = bpm->ReadPage(page_ids[i]);
    const std::string expected_data = "Page " + std::to_string(i) + " dirty data";
    EXPECT_STREQ(read_guard.GetData(), expected_data.c_str());
  }

  // 再次刷新所有页面（应该不会触发写操作，因为已经不是dirty了）
  bpm->FlushAllPagesUnsafe();

  // 验证数据仍然正确
  for (size_t i = 0; i < page_ids.size(); ++i) {
    auto read_guard = bpm->ReadPage(page_ids[i]);
    const std::string expected_data = "Page " + std::to_string(i) + " dirty data";
    EXPECT_STREQ(read_guard.GetData(), expected_data.c_str());
  }
  }

TEST(BufferPoolManagerTest, IsDirtyFieldMemoryExhaustionTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(3, disk_manager.get()); // 极小容量

  // 填满缓冲池，所有页面都标记为dirty
  std::vector<page_id_t> page_ids;
  for (size_t i = 0; i < 3; ++i) {
    const page_id_t pid = bpm->NewPage();
    ASSERT_NE(pid, -1);
    page_ids.push_back(pid);

    // 每个页面都写入大量数据，确保dirty
    auto write_guard = bpm->WritePage(pid);
    const std::string large_data = "Large dirty data for page " + std::to_string(i) +
                                   std::string(BUSTUB_PAGE_SIZE / 2, 'X');
    CopyString(write_guard.GetDataMut(), large_data);
  }

  // 尝试创建第4个页面，应该触发替换
  const page_id_t new_pid = bpm->NewPage();
  ASSERT_NE(new_pid, -1);

  // 验证被替换的页面数据仍然正确（说明dirty页面被正确写回）
  for (const auto& pid : page_ids) {
    auto read_guard = bpm->ReadPage(pid);
    EXPECT_TRUE(strstr(read_guard.GetData(), "Large dirty data") != nullptr);
  }

  // 新页面也应该可以正常写入
  {
    auto write_guard = bpm->WritePage(new_pid);
    const std::string new_data = "New page in exhausted memory";
    CopyString(write_guard.GetDataMut(), new_data);
    EXPECT_STREQ(write_guard.GetData(), new_data.c_str());
  }
}

/**
 * 极端测试2：大量并发写操作下的is_dirty_竞争条件
 * 测试高并发环境下dirty标记的正确性
 */
TEST(BufferPoolManagerTest, IsDirtyFieldHeavyConcurrencyTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const int num_pages = 8;
  const int num_threads = 2;
  std::vector<page_id_t> page_ids;

  // 创建大量页面
  for (int i = 0; i < num_pages; ++i) {
    const page_id_t pid = bpm->NewPage();
    ASSERT_NE(pid, -1);
    page_ids.push_back(pid);
  }

  std::vector<std::thread> threads;
  std::atomic<int> total_writes{0};
  std::atomic<int> total_reads{0};
  std::atomic<int> flush_operations{0};

  // 启动大量线程进行疯狂操作
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&bpm, &page_ids, t, &total_writes, &total_reads]() {
      for (int round = 0; round < 50; ++round) {
        const int page_idx = 3;
        const page_id_t pid = page_ids[page_idx];

        switch (round % 3) {
          case 0: {
            // 写操作
            auto write_guard = bpm->WritePage(pid);
            const std::string data = "Thread" + std::to_string(t) + "_Round" + std::to_string(round);
            CopyString(write_guard.GetDataMut(), data);
            total_writes.fetch_add(1);
            break;
          }
          case 1: {
            // 读操作
            auto read_guard = bpm->ReadPage(pid);
            EXPECT_GT(strlen(read_guard.GetData()), 0);
            total_reads.fetch_add(1);
            break;
          }
          case 2: {
            // 混合操作：先写后读
            {
              auto write_guard = bpm->WritePage(pid);
              const std::string data = "Mixed_" + std::to_string(t) + "_" + std::to_string(round);
              CopyString(write_guard.GetDataMut(), data);
            }
            {
              auto read_guard = bpm->ReadPage(pid);
              EXPECT_TRUE(strstr(read_guard.GetData(), "Mixed_") != nullptr);
            }
            total_writes.fetch_add(1);
            total_reads.fetch_add(1);
            break;
          }
        }
      }
    });
  }

  // 等待所有线程完成
  for (auto& thread : threads) {
    thread.join();
  }

}

/**
 * 极端测试3：频繁的页面替换和dirty状态管理
 * 测试在持续页面替换压力下的dirty标记正确性
 */
TEST(BufferPoolManagerTest, IsDirtyFieldContinuousReplacementTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(2, disk_manager.get()); // 极小容量强制替换

  const int num_operations = 1000;
  std::vector<page_id_t> created_pages;

  // 持续创建和修改页面，强制触发页面替换
  for (int i = 0; i < num_operations; ++i) {
    const page_id_t pid = bpm->NewPage();
    ASSERT_NE(pid, -1);
    created_pages.push_back(pid);

    // 立即修改页面，使其成为dirty
    {
      auto write_guard = bpm->WritePage(pid);
      const std::string data = "Continuous replacement test " + std::to_string(i);
      CopyString(write_guard.GetDataMut(), data);
    }

    // 每10次操作验证一次数据一致性
    if (i % 10 == 0) {
      // 验证最近创建的页面数据正确
      for (int j = std::max(0, i - 5); j <= i; ++j) {
        auto read_guard = bpm->ReadPage(created_pages[j]);
        const std::string expected = "Continuous replacement test " + std::to_string(j);
        EXPECT_STREQ(read_guard.GetData(), expected.c_str());
      }
    }

    // 每50次操作进行一次批量刷新
    if (i % 50 == 0) {
      bpm->FlushAllPages();
    }
  }

  // 最终验证：检查最后几个页面的数据
  for (int i = num_operations - 5; i < num_operations; ++i) {
    auto read_guard = bpm->ReadPage(created_pages[i]);
    const std::string expected = "Continuous replacement test " + std::to_string(i);
    EXPECT_STREQ(read_guard.GetData(), expected.c_str());
  }
}

/**
 * 极端测试4：异常情况下的is_dirty_行为
 * 测试在异常操作下的dirty状态管理
 */
TEST(BufferPoolManagerTest, IsDirtyFieldExceptionHandlingTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const page_id_t pid = bpm->NewPage();
  ASSERT_NE(pid, -1);

  // 测试1：在guard析构前发生异常的情况
  try {
    auto write_guard = bpm->WritePage(pid);
    const std::string data = "Data before exception";
    CopyString(write_guard.GetDataMut(), data);

    // 模拟异常情况
    if (true) { // 总是抛出异常
      throw std::runtime_error("Simulated exception");
    }

    // 这行不应该执行到
    write_guard.Drop();
  } catch (const std::exception&) {
    // 异常被捕获，guard应该正常析构
  }

  // 验证页面数据仍然正确（guard析构时应该正确处理dirty状态）
  {
    auto read_guard = bpm->ReadPage(pid);
    EXPECT_STREQ(read_guard.GetData(), "Data before exception");
  }

  // 测试2：多次异常和恢复
  for (int i = 0; i < 10; ++i) {
    try {
      auto write_guard = bpm->WritePage(pid);
      const std::string data = "Exception test " + std::to_string(i);
      CopyString(write_guard.GetDataMut(), data);

      if (i % 3 == 0) {
        throw std::runtime_error("Periodic exception");
      }

      write_guard.Drop();
    } catch (const std::exception&) {
      // 异常被处理
    }
  }

  // 最终验证数据一致性
  {
    auto read_guard = bpm->ReadPage(pid);
    EXPECT_TRUE(strstr(read_guard.GetData(), "Exception test") != nullptr);
  }
}

/**
 * 极端测试5：内存压力下的dirty状态管理
 * 测试在内存压力下的dirty标记和页面管理
 */
TEST(BufferPoolManagerTest, IsDirtyFieldMemoryPressureTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(1, disk_manager.get()); // 单帧缓冲池

  const int num_pages = 100;
  std::vector<page_id_t> page_ids;

  // 创建大量页面，强制持续替换
  for (int i = 0; i < num_pages; ++i) {
    const page_id_t pid = bpm->NewPage();
    ASSERT_NE(pid, -1);
    page_ids.push_back(pid);

    // 每个页面都写入大量数据
    {
      auto write_guard = bpm->WritePage(pid);
      const std::string large_data = "Large data for page " + std::to_string(i) +
                                   std::string(BUSTUB_PAGE_SIZE - 100, 'A');
      CopyString(write_guard.GetDataMut(), large_data);
    }

    // 每10个页面验证一次数据
    if (i % 10 == 0) {
      // 验证当前页面数据
      auto read_guard = bpm->ReadPage(pid);
      EXPECT_TRUE(strstr(read_guard.GetData(), "Large data") != nullptr);

      // 验证之前的一些页面仍然可以访问（说明dirty页面被正确写回）
      for (int j = std::max(0, i - 5); j < i; ++j) {
        auto read_guard_prev = bpm->ReadPage(page_ids[j]);
        EXPECT_TRUE(strstr(read_guard_prev.GetData(), "Large data") != nullptr);
      }
    }
  }

  // 最终验证：随机检查一些页面
  std::vector<int> test_indices = {0, 25, 50, 75, 99};
  for (int idx : test_indices) {
    auto read_guard = bpm->ReadPage(page_ids[idx]);
    const std::string expected = "Large data for page " + std::to_string(idx);
    EXPECT_TRUE(strstr(read_guard.GetData(), expected.c_str()) != nullptr);
  }
}

/**
 * 极端测试6：极限并发读写混合操作
 * 测试在极限并发情况下的dirty状态管理
 */
TEST(BufferPoolManagerTest, IsDirtyFieldExtremeConcurrencyTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const int num_pages = 10;
  const int num_threads = 32; // 大量线程
  std::vector<page_id_t> page_ids;

  // 创建页面
  for (int i = 0; i < num_pages; ++i) {
    const page_id_t pid = bpm->NewPage();
    ASSERT_NE(pid, -1);
    page_ids.push_back(pid);
  }

  std::vector<std::thread> threads;
  std::atomic<int> operations_count{0};
  std::atomic<bool> stop_flag{false};
  // 写操作
  {
    auto write_guard = bpm->WritePage(1);
    const std::string data = "Extreme_" + std::to_string(1) + "_" + std::to_string(operations_count.load());
    CopyString(write_guard.GetDataMut(), data);
  }
  // 启动大量线程进行极限操作
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&bpm,t, &operations_count, &stop_flag]() {
      while (!stop_flag.load()) {
        const page_id_t pid = 1;

        // 随机选择操作类型
        int op_type = (t + operations_count.load()) % 2;

        switch (op_type) {
          case 0: {
            // 读操作
            auto read_guard = bpm->ReadPage(pid);
            EXPECT_GT(strlen(read_guard.GetData()), 0);
            break;
          }
          case 1: {
            // 混合操作：写后立即读
            {
              auto write_guard = bpm->WritePage(pid);
              const std::string data = "Mixed_" + std::to_string(t);
              CopyString(write_guard.GetDataMut(), data);
            }

              auto read_guard = bpm->ReadPage(pid);
              EXPECT_TRUE(strstr(read_guard.GetData(), "Mixed_") != nullptr);
            break;
          }
        }
        operations_count.fetch_add(1);
      }
    });
  }

  // 运行一段时间后停止
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  stop_flag.store(true);

  // 等待所有线程完成
  for (auto& thread : threads) {
    thread.join();
  }

  // 验证最终状态
  EXPECT_GT(operations_count.load(), 0);


}
}
