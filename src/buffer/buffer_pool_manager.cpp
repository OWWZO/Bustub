//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"
#include <algorithm>
#include <any>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <utility>

namespace bustub {
/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to
 * default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for
 * more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header
 * for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id)
    : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) {
  Reset();
}

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

BufferPoolManager::BufferPoolManager(size_t num_frames,
                                     DiskManager *disk_manager,
                                     LogManager *log_manager)
    : num_frames_(num_frames), next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all  the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to
  // exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all the frame headers, and fill the free frame list with all
  // possible frame IDs (since all frames are initially free).

  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the
 * buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

auto BufferPoolManager::NewPage() -> page_id_t {
  page_id_t next_page;
  if (free_frames_.empty()) {
    auto temp = replacer_->Evict();
    if (temp.has_value()) {
      Cut(temp.value());
    } else {
      return -1;
    }
    std::unique_lock lock(*bpm_latch_);
    next_page = next_page_id_.fetch_add(1);
    frame_id_t frame_id = free_frames_.front();
    free_frames_.pop_front();

    for (auto &item : frames_) {
      if (item->frame_id_ == frame_id) {
        item->page_id_ = next_page;
        break;
      }
    }
    page_table_[next_page] = frame_id;
  } else {
    std::unique_lock lock(*bpm_latch_);
    next_page = next_page_id_.fetch_add(1);
    frame_id_t frame_id = free_frames_.front();
    free_frames_.pop_front();

    for (auto &item : frames_) {
      if (item->frame_id_ == frame_id) {
        item->page_id_ = next_page;
        break;
      }
    }

    page_table_[next_page] = frame_id;
  }
  return next_page;
}

/*
 * @brief 从数据库中移除一个页面，包括磁盘和内存中的页面。
 *
 * 如果该页面在缓冲池中处于固定状态，此函数不执行任何操作并返回`false`。否则，此函数会从磁盘和内存中（如果仍在缓冲池中）移除该页面，并返回`true`。
 *
 * ### 实现
 *
 * 考虑页面或页面元数据可能存在的所有位置，并据此指导你实现此函数。你可能希望在实现`CheckedReadPage`和`CheckedWritePage`之后再实现此函数。
 *
 * 你应该调用磁盘调度器中的`DeallocatePage`，以使空间可用于新页面。
 *
 * TODO(P1)：添加实现。
 *
 * @param page_id 我们要删除的页面的页面ID。
 * @return
 如果页面存在但无法删除，则返回`false`；如果页面不存在或删除成功，则返回`true`。
 图书馆下架一本书——先查这本书是否在书架上（页表找帧ID），
   * 如果在且没人借（pin_count=0），就把书架清空（重置帧头）、从页表删除记录、
   * 通知仓库删除这本书（磁盘删除页）；如果有人借或不在架上，删除失败。
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock lock(*bpm_latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto frame = page_table_[page_id];

  for (auto it = frames_.begin(); it != frames_.end(); it++) {
    if (frame == (*it)->frame_id_) {
      if (0 != (*it)->pin_count_.load()) {
        return false;
      }
      (*it)->Reset();
      (*it)->page_id_ = INVALID_PAGE_ID;

      free_frames_.push_front(frame);
      break;
    }
  }

  page_table_.erase(page_id);

  disk_scheduler_->DeallocatePage(page_id);

  return true;
}

/**
 * @brief 获取对一页数据的可选写锁定保护。用户可根据需要指定`AccessType`。
 *
 * 如果无法将该页数据载入内存，此函数将返回`std::nullopt`。
 *
 * 页数据只能通过页保护机制进行访问。本`BufferPoolManager`的用户应根据所需的访问模式，获取`ReadPageGuard`或`WritePageGuard`，
 * 以确保数据访问的线程安全性。
 *
 * 同一时间只能有一个`WritePageGuard`对某一页进行读写操作。这使得数据访问既可以是不可变的，也可以是可变的，即拥有`WritePageGuard`的线程
 * 可以随意操作该页的数据。如果用户希望多个线程同时读取该页，则必须通过`CheckedReadPage`获取`ReadPageGuard`。
 *
 * ### 实现说明
 *
 * 有三种主要情况需要实现。前两种相对简单：一种是存在充足的可用内存，另一种是无需执行额外的I/O操作。请思考这两种情况具体包含哪些场景。
 *
 * 第三种情况最为复杂，即当我们没有“轻易可用”的内存时。缓冲池需要寻找可用于载入一页内存的空间，此时需使用之前实现的替换算法来确定可驱逐的候选帧。
 *
 * 一旦缓冲池确定了要驱逐的帧，可能需要执行若干I/O操作，将所需的页数据载入该帧。
 *
 * 本函数与`CheckedReadPage`可能存在大量共享代码，因此创建辅助函数可能会很有帮助。
 *
 * 这两个函数是本项目的核心，因此我们不会提供更多提示。祝你顺利！
 *
 * TODO(P1)：添加实现代码。
 *
 * @param page_id 想要写入的页的ID。
 * @param access_type 页访问类型。
 * @return std::optional<WritePageGuard>
 * 一个可选的锁存保护。如果没有更多空闲帧（内存不足），则返回`std::nullopt`；
 * 否则，返回一个`WritePageGuard`，确保对某一页数据的独占且可变的访问权。
 */
/*先看相册在不在桌上：
如果相册已经在客厅桌子上（内存里已有这页数据），那正好，直接下一步。
如果不在，就得去柜子里把相册抱到桌上（从硬盘读数据到内存）。但要是桌子满了（内存没空闲空间），就得把桌上暂时不用的东西（比如爷爷的报纸）收起来放回柜子（用
“替换算法” 找个 “可赶走” 的内容，写回硬盘腾地方），再把相册放桌上。 给桌子上把
“专属锁”： 相册放好后，你得给桌子挂个 “只有我能用” 的牌子（对应 “WritePageGuard
写保护锁”）。这时候别人都不能碰 ——
比如爷爷想翻相册、妈妈想加新照片，都得等你把锁取下来才行（同一时间只能有一个
“写锁”，保证数据不乱）。 特殊情况：没地方放相册了：
如果客厅桌子、沙发、椅子全摆满了东西（内存完全没空闲），实在腾不出地方放相册，那你就没法抱相册出来改了（对应返回
“std::nullopt”，表示内存不够没法操作）*/
auto BufferPoolManager::CheckedWritePage(page_id_t page_id,
                                         AccessType access_type)
    -> std::optional<WritePageGuard> {
  if (page_table_.find(page_id) != page_table_.end()) {
    for (auto &item : frames_) {
      if (item->page_id_ == page_id) {
        std::unique_lock lock(*bpm_latch_);
        item->pin_count_.fetch_add(1);
        break;
      }
    }
    replacer_->RecordAccess(page_table_[page_id], page_id);
    auto temp_lock = bpm_latch_;
    return std::make_optional(
        WritePageGuard(page_id, GetFrameById(page_table_[page_id]), replacer_,
                       temp_lock, disk_scheduler_));
  }
  if (!free_frames_.empty() && NewPageById(page_id)) {
    auto data = GetFrameById(page_table_[page_id])->data_.data();
    if (!data) {
      return std::nullopt;
    }
    std::promise<bool> promise = disk_scheduler_->CreatePromise();

    auto task = std::make_optional(
        DiskRequest(false, data, page_id, std::move(promise)));

    disk_scheduler_->Read(task);

    replacer_->RecordAccess(page_table_[page_id], page_id);
    for (auto &item : frames_) {
      if (item->page_id_ == page_id) {
        std::unique_lock lock(*bpm_latch_);
        item->pin_count_.fetch_add(1);
        break;
      }
    }
    return std::make_optional(
        WritePageGuard(page_id, GetFrameById(page_table_[page_id]), replacer_,
                       bpm_latch_, disk_scheduler_));
  }

  auto frame_id = replacer_->Evict();
  // 已经有要淘汰的帧 执行pool的删除帧相关信息逻辑

  if (frame_id.has_value() && Cut(frame_id.value())) {
    auto data = GetFrameById(frame_id.value())->data_.data();
    if (!data) {
      return std::nullopt;
    }
    std::promise<bool> promise = disk_scheduler_->CreatePromise();
    auto task = std::make_optional(
        DiskRequest(false, data, page_id, std::move(promise)));

    disk_scheduler_->Read(task);
    replacer_->RecordAccess(page_table_[page_id], page_id);

    NewPageById(page_id);
    for (auto &item : frames_) {
      if (item->page_id_ == page_id) {
        std::unique_lock lock(*bpm_latch_);
        item->pin_count_.fetch_add(1);
        break;
      }
    }
    return std::make_optional(
        WritePageGuard(page_id, GetFrameById(frame_id.value()), replacer_,
                       bpm_latch_, disk_scheduler_));
  }

  return std::nullopt;
}

/**
@brief 获取数据页上的可选读锁保护。用户可根据需要指定AccessType。
若无法将数据页载入内存，此函数将返回std::nullopt。
页数据只能通过页保护来访问。BufferPoolManager的用户应根据所需的访问模式获取ReadPageGuard或WritePageGuard，
以确保数据访问的线程安全性。
在不同线程中，可同时存在任意数量的ReadPageGuard读取同一数据页。但所有数据访问都必须是不可变的。
若用户想要修改页数据，必须通过CheckedWritePage获取WritePageGuard。
实现
参见CheckedWritePage的实现细节。
TODO (P1)：添加实现。
@param page_id 想要读取的页的 ID。
@param access_type 页访问的类型。
@return std::optional<ReadPageGuard>
一个可选的闩锁保护，若没有更多空闲帧（内存不足），则返回std::nullopt；
否则，返回一个ReadPageGuard，确保对页数据的共享和只读访问。
*/
auto BufferPoolManager::CheckedReadPage(page_id_t page_id,
                                        AccessType access_type)
    -> std::optional<ReadPageGuard> {
  if (page_id == -1) {
    return std::nullopt;
  }
  if (page_table_.find(page_id) != page_table_.end()) {
    replacer_->RecordAccess(page_table_[page_id], page_id);
    for (auto &item : frames_) {
      if (item->page_id_ == page_id) {
        std::unique_lock lock(*bpm_latch_);
        item->pin_count_.fetch_add(1);
        break;
      }
    }

    auto temp_lock = bpm_latch_;
    return std::make_optional(
        ReadPageGuard(page_id, GetFrameById(page_table_[page_id]), replacer_,
                      temp_lock, disk_scheduler_));
  }
  if (!free_frames_.empty() && NewPageById(page_id)) {
    auto data = GetFrameById(page_table_[page_id])->data_.data();
    if (!data) {
      return std::nullopt;
    }
    std::promise<bool> promise = disk_scheduler_->CreatePromise();

    auto task = std::make_optional(
        DiskRequest(false, data, page_id, std::move(promise)));

    disk_scheduler_->Read(task);

    replacer_->RecordAccess(page_table_[page_id], page_id);
    for (auto &item : frames_) {
      if (item->page_id_ == page_id) {
        std::unique_lock lock(*bpm_latch_);
        item->pin_count_.fetch_add(1);
        break;
      }
    }
    return std::make_optional(
        ReadPageGuard(page_id, GetFrameById(page_table_[page_id]), replacer_,
                      bpm_latch_, disk_scheduler_));
  }

  auto frame_id = replacer_->Evict();
  // 已经有要淘汰的帧 执行pool的删除帧相关信息逻辑

  if (frame_id.has_value() && Cut(frame_id.value())) {
    auto data = GetFrameById(frame_id.value())->data_.data();
    if (!data) {
      return std::nullopt;
    }
    std::promise<bool> promise = disk_scheduler_->CreatePromise();
    auto task = std::make_optional(
        DiskRequest(false, data, page_id, std::move(promise)));

    disk_scheduler_->Read(task);

    replacer_->RecordAccess(frame_id.value(), page_id);

    NewPageById(page_id);
    for (auto &item : frames_) {
      if (item->page_id_ == page_id) {
        std::unique_lock lock(*bpm_latch_);
        item->pin_count_.fetch_add(1);
        break;
      }
    }
    // TODO(wwz) 加锁等待任务完成
    return std::make_optional(
        ReadPageGuard(page_id, GetFrameById(frame_id.value()), replacer_,
                      bpm_latch_, disk_scheduler_));
  }

  return std::nullopt;
}

/**
@brief 对CheckedWritePage的一个包装，如果内部值存在则将其解包。
如果CheckedWritePage返回std::nullopt，此函数会终止整个进程。
此函数仅应出于测试和便捷性考虑使用。如果缓冲池管理器存在耗尽内存的可能性，则应使用CheckedPageWrite以处理该情况。
有关实现的更多信息，请参阅CheckedPageWrite的文档。
@param page_id 我们想要读取的页的 ID。
@param access_type 页访问的类型。
@return WritePageGuard 一个页保护机制，确保对页数据的独占且可修改的访问。
*/
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type)
    -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n",
                 page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief 一个围绕`CheckedReadPage`的包装器，若内部值存在则对其进行解包。
 *
 * 若`CheckedReadPage`返回`std::nullopt`，**此函数会终止整个进程。**
 *
 * 此函数**仅**应出于测试和便捷性考虑使用。如果缓冲池管理器存在耗尽内存的可能性，则应使用`CheckedPageWrite`以应对这种情况。
 *
 * 有关实现的更多信息，请参阅`CheckedPageRead`的文档。
 *
 * @param page_id 我们想要读取的页面的ID。
 * @param access_type 页面访问的类型。
 * @return ReadPageGuard 一个页面保护机制，确保对页面数据的共享和只读访问。
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type)
    -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n",
                 page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief 不安全地将页面数据刷新到磁盘。
 *
 * 如果页面已被修改，此函数会将页面数据写入磁盘。如果给定的页面不在内存中，
 * 此函数将返回`false`。
 *
 * 在此函数中不应为页面加锁。
 * 这意味着你需要仔细考虑何时切换`is_dirty_`位。
 *
 * ### 实现
 *
 * 你或许应该在完成`CheckedReadPage`和`CheckedWritePage`之后再着手实现此函数，
 * 因为这样可能会更容易理解该做什么。
 *
 * TODO(P1)：添加实现
 *
 * @param page_id 要刷新的页面的页面ID。
 * @return 如果在页表中找不到该页面，则返回`false`；否则，返回`true`。
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  auto it = page_table_.find(page_id);
  std::shared_ptr<FrameHeader> frame;
  if (it != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    bool is_dirty = false;
    for (auto &item : frames_) {
      if (item->frame_id_ == frame_id) {
        is_dirty = item->is_dirty_;
        frame = item;
        break;
      }
    }
    if (is_dirty) {
      auto data = frame->data_.data();

      std::promise<bool> promise = disk_scheduler_->CreatePromise();
      auto task = std::make_optional(
          DiskRequest(false, data, page_id, std::move(promise)));

      disk_scheduler_->Write(task);
      frame->Reset();
    }
    return true;
  }
  return false;
}

/**
 * @brief 安全地将页面的数据刷新到磁盘。
 *
 * 如果页面已被修改，此函数会将页面的数据写入磁盘。如果给定的页面不在内存中，
 * 此函数将返回`false`。
 *
 * 你应该在此函数中对页面加锁，以确保将一致的状态刷新到磁盘。
 *
 * ### 实现
 *
 * 你或许应该在完成页面保护中的`CheckedReadPage`、`CheckedWritePage`和`Flush`之后，
 * 再着手实现此函数，因为这样可能会更容易理解需要做什么。
 *
 * TODO(P1)：添加实现
 *
 * @param page_id 要刷新的页面的页面ID。
 * @return 如果在页表中找不到该页面，则返回`false`；否则，返回`true`。
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock lock(*bpm_latch_);
  auto it = page_table_.find(page_id);
  std::shared_ptr<FrameHeader> frame;
  if (it != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    for (auto &item : frames_) {
      if (item->frame_id_ == frame_id) {
        frame = item;
        break;
      }
    }
    auto data = frame->data_.data();

    std::promise<bool> promise = disk_scheduler_->CreatePromise();

    auto task = std::make_optional(
        DiskRequest(false, data, page_id, std::move(promise)));
    disk_scheduler_->Write(task);
    frame->Reset();
    return true;
  }
  return false;
}

/**
 * @brief 不安全地将内存中的所有页面数据刷新到磁盘。
 *
 * 在此函数中不应对页面加锁。
 * 这意味着你需要仔细考虑何时切换`is_dirty_`位。
 *
 * ### 实现
 *
 * 你或许应该在完成`CheckedReadPage`、`CheckedWritePage`和`FlushPage`之后再着手实现此函数，因为这样可能会更容易理解该做什么。
 *
 * TODO(P1)：添加实现
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  for (auto &item : frames_) {
    if (item->is_dirty_) {
      page_id_t page_id;
      for (auto &i : page_table_) {
        if (i.second == item->frame_id_) {
          page_id = i.first;
          break;
        }
      }

      auto data = item->data_.data();

      std::promise<bool> promise = disk_scheduler_->CreatePromise();
      auto task = std::make_optional(
          DiskRequest(false, data, page_id, std::move(promise)));

      disk_scheduler_->Write(task);
      item->Reset();
    }
  }
}

/**
 * @brief 安全地将内存中的所有页数据刷新到磁盘。
 *
 * 你应该在此函数中对页进行加锁，以确保将一致的状态刷新到磁盘。
 *
 * ### 实现
 *
 * 你或许应该在完成`CheckedReadPage`、`CheckedWritePage`和`FlushPage`之后再着手实现此函数，因为这样可能会更容易理解需要做什么。
 *
 * TODO(P1)：添加实现
 */
void BufferPoolManager::FlushAllPages() {
  std::unique_lock lock(*bpm_latch_);
  for (auto &item : frames_) {
    if (item->is_dirty_) {
      page_id_t page_id;
      for (auto &i : page_table_) {
        if (i.second == item->frame_id_) {
          page_id = i.first;
          break;
        }
      }

      auto data = item->data_.data();

      std::promise<bool> promise = disk_scheduler_->CreatePromise();
      auto task = std::make_optional(
          DiskRequest(false, data, page_id, std::move(promise)));

      disk_scheduler_->Write(task);
      item->Reset();
    }
  }
}

/**
 * @brief 获取页面的钉住计数。如果该页面不存在于内存中，则返回`std::nullopt`。
 *
 * 此函数是线程安全的。调用者可以在多线程环境中调用此函数，其中多个线程会访问同一个页面。
 *
 * 此函数用于测试目的。如果此函数实现不正确，肯定会导致测试套件和自动评分程序出现问题。
 *
 * # 实现
 *
 * 我们将使用此函数来测试你的缓冲池管理器是否正确管理钉住计数。由于`FrameHeader`中的`pin_count_`字段是原子类型，你无需对持有我们要查看的页面的帧加锁。相反，你可以直接使用原子的`load`操作来安全地加载存储的值。不过，你仍然需要获取缓冲池的锁。
 *
 * 再次说明，如果你不熟悉原子类型，请参阅官方C++文档
 * [此处](https://en.cppreference.com/w/cpp/atomic/atomic)。
 *
 * TODO(P1)：添加实现
 *
 * @param page_id 我们想要获取其钉住计数的页面的页面ID。
 * @return std::optional<size_t>
 * 如果页面存在，则返回钉住计数；否则，返回`std::nullopt`。
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id)
    -> std::optional<size_t> {
  std::unique_lock lock(*bpm_latch_); // 修复：添加锁保护
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  auto item = page_table_[page_id];
  for (auto &i : frames_) {
    if (i->frame_id_ == item) {
      return std::make_optional(i->pin_count_.load());
    }
  }
  return std::nullopt;
}

// TODO(wwz): 查找操作替换成辅助函数
auto BufferPoolManager::GetFrameById(frame_id_t frame_id)
    -> std::shared_ptr<FrameHeader> {
  // 确保已经在内存里
  std::unique_lock lock(*bpm_latch_);
  for (auto &item : frames_) {
    if (item->frame_id_ == frame_id) {
      return item;
    }
  }

  return nullptr;
}

auto BufferPoolManager::NewPageById(page_id_t page_id) -> bool {
  std::unique_lock lock(*bpm_latch_);
  if (!free_frames_.empty()) {
    frame_id_t frame_id = free_frames_.front();
    free_frames_.pop_front();
    page_table_[page_id] = frame_id;

    for (auto &item : frames_) {
      if (item->frame_id_ == frame_id) {
        item->page_id_ = page_id;
        break;
      }
    }
    return true;
  }
  return false;
}

auto BufferPoolManager::Cut(frame_id_t frame_id) -> bool {
  std::unique_lock lock(*bpm_latch_);
  free_frames_.insert(free_frames_.begin(), frame_id);
  lock.unlock();

  for (auto it = frames_.begin(); it != frames_.end();) {
    if ((*it)->frame_id_ == frame_id) {
      FlushPage((*it)->page_id_); // 内部会获取bpm_latch_
      break;
    }
    it++;
  }

  std::unique_lock lock1(*bpm_latch_);
  for (auto it = page_table_.begin(); it != page_table_.end();) {
    if (it->second == frame_id) {
      page_table_.erase(it);
      break;
    }
    it++;
  }

  // Reset the frame to mark it as available for reuse
  for (auto it = frames_.begin(); it != frames_.end(); it++) {
    if ((*it)->frame_id_ == frame_id) {
      (*it)->Reset();
      (*it)->page_id_ = INVALID_PAGE_ID;
      break;
    }
  }

  return true;
}
} // namespace bustub