// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <mutex>
#include <optional>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames, BufferPoolManager *buffer_pool_manager)
    : replacer_size_(num_frames), buffer_pool_manager_(buffer_pool_manager) {}

auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  for (auto &item : mru_) {
    if (alive_map_[item]->evictable_) {
      return alive_map_[item]->frame_id_;
    }
  }
  for (auto &item : mfu_) {
    if (alive_map_[item]->evictable_) {
      return alive_map_[item]->frame_id_;
    }
  }
  return std::nullopt;
}

auto ArcReplacer::Cut(frame_id_t frame_id) -> bool {
  auto frame = alive_map_[frame_id];
  auto status = frame->arc_status_;
  if (status == ArcStatus::MFU) {
    for (auto it = mfu_.begin(); it != mfu_.end();) {
      if (*it == frame_id) {
        mfu_.erase(it);
        break;
      }
      it++;
    }
    mfu_ghost_.insert(mfu_ghost_.begin(), frame->page_id_);
  } else {
    for (auto it = mru_.begin(); it != mru_.end();) {
      if (*it == frame_id) {
        mru_.erase(it);
        break;
      }
      it++;
    }
    mru_ghost_.insert(mru_ghost_.begin(), frame->page_id_);
  }
  buffer_pool_manager_->free_frames_.insert(buffer_pool_manager_->free_frames_.begin(), frame_id);

  ghost_map_[frame->page_id_] = frame;
  for (auto it = alive_map_.begin(); it != alive_map_.end();) {
    if (it->first == frame_id) {
      it = alive_map_.erase(it);
      break;
    }
    it++;
  }

  return true;
}

void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  auto status = alive_map_[frame_id]->arc_status_;
  if (status == ArcStatus::MRU) {
    for (auto it = mru_.begin(); it != mru_.end();) {
      if (*it == frame_id) {
        mru_.erase(it);
        mfu_.insert(mfu_.begin(), frame_id);
        break;
      }
      it++;
    }
  } else if (status == ArcStatus::MFU) {
    for (auto it = mfu_.begin(); it != mfu_.end();) {
      if (*it == frame_id) {
        mfu_.erase(it);
        mfu_.insert(mfu_.begin(), frame_id);
        break;
      }
      it++;
    }
  } else if (status == ArcStatus::MRU_GHOST) {
    if (mru_ghost_.size() >= mfu_ghost_.size()) {
      mru_target_size_ += 1;
    } else {
      mru_target_size_ += std::floor(static_cast<float>(mfu_ghost_.size()) / static_cast<float>(mru_ghost_.size()));
    }
    mru_target_size_ = std::min(mru_target_size_, replacer_size_);
    for (auto it = mru_ghost_.begin(); it != mru_ghost_.end();) {
      if (*it == page_id) {
        mru_ghost_.erase(it);
        mfu_.insert(mfu_.begin(), ) break;
      }
    }
  } else {
    if (mfu_ghost_.size() >= mru_ghost_.size()) {
      mru_target_size_ -= 1;
    } else {
      mru_target_size_ -= std::floor(static_cast<float>(mru_ghost_.size()) / static_cast<float_t>(mfu_ghost_.size()));
    }
    mru_target_size_ = std::max(static_cast<size_t>(0), mru_target_size_);
    for (auto it = mru_ghost_.begin(); it != mru_ghost_.end();) {
      if (*it == page_id) {
        mru_ghost_.erase(it);
        mfu_.insert(mfu_.begin(), ) break;
      }
    }
  }
}

void ArcReplacer::SetEvictable(frame_id_t frame_id,
                               bool set_evictable) {  // TODO(wwz)申请写或者读权限时 应该调用这个函数 设置状态
  buffer_pool_manager_->FlushPage(alive_map_[frame_id]->page_id_);
  
  alive_map_[frame_id]->evictable_ = set_evictable;
}

void ArcReplacer::Remove(frame_id_t frame_id) {
  auto status = alive_map_[frame_id]->arc_status_;
  for (auto it = alive_map_.begin(); it != alive_map_.end();) {
    if (it->first == frame_id) {
      it = alive_map_.erase(it);
      break;
    }
    it++;
  }
  switch (status) {
    case ArcStatus::MFU:
      for (auto it = mfu_.begin(); it != mfu_.end();) {
        if (*it == frame_id) {
          mfu_.erase(it);
          break;
        }
        it++;
      }
      break;
    case ArcStatus::MRU:
      for (auto it = mru_.begin(); it != mru_.end();) {
        if (*it == frame_id) {
          mru_.erase(it);
          break;
        }
        it++;
      }
      break;
    case ArcStatus::MRU_GHOST:
    case ArcStatus::MFU_GHOST:
      break;
  }
  buffer_pool_manager_->free_frames_.insert(buffer_pool_manager_->free_frames_.begin(), frame_id);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
  size_t result = 0;
  std::unique_lock<std::mutex> lock(latch_);
  for (auto &item : mru_) {
    if (alive_map_[item]->evictable_) {
      result++;
    }
  }
  for (auto &item : mfu_) {
    if (alive_map_[item]->evictable_) {
      result++;
    }
  }
  return result;
}

}  // namespace bustub
