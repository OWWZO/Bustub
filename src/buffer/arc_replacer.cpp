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
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/index_iterator.h"

namespace bustub {
/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::unique_lock lock(latch_);
  auto find_from_list_tail = [&](std::list<frame_id_t> &lst) -> std::optional<frame_id_t> {
    for (auto it = lst.rbegin(); it != lst.rend(); ++it) {
      auto a = alive_map_.find(*it);
      if (a != alive_map_.end() && a->second->evictable_) {
        return {*it};
      }
    }
    return std::nullopt;
  };

  // 选result的策略代码：加入“最后时刻双检”，避免刚被pin的帧被误淘汰
  frame_id_t result = -1;
  auto try_pick = [&]() -> bool {
    // 策略：若 MRU 实际大小 < 目标，优先从 MFU；否则优先 MRU
    bool prefer_mfu = (mru_.size() < mru_target_size_);
    std::optional<frame_id_t> pick;
    if (prefer_mfu) {
      pick = find_from_list_tail(mfu_);
      if (!pick.has_value()) {
        pick = find_from_list_tail(mru_);
      }
    } else {
      pick = find_from_list_tail(mru_);
      if (!pick.has_value()) {
        pick = find_from_list_tail(mfu_);
      }
    }
    if (!pick.has_value()) {
      return false;
    }
    result = pick.value();
    return true;
  };

  while (try_pick()) {
    auto it = alive_map_.find(result);
    if (it != alive_map_.end() && it->second->evictable_) {
      break;  // 双检通过，确认可以淘汰
    }
    result = -1;  // 双检失败，继续尝试下一候选
  }
  if (result == -1) {
    return std::nullopt;
  }
  // 看所选的帧当前在哪个区
  auto status = alive_map_[result]->arc_status_;
  if (status == ArcStatus::MRU) {
    // 使用迭代器映射快速定位并删除
    auto mru_it = mru_iter_map_.find(result);
    if (mru_it != mru_iter_map_.end()) {
      mru_ghost_.insert(mru_ghost_.begin(), alive_map_[result]->page_id_);
      mru_ghost_iter_map_[alive_map_[result]->page_id_] = mru_ghost_.begin();
      mru_.erase(mru_it->second);
      mru_iter_map_.erase(mru_it);
    }
  } else if (status == ArcStatus::MFU) {
    auto mfu_it = mfu_iter_map_.find(result);
    if (mfu_it != mfu_iter_map_.end()) {
      mfu_ghost_.insert(mfu_ghost_.begin(), alive_map_[result]->page_id_);
      mfu_ghost_iter_map_[alive_map_[result]->page_id_] = mfu_ghost_.begin();
      mfu_.erase(mfu_it->second);
      mfu_iter_map_.erase(mfu_it);
    }
  } else {
    return std::nullopt;
  }
  // alive映射区删除 然后ghost区相对应的加入
  auto alive_it = alive_map_.find(result);
  if (alive_it != alive_map_.end()) {
    ghost_map_[alive_it->second->page_id_] = alive_it->second;  // 建立page_id 和帧状态的映射
    if (alive_it->second->arc_status_ == ArcStatus::MFU) {
      alive_it->second->arc_status_ = ArcStatus::MFU_GHOST;
    } else {
      alive_it->second->arc_status_ = ArcStatus::MRU_GHOST;
    }
    // 如果该帧可淘汰，需要减少curr_size_
    if (alive_it->second->evictable_) {
      curr_size_--;
    }
    alive_it->second->evictable_ = false;
    alive_map_.erase(alive_it);
  }
  return result;
}

void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock lock(latch_);
  // 新页处理逻辑
  if (ghost_map_.find(page_id) == ghost_map_.end() && alive_map_.find(frame_id) == alive_map_.end()) {
    // 检查是否为新页
    // 使用实际占用（活跃帧数量）判断是否需要淘汰，避免在高并发下由于curr_size_
    // 统计“可淘汰数”而误判容量充足。
    if (mru_.size() + mfu_.size() == replacer_size_) {
      lock.unlock();
      auto id = Evict();  // 选出一个来淘汰
      lock.lock();
      if (!id.has_value()) {
        // 处理淘汰不了情况
        return;
      }
    }
    // 正常插入新页逻辑
    auto frame = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
    alive_map_[frame_id] = frame;
    mru_.insert(mru_.begin(), frame_id);
    mru_iter_map_[frame_id] = mru_.begin();

    // 如果超过总容量的2倍 要根据p参数来进行淘汰幽灵帧
    if (mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size() - 1 == replacer_size_ * 2) {
      if (!mfu_ghost_.empty()) {
        // 清除mfu_ghost的末尾元素的ghost_map_
        page_id_t back_page_id = mfu_ghost_.back();
        auto it = ghost_map_.find(back_page_id);
        if (it != ghost_map_.end()) {
          ghost_map_.erase(it);
        }
        // 清除迭代器映射并抛弃末尾
        mfu_ghost_iter_map_.erase(back_page_id);
        mfu_ghost_.pop_back();
        return;
      }
    }
    // 处理mru mru_ghost总和最大为size的逻辑
    if (mru_.size() + mru_ghost_.size() - 1 == replacer_size_) {
      page_id_t back_page_id = mru_ghost_.back();
      auto it = ghost_map_.find(back_page_id);
      if (it != ghost_map_.end()) {
        ghost_map_.erase(it);
      }
      mru_ghost_iter_map_.erase(back_page_id);
      mru_ghost_.pop_back();
    }
    return;
  }
  ArcStatus status;

  if (alive_map_.find(frame_id) != alive_map_.end()) {
    status = alive_map_[frame_id]->arc_status_;
  } else {
    status = ghost_map_[page_id]->arc_status_;
  }

  if (status == ArcStatus::MRU) {
    auto mru_it = mru_iter_map_.find(frame_id);
    if (mru_it != mru_iter_map_.end()) {
      mru_.erase(mru_it->second);
      mru_iter_map_.erase(mru_it);
      mfu_.insert(mfu_.begin(), frame_id);
      mfu_iter_map_[frame_id] = mfu_.begin();
      alive_map_[frame_id]->arc_status_ = ArcStatus::MFU;
    }
  } else if (status == ArcStatus::MFU) {
    auto mfu_it = mfu_iter_map_.find(frame_id);
    if (mfu_it != mfu_iter_map_.end()) {
      mfu_.erase(mfu_it->second);
      mfu_.insert(mfu_.begin(), frame_id);
      mfu_iter_map_[frame_id] = mfu_.begin();
    }
  } else if (status == ArcStatus::MRU_GHOST) {
    if (mru_ghost_.size() >= mfu_ghost_.size()) {
      mru_target_size_ += 1;
    } else {
      mru_target_size_ += std::floor(static_cast<float>(mfu_ghost_.size()) / static_cast<float>(mru_ghost_.size()));
    }
    mru_target_size_ = std::min(mru_target_size_, replacer_size_);

    // 从ghost区 取之前的数据 然后放进alive区 然后改一下frame_id
    auto ghost_list_it = mru_ghost_iter_map_.find(page_id);
    if (ghost_list_it != mru_ghost_iter_map_.end()) {
      // 仅在该frame当前不在alive状态时执行提升，避免误操作在用的frame
      if (alive_map_.find(frame_id) == alive_map_.end()) {
        auto ghost_it = ghost_map_.find(page_id);
        if (ghost_it != ghost_map_.end()) {
          alive_map_[frame_id] = ghost_it->second;
          alive_map_[frame_id]->frame_id_ = frame_id;
          alive_map_[frame_id]->arc_status_ = ArcStatus::MFU;
          alive_map_[frame_id]->evictable_ = false;
          ghost_map_.erase(ghost_it);
        }
        mru_ghost_.erase(ghost_list_it->second);
        mru_ghost_iter_map_.erase(ghost_list_it);
        mfu_.insert(mfu_.begin(), frame_id);
        mfu_iter_map_[frame_id] = mfu_.begin();
      }
    }
  } else {
    if (mfu_ghost_.size() >= mru_ghost_.size()) {
      if (mru_target_size_ > 0) {
        mru_target_size_ -= 1;
      }
    } else {
      if (mru_target_size_ -
              (std::floor(static_cast<float>(mru_ghost_.size()) / static_cast<float>(mfu_ghost_.size()))) >=
          0) {
        mru_target_size_ -= std::floor(static_cast<float>(mru_ghost_.size()) / static_cast<float>(mfu_ghost_.size()));
      } else {
        mru_target_size_ = 0;
      }
    }
    auto ghost_list_it = mfu_ghost_iter_map_.find(page_id);
    if (ghost_list_it != mfu_ghost_iter_map_.end()) {
      if (alive_map_.find(frame_id) == alive_map_.end()) {
        auto ghost_it = ghost_map_.find(page_id);
        if (ghost_it != ghost_map_.end()) {
          alive_map_[frame_id] = ghost_it->second;
          alive_map_[frame_id]->frame_id_ = frame_id;
          alive_map_[frame_id]->arc_status_ = ArcStatus::MFU;
          alive_map_[frame_id]->evictable_ = false;
          ghost_map_.erase(ghost_it);
        }
        mfu_ghost_.erase(ghost_list_it->second);
        mfu_ghost_iter_map_.erase(ghost_list_it);
        mfu_.insert(mfu_.begin(), frame_id);
        mfu_iter_map_[frame_id] = mfu_.begin();
      }
    }
  }
}

void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock lock(latch_);
  auto it = alive_map_.find(frame_id);
  if (it != alive_map_.end()) {
    bool old_evictable = it->second->evictable_;
    it->second->evictable_ = set_evictable;
    // 维护curr_size_计数
    if (old_evictable && !set_evictable) {
      curr_size_--;
    } else if (!old_evictable && set_evictable) {
      curr_size_++;
    }
  }
}

void ArcReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock lock(latch_);
  auto alive_it = alive_map_.find(frame_id);
  if (alive_it == alive_map_.end()) {
    return;
  }
  auto status = alive_it->second->arc_status_;
  // 如果该帧可淘汰，需要减少curr_size_
  if (alive_it->second->evictable_) {
    curr_size_--;
  }
  alive_map_.erase(alive_it);

  switch (status) {
    case ArcStatus::MFU: {
      auto mfu_iter_it = mfu_iter_map_.find(frame_id);
      if (mfu_iter_it != mfu_iter_map_.end()) {
        mfu_.erase(mfu_iter_it->second);
        mfu_iter_map_.erase(mfu_iter_it);
      }
      break;
    }
    case ArcStatus::MRU: {
      auto mru_iter_it = mru_iter_map_.find(frame_id);
      if (mru_iter_it != mru_iter_map_.end()) {
        mru_.erase(mru_iter_it->second);
        mru_iter_map_.erase(mru_iter_it);
      }
      break;
    }
    case ArcStatus::MRU_GHOST:
    case ArcStatus::MFU_GHOST:
      break;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
  std::unique_lock lock(latch_);
  return curr_size_;
}
}  // namespace bustub