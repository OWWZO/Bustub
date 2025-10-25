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
#include <iostream>
#include <iterator>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
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
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {
}

/*基于 p 参数决定淘汰优先级
根据p（MRU 的目标大小）与当前 MRU 实际大小的对比，决定优先淘汰 MRU 还是 MFU：
情况 1：当前 MRU 大小 > p → MRU 空间超出目标，优先淘汰 MRU
中最久未用的帧（evictable_mru.front()，因你的代码中evictable_mru是按 “最近使用” 倒序存储，front 是最久未用）； 情况
2：当前 MRU 大小 ≤ p → MRU 空间不足或达标，优先淘汰 MFU 中最少使用的帧（evictable_mfu.front()，同理，front
是最少使用）； 边界处理：若优先淘汰的列表为空（如 MRU 应优先淘汰，但evictable_mru为空），则淘汰另一列表的帧。*/
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::list<frame_id_t> evictable_mru;

  std::list<frame_id_t> evictable_mfu;

  // 两个for 将可淘汰的帧率加入
  for (auto it = mru_.begin(); it != mru_.end();) {
    if (alive_map_[*it]->evictable_) {
      evictable_mru.push_front(alive_map_[*it]->frame_id_);
    }
    it++;
  }

  for (auto it = mfu_.begin(); it != mfu_.end();) {
    if (alive_map_[*it]->evictable_) {
      evictable_mfu.push_front(alive_map_[*it]->frame_id_);
    }
    it++;
  }

  if (evictable_mfu.empty() && evictable_mru.empty()) {
    return std::nullopt;
  }

  // 选result的策略代码
  frame_id_t result;
  if ((mru_target_size_ > mru_.size() && !evictable_mfu.empty()) || evictable_mru.empty()) {
    result = evictable_mfu.front();
    evictable_mfu.pop_front();
  } else if (mru_target_size_ <= mru_.size() || evictable_mfu.empty()) {
    result = evictable_mru.front();
    evictable_mru.pop_front();
  }
  // 看所选的帧当前在哪个区
  auto status = alive_map_[result]->arc_status_;

  if (status == ArcStatus::MRU) {
    // 遍历 将所选帧加入ghost列表 再从活跃区删除
    for (auto it = mru_.begin(); it != mru_.end();) {
      if (*it == result) {
        mru_ghost_.insert(mru_ghost_.begin(), alive_map_[result]->page_id_);
        mru_.erase(it);
        break;
      }
      it++;
    }
  } else if (status == ArcStatus::MFU) {
    for (auto it = mfu_.begin(); it != mfu_.end();) {
      if (*it == result) {
        mfu_ghost_.insert(mfu_ghost_.begin(), alive_map_[result]->page_id_);
        mfu_.erase(it);
        break;
      }
      it++;
    }
  }
  // alive映射区删除 然后ghost区相对应的加入
  // TODO(wwz) 其实写注释 有利于思路的拓展 与bug的发现
  for (auto item = alive_map_.begin(); item != alive_map_.end();) {
    if (item->first == result) {
      ghost_map_[item->second->page_id_] = item->second;
      if (item->second->arc_status_ == ArcStatus::MFU) {
        item->second->arc_status_ = ArcStatus::MFU_GHOST;
      } else {
        item->second->arc_status_ = ArcStatus::MRU_GHOST;
      }
      alive_map_.erase(item);
      break;
    }
    item++;
  }
  return result;

  // if (evictable_mfu.empty() && evictable_mru.empty()) {
  //   return std::nullopt;
  // }
}

void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  if (ghost_map_.find(page_id) == ghost_map_.end() && alive_map_.find(frame_id) == alive_map_.end()) {
    auto frame = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);

    alive_map_[frame_id] = frame;

    mru_.insert(mru_.begin(), frame_id);
    // 超过总容量的2倍 要根据p参数来进行淘汰幽灵帧

    // 计算现有总数
    auto count = Size();

    for (auto item : mru_ghost_) {
      if (ghost_map_[item]->evictable_) {
        count += 1;
      }
    }

    for (auto item : mfu_ghost_) {
      if (ghost_map_[item]->evictable_) {
        count += 1;
      }
    }

    if (count >= replacer_size_ * 2) {
      if (!mru_ghost_.empty()) {
        for (auto it = ghost_map_.begin(); it != ghost_map_.end();) {
          if (mru_ghost_.back() == (*it).second->page_id_) {
            ghost_map_.erase(it);
            break;
          }
          it++;
        }
        // 抛弃末尾
        mru_ghost_.pop_back();
        return;
      }

      for (auto it = ghost_map_.begin(); it != ghost_map_.end();) {
        if (mfu_ghost_.back() == (*it).second->page_id_) {
          ghost_map_.erase(it);
          break;
        }
        it++;
      }
      // 抛弃末尾
      mfu_ghost_.pop_back();
      return;
    }
    // 如果mru_ghost 满了3 再插入新页 就要抛弃1个 但是mfu_ghost可以超设置的size

    // 清除要删除的帧的 ghost映射
    if (!mru_ghost_.empty() && count >= replacer_size_) {
      for (auto it = ghost_map_.begin(); it != ghost_map_.end();) {
        if (mru_ghost_.back() == (*it).second->page_id_) {
          ghost_map_.erase(it);
          break;
        }
        it++;
      }
      // 抛弃末尾
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
    for (auto it = mru_.begin(); it != mru_.end();) {
      if (*it == frame_id) {
        mru_.erase(it);
        mfu_.insert(mfu_.begin(), frame_id);
        alive_map_[frame_id]->arc_status_ = ArcStatus::MFU;
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
      mru_target_size_ = std::min(mru_target_size_ + 1, replacer_size_);
    } else {
      mru_target_size_ += std::floor(static_cast<float>(mfu_ghost_.size()) / static_cast<float>(mru_ghost_.size()));
    }
    mru_target_size_ = std::min(mru_target_size_, replacer_size_);

    for (auto it = mru_ghost_.begin(); it != mru_ghost_.end();) {
      if (*it == page_id) {
        // 从ghost区 取之前的数据 然后放进alive区 然后改一下frame_id
        alive_map_[frame_id] = ghost_map_[page_id];
        alive_map_[frame_id]->frame_id_ = frame_id;
        alive_map_[frame_id]->arc_status_ = ArcStatus::MFU;
        mru_ghost_.erase(it);
        mfu_.insert(mfu_.begin(), frame_id);

        break;
      }
      it++;
    }
  } else {
    if (mfu_ghost_.size() >= mru_ghost_.size()) {
      mru_target_size_ = std::min(mru_target_size_ - 1, replacer_size_);
    } else {
      mru_target_size_ -= std::floor(static_cast<float>(mru_ghost_.size()) / static_cast<float_t>(mfu_ghost_.size()));
    }
    mru_target_size_ = std::max(static_cast<size_t>(0), mru_target_size_);
    for (auto it = mfu_ghost_.begin(); it != mfu_ghost_.end();) {
      if (*it == page_id) {
        alive_map_[frame_id] = ghost_map_[page_id];
        alive_map_[frame_id]->frame_id_ = frame_id;

        mfu_ghost_.erase(it);
        mfu_.insert(mfu_.begin(), frame_id);
        break;
      }
      it++;
    }
  }
}

void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // TODO(wwz)申请写或者读权限时 应该调用这个函数 设置状态 还有其他调用时 都要set一下

  // TODO(wwz): 调用这个函数 一定要加上  FlushPage(alive_map_[frame_id]->page_id_);
  if (alive_map_.find(frame_id) != alive_map_.end())
    alive_map_[frame_id]->evictable_ = set_evictable;
}

void ArcReplacer::Remove(frame_id_t frame_id) {
  // TODO(wwz)buffer 调用这个函数 之后 一定加上
  // buffer_pool_manager_->free_frames_.insert(buffer_pool_manager_->free_frames_.begin(), frame_id);
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
} // namespace bustub