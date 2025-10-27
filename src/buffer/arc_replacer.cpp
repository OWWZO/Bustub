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


auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::unique_lock lock(latch_);
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
//处理无可淘汰情况
  if (evictable_mfu.empty() && evictable_mru.empty()) {
    return std::nullopt;
  }

  // 选result的策略代码 可以选出哪个最要被淘汰
  frame_id_t result = -1;
  if ((mru_target_size_ > mru_.size() && !evictable_mfu.empty()) || evictable_mru.empty()) {
    result = evictable_mfu.front();
    evictable_mfu.pop_front();
  } else if (mru_target_size_ <= mru_.size() || evictable_mfu.empty()) {
    result = evictable_mru.front();
    evictable_mru.pop_front();
  } else {
    return std::nullopt;
  }
  // 看所选的帧当前在哪个区
  auto status = alive_map_[result]->arc_status_;
  if (status == ArcStatus::MRU) {
    // 遍历 将所选帧加入ghost列表 再从活跃区删除
    for (auto it = mru_.begin(); it != mru_.end();) {
      if (*it == result) {
        mru_ghost_.insert(mru_ghost_.begin(),alive_map_[result]->page_id_);
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
  } else {
    return std::nullopt;
  }
  // alive映射区删除 然后ghost区相对应的加入
  for (auto item = alive_map_.begin(); item != alive_map_.end();) {
    if (item->first == result) {
      ghost_map_[item->second->page_id_] = item->second;//建立page_id 和帧状态的映射
      if (item->second->arc_status_ == ArcStatus::MFU) {
        item->second->arc_status_ = ArcStatus::MFU_GHOST;
      } else {
        item->second->arc_status_ = ArcStatus::MRU_GHOST;
      }
      item->second->evictable_=false;
      alive_map_.erase(item);
      break;
    }
    item++;
  }
  return result;
}
/*RecordAccess的 4 种处理场景
页在 MRU/MFU 中（缓存命中）：将页移到 MFU 列表前端；
页在 MRU 幽灵列表中（缓存未命中，幽灵命中）：
伪命中，调整 MRU 目标大小（MRU 幽灵列表大小≥MFU 幽灵列表则 + 1，否则 +「MFU 幽灵大小 / MRU 幽灵大小（向下取整）」，不超过替换器容量）；
将页移到 MFU 列表前端；
页在 MFU 幽灵列表中（缓存未命中，幽灵命中）：
伪命中，调整 MRU 目标大小（MFU 幽灵列表大小≥MRU 幽灵列表则 - 1，否则 -「MRU 幽灵大小 / MFU 幽灵大小（向下取整）」，不小于 0）；
将页移到 MFU 列表前端；
页不在替换器中（缓存、幽灵均未命中）：
a. 若MRU大小 + MRU幽灵大小 = 替换器容量：删除 MRU 幽灵列表最后 1 个元素，将页加入 MRU 前端；
b. 若MRU大小 + MRU幽灵大小 < 替换器容量：
若4个列表总大小 = 2*替换器容量：删除 MFU 幽灵列表最后 1 个元素，再将页加入 MRU 前端；
否则直接将页加入 MRU 前端。*/
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock lock(latch_);
  //新页处理逻辑
  if (ghost_map_.find(page_id) == ghost_map_.end() && alive_map_.find(frame_id) == alive_map_.end()) {//检查是否为新页
    // 计算现有总数
    auto count = Size();
    //处理mru mfu总数超的情况
    if (count==replacer_size_) {
      lock.unlock();
      auto id= Evict();//选出一个来淘汰
      lock.lock();
      if (!id.has_value()) {//处理淘汰不了情况
        return;
      }
    }
    //正常插入新页逻辑
    auto frame = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
    alive_map_[frame_id] = frame;
    mru_.insert(mru_.begin(), frame_id);

    // 如果超过总容量的2倍 要根据p参数来进行淘汰幽灵帧
    if (mru_.size()+mfu_.size()+mru_ghost_.size()+mfu_ghost_.size()-1 == replacer_size_ * 2) {
      if (!mfu_ghost_.empty()) {//清除mfu_ghost的末尾元素的ghost_map_
        for (auto it = ghost_map_.begin(); it != ghost_map_.end();) {
          if (mfu_ghost_.back() == (*it).second->page_id_) {
            ghost_map_.erase(it);
            break;
          }
          it++;
        }
        // 再抛弃末尾
        mfu_ghost_.pop_back();
        return;
      }
    }
    //处理mru mru_ghost总和最大为size的逻辑
     if (mru_.size()-1+mru_ghost_.size()==replacer_size_) {
       for (auto it = ghost_map_.begin(); it != ghost_map_.end();) {
         if (mru_ghost_.back() == (*it).second->page_id_) {
           ghost_map_.erase(it);
           break;
         }
         it++;
       }
       mru_ghost_.pop_back();
     }
    // // 处理mru mfu 超总数逻辑
    // if (!mru_ghost_.empty() && count == replacer_size_) {
    //   for (auto it = ghost_map_.begin(); it != ghost_map_.end();) {
    //     if (mru_ghost_.back() == (*it).second->page_id_) {
    //       ghost_map_.erase(it);
    //       break;
    //     }
    //     it++;
    //   }
    //   // 抛弃末尾
    //   mru_ghost_.pop_back();
    // }
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
      mru_target_size_ +=1;
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
        alive_map_[frame_id]->evictable_=false;
        mru_ghost_.erase(it);
        mfu_.insert(mfu_.begin(), frame_id);
        auto temp= ghost_map_.find(page_id);
        if (temp!=ghost_map_.end()) {
          ghost_map_.erase(temp);
        }
        break;
      }
      it++;
    }
  } else {
    if (mfu_ghost_.size() >= mru_ghost_.size()) {
      mru_target_size_ -=1;
    } else {
        mru_target_size_ -= std::floor(static_cast<float>(mru_ghost_.size()) / static_cast<float>(mfu_ghost_.size()));
    }
    mru_target_size_ = std::max(static_cast<size_t>(0), mru_target_size_);
    for (auto it = mfu_ghost_.begin(); it != mfu_ghost_.end();) {
      if (*it == page_id) {
        alive_map_[frame_id] = ghost_map_[page_id];
        alive_map_[frame_id]->frame_id_ = frame_id;
        alive_map_[frame_id]->arc_status_ = ArcStatus::MFU;
        mfu_ghost_.erase(it);
        mfu_.insert(mfu_.begin(), frame_id);
        auto temp= ghost_map_.find(page_id);
        if (temp!=ghost_map_.end()) {
          ghost_map_.erase(temp);
        }
        break;
      }
      it++;
    }
  }
}

void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // TODO(wwz)申请写或者读权限时 应该调用这个函数 设置状态 还有其他调用时 都要set一下

  // TODO(wwz): 调用这个函数 一定要加上  FlushPage(alive_map_[frame_id]->page_id_);
  std::unique_lock lock(latch_);
  if (alive_map_.find(frame_id) != alive_map_.end()) {
    alive_map_[frame_id]->evictable_ = set_evictable;
  }
}

void ArcReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock lock(latch_);
  // 修复：先检查frame_id是否存在，避免未定义行为
  if (alive_map_.find(frame_id) == alive_map_.end()) {
    return;
  }

  auto status = alive_map_[frame_id]->arc_status_;
  auto it = alive_map_.find(frame_id);
  if (it != alive_map_.end()) {
    alive_map_.erase(it);
  }
  switch (status) {
    case ArcStatus::MFU:
      for (auto iterator = mfu_.begin(); iterator != mfu_.end();) {
        if (*iterator == frame_id) {
          mfu_.erase(iterator);
          break;
        }
        iterator++;
      }
      break;
    case ArcStatus::MRU:
      for (auto iterator = mru_.begin(); iterator != mru_.end();) {
        if (*iterator == frame_id) {
          mru_.erase(iterator);
          break;
        }
        iterator++;
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