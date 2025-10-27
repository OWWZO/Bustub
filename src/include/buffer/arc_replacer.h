//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.h
//
// Identification: src/include/buffer/arc_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <queue>
#include <unordered_map>

// 引入BusTub项目的公共配置和宏定义（定义了页ID、帧ID等基础类型）

#include "common/config.h"
#include "common/macros.h"

// 定义BusTub命名空间，避免与其他库的类/函数重名
namespace bustub {
class BufferPoolManager;
/**
 * 访问类型枚举：标记页面是如何被访问的，用于后续替换策略的微调
 * - Unknown：未知访问类型
 * - Lookup：查询访问（比如按主键查数据）
 * - Scan：扫描访问（比如全表扫描）
 * - Index：索引访问（通过索引查找数据）
 * 类比：图书馆中读者拿书的目的——"查特定资料"（Lookup）、"逐页翻找"（Scan）、"按索引找书"（Index）
 */
enum class AccessType { Unknown = 0, Lookup, Scan, Index };

/**
 * ARC状态枚举：标记帧/页在ARC策略中的归属，是ARC的核心分类
 * - MRU：Most Recently Used（最近使用的帧）——属于"活跃帧"，刚被使用过
 * - MFU：Most Frequently Used（最常使用的帧）——属于"活跃帧"，近期被多次使用
 * - MRU_GHOST：MRU的"幽灵帧"——帧已被淘汰，但记录其历史（类似"已还书但记着谁借过"）
 * - MFU_GHOST：MFU的"幽灵帧"——同上，用于预测未来访问
 * 类比：图书馆的书架分类——"新书区（MRU，刚被借还的书）"、"热门区（MFU，常被借的书）"、
 *      "历史登记本（GHOST，记着已还的热门/新书，方便再采购）"
 */
enum class ArcStatus { MRU, MFU, MRU_GHOST, MFU_GHOST };

// TODO(student): You can modify or remove this struct as you like.
/**
 * 帧状态结构体：存储单个帧的核心信息，是ARC管理的"最小单元"
 * 类比：图书馆中每本书的"借阅卡"——记录书的ID（page_id）、存放位置（frame_id）、
 *      是否可借出（evictable）、当前在哪个区域（arc_status）
 */
struct FrameStatus {
  page_id_t page_id_; // 该帧对应的页ID（类似"书的ISBN号"，唯一标识一本书）
  frame_id_t frame_id_; // 帧在内存中的ID（类似"书架编号"，唯一标识存放位置）
  bool evictable_; // 该帧是否可被淘汰（类似"书是否允许借出"，true=可淘汰/可借出）
  ArcStatus arc_status_; // 该帧在ARC中的状态（属于MRU/MFU还是对应的幽灵区）

  // 构造函数：初始化帧状态（类似"新书入库时填写借阅卡"，明确书的ID、位置、是否可借、放哪个区）
  FrameStatus(page_id_t pid, frame_id_t fid, bool ev, ArcStatus st)
    : page_id_(pid), frame_id_(fid), evictable_(ev), arc_status_(st) {
  }
};

/**
 * ArcReplacer类：实现ARC（Adaptive Replacement Cache）缓存替换策略
 * ARC是一种"智能缓存"，会根据历史访问情况动态调整"最近使用"和"最常使用"的帧比例，
 * 同时通过"幽灵帧"记录已淘汰的帧，减少"缓存抖动"（频繁淘汰又重新加载）。
 * 类比：图书馆的"智能书架管理系统"——会根据读者借书记录，调整新书区（MRU）和热门区（MFU）的大小，
 *      同时记着之前被借走又还回的书（幽灵帧），如果读者再借，能快速找出来。
 */
class ArcReplacer {
  friend BufferPoolManager;

public:
  /**
   * 构造函数：初始化ARC替换器，指定最大可管理的帧数量
   * @param num_frames 缓存能容纳的最大帧数量（类似图书馆的"总书架容量"，最多能放多少本书）
   */
  explicit ArcReplacer(size_t num_frames);

  /**
   * 禁用拷贝和移动构造：避免多个ArcReplacer实例共享同一套缓存数据（类似图书馆不会有两个完全相同的"书架管理系统"，
   * 否则会出现重复管理或数据混乱）
   */
  DISALLOW_COPY_AND_MOVE(ArcReplacer);

  /**
   * TODO(P1): Add implementation
   * @brief 析构函数：销毁ArcReplacer实例
   * 默认实现（= default）即可，因为所有成员变量（list、unordered_map、mutex）都有自己的析构函数，
   * 会自动释放资源（类似图书馆关闭时，书架、借阅卡会自动清理，无需额外操作）
   */
  ~ArcReplacer() = default;

  /**
   *全锁
   * 内部逻辑：先将可淘汰的帧入2个数组 然后根据target_size动态的选出要淘汰的帧
   * 1.然后看选出的帧是哪个区 (mru区 加入mru_ghost 然后mru数组删除这个元素  mfu区一样的 最后就是else return nullopt)
   * 2.最后进行alive映射区删除 然后ghost区相对应的加入
   * (所有参数已覆盖)
   * 已处理特殊情况(1.无可淘汰帧 )
   */
  auto Evict() -> std::optional<frame_id_t>;


  /**
   *页被访问后 应该用这个函数来记录访问 更改replace里的参数 进而选出最佳淘汰人选
   *内部逻辑：
   *1.处理新页(已处理特殊情况：mru mfu总数超的情况) alive更新 mru插入
   *2.如果超过总容量的2倍 要根据p参数来进行淘汰幽灵帧
   *3.
   **/
  void RecordAccess(frame_id_t frame_id, page_id_t page_id, AccessType access_type = AccessType::Unknown);


  /**
   * 设置帧的可淘汰状态：标记某个帧是否允许被淘汰
   * @param frame_id 要设置的帧ID（类似"目标书的书架位置"）
   * @param set_evictable true=可淘汰（允许下架），false=不可淘汰（正在被使用，禁止下架）
   * 类比：图书馆中"标记一本书是否可借出"——如果书正在被读者阅读（不可淘汰），则不能下架；
   *      读者读完还回后（可淘汰），则可以在下架时被选中。

   setevictable的核心作用之一，就是给 “正在使用的帧” 打上false（不可淘汰）标签，强制Evict()在选择时跳过它们。
举例：当业务代码调用某帧的数据时，会先调用setevictable(frame_id, false)，标记该帧 “正在使用，禁止淘汰”；
当业务代码用完该帧（释放引用）后，再调用setevictable(frame_id, true)，标记该帧 “闲置，可纳入淘汰候选”。
如果没有setevictable，Evict()可能会误淘汰正在使用的帧，直接破坏系统正确性。
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * 移除指定帧：从ARC管理中删除某个帧（比如该帧对应的页被永久删除，不再需要缓存）
   * @param frame_id 要移除的帧ID（类似"图书馆剔除某本破损的书，彻底从书架和管理系统中删除"）
   * 注意：移除和淘汰的区别——移除是"主动删除"（书坏了），淘汰是"被动下架"（书架满了）。
   */
  void Remove(frame_id_t frame_id);

  /**
   * 获取当前可淘汰的帧数量（即处于MRU/MFU区且evictable=true的帧总数）
   * @return 可淘汰帧的数量（类似"图书馆当前可下架的书的数量"）
   */
  auto Size() -> size_t;

private:
  // TODO(student): implement me! You can replace or remove these member variables as you like.

  /**
   * 活跃帧列表：存储当前在缓存中的活跃帧ID，按访问顺序维护
   * - mru_：最近使用的活跃帧列表（类似"图书馆新书区的书架，按'最近被借还'排序，最前面是刚还的"）
   * - mfu_：最常使用的活跃帧列表（类似"图书馆热门区的书架，按'被借次数'排序，最前面是借的人最多的"）
   * 列表特性：支持快速在头部/尾部插入/删除（对应"最新加入"和"最早淘汰"）
   */

  std::list<frame_id_t> mru_;
  std::list<frame_id_t> mfu_;

  std::list<page_id_t> mru_ghost_;
  std::list<page_id_t> mfu_ghost_;

  std::unordered_map<frame_id_t, std::shared_ptr<FrameStatus>> alive_map_;

  std::unordered_map<page_id_t, std::shared_ptr<FrameStatus>> ghost_map_;

  /**
   * 当前可淘汰的活跃帧数量（即alive_map_中evictable=true的帧总数）
   * 类似"图书馆当前可下架的书的数量"，用于快速返回Size()函数的结果，避免每次遍历列表统计
   * [[maybe_unused]]：告诉编译器"这个变量可能暂时没被使用，但不要报警告"（学生实现时会用到）
   */
  size_t curr_size_{0};

  /**
   * MRU区的目标大小（对应ARC论文中的"p"参数）
   * 作用：动态调整mru_和mfu_列表的比例（比如mru_target_size_=3，表示MRU区最多放3个活跃帧，
   *      剩下的活跃帧放MFU区），根据幽灵帧的访问情况自动更新（类似"图书馆根据读者对新书和热门书的借阅比例，
   *      动态调整新书区和热门区的书架容量"）
   */
  size_t mru_target_size_{0}; // TODO(wwz) 大小做限制

  /**
   * ARC替换器的总容量（对应构造函数的num_frames，即缓存能容纳的最大帧数量）
   * 类似"图书馆的总书架容量"，是mru_和mfu_列表的总大小上限（活跃帧总数不能超过此值）
   */
   size_t replacer_size_;

  /**
   * 线程互斥锁：保证多线程下对ARC数据结构（list、map）的操作是线程安全的
   * 类似"图书馆的'单门进出'规则——同一时间只有一个管理员能操作书架或借阅卡，避免数据混乱"
   * 比如：线程A在Evict（淘汰帧），线程B不能同时RecordAccess（更新帧状态），必须等A操作完。
   */
  std::mutex latch_;

  // TODO(student): You can add member variables / functions as you like.
};
} // namespace bustub