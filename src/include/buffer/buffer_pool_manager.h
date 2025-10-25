//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

// 引入标准库头文件：list用于维护空闲帧列表，memory用于智能指针，shared_mutex用于读写锁，
// unordered_map用于页表（页ID到帧ID的映射），vector用于存储帧头和帧数据
#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

// 引入BusTub项目内部头文件：ARC替换策略（缓存淘汰算法）、配置参数、日志管理器（恢复用）、
// 磁盘调度器（处理磁盘IO）、页结构（存储数据的基本单位）、页守卫（控制页的读写权限）
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

// 定义BusTub命名空间，避免与其他库的类/函数重名
namespace bustub {
// 前向声明：提前告诉编译器这些类存在，避免循环依赖（因为FrameHeader与这些类互相引用）
class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;
class ArcReplacer;
/**
 * @brief 为BufferPoolManager服务的辅助类，管理单个内存帧及其元数据
 *
 * 这个类相当于内存帧的"身份证+状态卡"：它不直接存储帧的原始数据，而是存储数据的指针和描述信息。
 * 类比现实：就像图书馆的"书架标签"——标签上记录了书架编号（帧ID）、当前放的书ID（页ID）、
 * 是否被借出（pin_count）、书是否被批注（is_dirty），但标签本身不是书（帧数据）。
 *
 * ---
 *
 * 关于data_字段用动态vector<char>而不是预分配内存指针的说明（帮助理解设计思路）：
 * 在传统数据库的缓冲池里，所有帧会提前分配成一大块连续内存（比如一次malloc几GB），
 * 帧就像这块内存里的"小格子"，通过偏移量找到对应帧（类似图书馆的连续书架）。
 * 但BusTub用vector<char>为每个帧单独分配内存，原因是：C++没有内存安全检查，
 * 如果帧是连续的，很容易因指针错误覆盖其他帧的数据（比如把A帧的指针强制转换后写超范围）；
 * 而单独分配的vector能让地址 sanitizer（内存检测工具）轻松发现这类错误，就像图书馆给每本书装了"防盗磁条"，
 * 防止误拿/误改其他书。
 */
class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

public:
  /**
   * @brief 构造函数：创建帧头时指定帧ID
   * 类比现实：图书馆新增一个书架，给它贴标签时先写好书架编号（frame_id）
   * @param frame_id 该帧头对应的帧ID（唯一标识一个内存帧）
   */
  explicit FrameHeader(frame_id_t frame_id);

private:
  /**
   * @brief 获取帧数据的只读指针
   * 类比现实：读者只能看书架上的书（不能改），所以管理员给一个"只读版本"的书内容指针
   * @return 指向帧数据的const char*（不能通过该指针修改数据）
   */
  [[nodiscard]] auto GetData() const -> const char *;

  /**
   * @brief 获取帧数据的可修改指针
   * 类比现实：管理员要修改书的内容（比如修正错误），所以拿到能修改的书内容指针
   * @return 指向帧数据的char*（可通过该指针修改数据）
   */
  auto GetDataMut() -> char *;

  /**
   * @brief 重置帧头的状态（清空元数据，不删除数据）
   * 类比现实：书架上的书被借走了，管理员把标签上的"当前书ID"、"借出次数"、"是否批注"等信息清空，
   * 但书架本身还在（帧数据会被后续新页覆盖）
   */
  void Reset();
  /** @brief 该帧头对应的帧ID（唯一标识，类似图书馆书架的编号） */
  const frame_id_t frame_id_;

  page_id_t page_id_;
  /** @brief 该帧的读写锁（控制并发访问）
   * 类比现实：书架前的"正在整理"牌子——多个读者可同时看（共享锁），
   * 但管理员修改时不允许读者看（排他锁）
   */
  std::shared_mutex rwlatch_;

  /** @brief 该帧的pin计数（表示当前有多少线程在使用这个帧，类似书的"借出次数"）
   * 用atomic确保并发修改安全（多个线程同时借/还书时，次数计算不出错）
   */
  std::atomic<size_t> pin_count_;

  /** @brief 脏页标记（表示帧数据是否被修改过，类似书是否被读者批注过）
   * 如果为true，淘汰该帧时需要写回磁盘（把批注后的书抄回仓库）；false则不用写回
   */
  bool is_dirty_;

  /**
   * @brief 指向该帧存储的页数据（实际存储数据的地方，类似书架上放的书）
   * 如果帧没存储任何页，data_里全是null字节（类似空书架）
   */
  std::vector<char> data_;

  /**
   * TODO(P1): 你可以在这里添加需要的字段或辅助函数
   * 一个潜在优化：存储当前帧对应的页ID（page_id），
   * 这样不用每次都去BufferPoolManager的页表里查"页ID->帧ID"映射，
   * 类比现实：书架标签上直接写当前放的书ID，管理员不用查总目录就能知道，提高效率
   */
};

/**
 * @brief BufferPoolManager类的声明（缓冲池管理器的核心类）
 *
 * 缓冲池的核心作用：把磁盘上的物理页（类似仓库里的书）和内存中的帧（类似图书馆的书架）做"搬运+缓存"：
 * 1. 当需要访问某页时，把磁盘上的页加载到内存帧（把书从仓库搬到书架）；
 * 2. 频繁访问的页留在内存（常用书放在书架），不常用的页淘汰回磁盘（冷门书放回仓库）；
 * 3. 管理并发访问和脏页写回（确保多读者/作者不冲突，批注过的书要写回仓库）。
 *
 * 注意：实现前需通读项目文档，且必须先完成ArcReplacer（缓存淘汰算法）和DiskManager（磁盘IO）的实现
 */
class BufferPoolManager {
public:
  /**
   * @brief 构造函数：初始化缓冲池管理器
   * 类比现实：创建一个图书馆，指定有多少个书架（num_frames）、
   * 仓库在哪里（disk_manager，存储原始书的地方）、
   * 日志记录员（log_manager，记录借书还书记录，P1阶段暂不用）
   * @param num_frames 缓冲池的总帧数（内存中能同时放多少页，类似图书馆书架总数）
   * @param disk_manager 磁盘管理器（负责与磁盘交互，类似图书馆的仓库管理员）
   * @param log_manager 日志管理器（负责记录操作日志，P1暂不用）
   */
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager = nullptr);

  /**
   * @brief 析构函数：释放缓冲池资源
   * 类比现实：图书馆关闭，清理所有书架标签、页表记录，确保所有脏页写回磁盘（把批注书抄回仓库）
   */
  ~BufferPoolManager();

  /**
   * @brief 获取缓冲池的总帧数（类似查询图书馆有多少个书架）
   * @return 缓冲池的总帧数（num_frames_）
   */
  auto Size() const -> size_t;

  /**
   * @brief 创建一个新的页（在磁盘和内存中分配空间）
   * 类比现实：图书馆新增一本书——先分配一个新的书ID，
   * 找一个空书架（空闲帧）或淘汰一本冷门书（用ARC算法选淘汰帧），
   * 把新书放到书架上，记录书ID和书架ID的对应关系（更新页表）。
   * @return 新页的page_id（如果创建失败返回INVALID_PAGE_ID，类似新书编号无效）
   */
  auto NewPage() -> page_id_t;

  /**
   * @brief 删除一个页（从磁盘和内存中移除）
   * 类比现实：图书馆下架一本书——先查这本书是否在书架上（页表找帧ID），
   * 如果在且没人借（pin_count=0），就把书架清空（重置帧头）、从页表删除记录、
   * 通知仓库删除这本书（磁盘删除页）；如果有人借或不在架上，删除失败。
   * @param page_id 要删除的页的ID（类似要下架的书ID）
   * @return 删除成功返回true，失败返回false
   */
  auto DeletePage(page_id_t page_id) -> bool;

  /**
   * @brief 尝试获取一个页的写权限（带检查，失败返回空）
   * 类比现实：读者想借一本书来批注（写权限），管理员先查书是否在架上（页表），
   * 不在就从仓库搬来（加载磁盘页到帧），然后检查是否能借（比如是否被其他人批注中），
   * 能借就增加借出次数（pin_count+1），给读者一个"可批注"的凭证（WritePageGuard）；
   * 不能借就返回空（比如书已被借走且没还）。
   * @param page_id 要写的页ID（类似要批注的书ID）
   * @param access_type 访问类型（暂时不用，保留参数）
   * @return 成功返回WritePageGuard（写权限凭证），失败返回std::optional空值
   */
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
    -> std::optional<WritePageGuard>;

  /**
   * @brief 尝试获取一个页的读权限（带检查，失败返回空）
   * 类比现实：读者想借一本书来看（读权限），管理员先查书是否在架上，
   * 不在就从仓库搬来，检查是否能借（比如是否被管理员整理中），
   * 能借就增加借出次数，给读者一个"可阅读"的凭证（ReadPageGuard）；不能借返回空。
   * @param page_id 要读的页ID（类似要读的书ID）
   * @param access_type 访问类型（暂时不用，保留参数）
   * @return 成功返回ReadPageGuard（读权限凭证），失败返回std::optional空值
   */
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;

  /**
   * @brief 获取一个页的写权限（不检查，失败会断言崩溃）
   * 功能与CheckedWritePage类似，但不处理失败情况——如果无法获取权限，程序直接崩溃（用于确保必须成功的场景）
   * 类比现实：管理员必须拿到某本书来修改，找不到就报错（而不是返回空）。
   * @param page_id 要写的页ID
   * @param access_type 访问类型（暂时不用）
   * @return 写权限凭证（WritePageGuard）
   */
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;

  /**
   * @brief 获取一个页的读权限（不检查，失败会断言崩溃）
   * 功能与CheckedReadPage类似，失败直接崩溃（用于必须成功的读场景）。
   * @param page_id 要读的页ID
   * @param access_type 访问类型（暂时不用）
   * @return 读权限凭证（ReadPageGuard）
   */
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;

  /**
   * @brief 强制刷新一个页到磁盘（不加缓冲池全局锁，unsafe表示需调用者自己保证线程安全）
   * 类比现实：管理员把书架上的批注书抄回仓库（写回磁盘），但不锁总目录（页表），
   * 需调用者确保此时没人修改页表（比如没人同时把这本书移到其他书架）。
   * @param page_id 要刷新的页ID
   * @return 刷新成功返回true（比如书确实在架上且抄完了），失败返回false（比如书不在架上）
   */
  auto FlushPageUnsafe(page_id_t page_id) -> bool;

  /**
   * @brief 强制刷新一个页到磁盘（加缓冲池全局锁，线程安全）
   * 类比现实：管理员先锁上总目录（页表），再把批注书抄回仓库，确保过程中没人动这本书的位置，线程安全。
   * @param page_id 要刷新的页ID
   * @return 刷新成功返回true，失败返回false
   */
  auto FlushPage(page_id_t page_id) -> bool;

  /**
   * @brief 刷新所有页到磁盘（不加固锁，unsafe）
   * 类比现实：管理员把所有书架上的批注书都抄回仓库，不锁总目录，需调用者保证线程安全。
   */
  void FlushAllPagesUnsafe();

  /**
   * @brief 刷新所有页到磁盘（加锁，线程安全）
   * 类比现实：管理员锁上总目录，把所有书架的批注书抄回仓库，确保过程安全。
   */
  void FlushAllPages();

  /**
   * @brief 获取某个页的pin计数（当前有多少线程在使用该页）
   * 类比现实：查询某本书当前被借出了多少次。
   * @param page_id 要查询的页ID
   * @return 成功返回pin_count（借出次数），失败返回std::optional空值（比如书不在架上）
   */
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;

  auto ExistsInTable(page_id_t page_id) -> bool;

  auto GetFrameById(frame_id_t frame_id) -> std::shared_ptr<FrameHeader>;

  auto NewPageById(page_id_t page_id) -> bool;

  bool Cut(frame_id_t frame_id);

private:
  /** @brief 缓冲池的总帧数（内存中能同时存储的页数量，类似图书馆的总书架数） */
  const size_t num_frames_;

  /** @brief 下一个要分配的页ID（自增生成，类似图书馆给新书编编号的计数器）
   * 用atomic确保并发分配时ID不重复（多个线程同时新增书时，编号不冲突）
   */
  std::atomic<page_id_t> next_page_id_;

  /**
   * @brief 保护缓冲池内部数据结构的互斥锁（全局锁）
   * 保护的对象包括：page_table_（页表）、free_frames_（空闲帧列表）、replacer_（淘汰器）
   * 类比现实：图书馆的总目录锁——有人查/改总目录时，其他人要等，防止目录信息混乱。
   * TODO(P1)：建议补充该锁具体保护哪些操作，比如"修改页表时加锁"、"从空闲帧列表取帧时加锁"等
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /** @brief 缓冲池管理的所有帧头（类似图书馆所有书架的标签集合）
   * 每个元素是FrameHeader的智能指针，管理帧的元数据（帧ID、pin计数、脏标记等）
   */
  std::vector<std::shared_ptr<FrameHeader>> frames_;

  /** @brief 页表（页ID到帧ID的映射，类似图书馆的"书ID->书架ID"总目录）
   * 作用：快速找到某页当前存在于内存中的哪个帧（通过书ID快速找书架）
   */
  std::unordered_map<page_id_t, frame_id_t> page_table_;

  /** @brief 空闲帧列表（当前没有存储任何页的帧ID集合，类似图书馆的空书架列表）
   * 当需要新帧时，优先从这里取（不用淘汰旧页，效率高）
   */
  std::list<frame_id_t> free_frames_;

  /** @brief 淘汰器（用ARC算法选择要淘汰的帧，类似图书馆的"冷门书筛选器"）
   * 当没有空闲帧时，通过淘汰器找到"最不常用"的未被使用帧（pin_count=0），淘汰它来腾出空间
   */
  std::shared_ptr<ArcReplacer> replacer_; // TODO(wwz) 试试根据gtest的测试样例 来写代码

  /** @brief A pointer to the disk scheduler. Shared with the page guards for flushing. */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief A pointer to the log manager.
   *
   * Note: Please ignore this for P1.
   */
  LogManager *log_manager_ __attribute__((__unused__));

  // 我们建议实现一个辅助函数，用于返回一个空闲且内部未存储任何内容的帧的
  // ID。此外，你可能还需要实现一个辅助函数，该函数要么返回一个已存储页面数据的FrameHeader的共享指针，要么返回该FrameHeader的索引。
};
} // namespace bustub