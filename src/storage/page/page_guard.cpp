#include "storage/page/page_guard.h"
#include <memory>
#include <optional>
#include <utility>
#include "buffer/arc_replacer.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"

namespace bustub {
// 现实逻辑类比：
// 相当于图书馆的"图书阅读许可证"创建过程：
// 1. page_id 是你要借的书的唯一编号（比如《数据库原理》的编号是10086）
// 2. frame 是这本书实际存放的"书架格子"（包含了书的实体内容）
// 3. replacer 是图书馆的"图书整理员"（负责管理书架格子的占用/释放，比如旧书移到仓库）
// 4. bpm_latch 是图书馆的"前台取书锁"（确保同一时间只有一个人办理该书的借阅手续，避免冲突）
// 5. disk_scheduler 是图书馆的"仓库管理员"（如果书不在书架上，需要从仓库调过来）
// 构造函数的作用：拿着图书编号，找到对应的书架格子，让整理员、前台、仓库管理员做好准备，
// 最终给你发一张"阅读许可证"，只有有这张证，你才能读这本书，且保证阅读过程中数据不被篡改。

ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
  : page_id_(page_id),
    frame_(std::move(frame)),
    replacer_(std::move(replacer)),
    bpm_latch_(std::move(bpm_latch)),
    disk_scheduler_(std::move(disk_scheduler)) {
  lock_ = std::shared_lock<std::shared_mutex>(frame_->rwlatch_);
  replacer_->RecordAccess(frame_->frame_id_, page_id);

  is_valid_ = true;
}

// 相当于"图书阅读许可证"的转让：比如你临时有事，把许可证转让给同学A，转让后你手里的证失效，只有A的证有效。
// 1.
// that（原许可证持有者）的所有资源（page_id_、frame_、replacer_等）都"转移"到新对象（this）——相当于你把许可证和相关手续都给A
// 2. 标记that为无效（that.is_valid_ = false）——相当于你的许可证被注销，不能再用
// 3. 新对象（this）保持有效状态（is_valid_ = true）——相当于A的许可证生效
// 关键约束：不能同时有两张有效许可证对应同一本书，否则会出现"两个人同时改同一本书"的冲突（双重释放/锁竞争）
// 类比场景：你把《数据库原理》的阅读许可证给A后，你不能再去书架拿这本书，只有A能拿，且A的使用权限和你原来的一致

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { *this = std::move(that); }

// 相当于"图书阅读许可证"的二次转让+旧证回收：比如A已经有一本《操作系统》的许可证，现在要把你的《数据库原理》许可证转让给A。
//  1. 先处理A手里的旧许可证（this原来的资源）：如果A原来的许可证有效，先调用Drop()释放（比如把《操作系统》还回去）
//  2. 再执行和移动构造一样的逻辑：把你的许可证资源转移给A，标记你的无效、A的有效
//  类比场景：A不能同时持有两本有效图书的阅读许可证，所以要先还掉手里的，才能接过你的《数据库原理》许可证
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  this->Drop();
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->frame_ = std::move(that.frame_);
  this->disk_scheduler_ = std::move(that.disk_scheduler_);
  this->replacer_ = std::move(that.replacer_);
  this->lock_ = std::move(that.lock_); // 关键修复：移动锁对象
  this->page_id_ = that.page_id_; // TODO(wwz) 这个资源转移之后 被转移的对象要怎么处理？？
  this->is_valid_ = that.is_valid_;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.

// 现实逻辑类比：
// 相当于你拿着阅读许可证，查询你当前能读的书的编号（比如查看许可证上写的《数据库原理》编号10086）
// 前提：许可证必须有效（is_valid_为true）——如果许可证已经注销，就不能查编号了（会触发BUSTUB_ENSURE断言失败）
// 输入：无（基于当前Guard的状态）
// 输出：书的编号（page_id_）
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard"); // 断言：许可证必须有效，否则报错
  return page_id_; // 返回当前保护的页面ID（书的编号）
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.

// 现实逻辑类比：
// 相当于你拿着有效的阅读许可证，打开书架格子（frame），拿到书的内容（data），但只能读不能改（const指针）
// 1. 前提：许可证有效（is_valid_为true）——没证不能拿书
// 2. 操作：通过frame的GetData()获取数据指针——相当于从书架格子里取出书的内容
// 3. 约束：返回const指针——相当于书的内容是"只读"的，你不能在书上涂画
// 类比场景：有《数据库原理》的阅读许可证，才能翻开书看内容，但不能用笔在书上写字
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard"); // 断言：许可证必须有效
  return frame_->GetData(); // 从frame中获取只读数据指针（书的内容）
}

/**
// 现实逻辑类比：
// 相当于你拿着阅读许可证，查看这本书是否被人修改过但没放回仓库（比如有人用铅笔在书上做了笔记，但没交给仓库管理员归档）
// 1. 前提：许可证有效——没证不能检查书的状态
// 2. 操作：读取frame的is_dirty_标志——相当于查看书的"修改状态标签"
// 3. 输出：true（被修改过未归档）/ false（未修改或已归档）
// 类比场景：你借《数据库原理》时，检查书里是否有别人没清理的笔记（dirty=true），还是干净的（dirty=false）
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard"); // 断言：许可证必须有效
  return frame_->is_dirty_; // 返回页面是否为"脏页"（被修改未刷盘）
}

void ReadPageGuard::Flush() {
  if (IsDirty()) {
    std::promise<bool> promise = disk_scheduler_->CreatePromise();
    auto task = std::make_optional(DiskRequest(false, frame_->data_.data(), page_id_, std::move(promise)));
    // 3. 传递左值引用给Write函数
    disk_scheduler_->Write(task);
    frame_->is_dirty_ = false;
  }
}

void ReadPageGuard::Drop() {
  // 安全释放锁：检查是否持有锁，如果持有则释放
  if (lock_.owns_lock()) {
    lock_.unlock();
  }

  // 使用BPM锁保护对frame的修改，防止并发冲突
  if (bpm_latch_) {
    std::lock_guard<std::mutex> bpm_lock(*bpm_latch_);
    if (frame_ && frame_->pin_count_.load() != 0) {
      frame_->pin_count_.fetch_sub(1);
    }
    if (frame_) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  } else {
    // 如果没有bpm_latch_，直接执行（向后兼容）
    if (frame_ && frame_->pin_count_.load() != 0) {
      frame_->pin_count_.fetch_sub(1);
    }
    if (frame_) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
  is_valid_ = false;
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */

// 现实逻辑类比：
// 相当于图书馆的"自动还书机制"——如果你的阅读许可证到期（Guard对象生命周期结束），系统会自动调用Drop()完成还书流程
// 作用：防止"忘记还书"（内存泄漏/资源未释放），即使你没手动调用Drop()，析构函数也会自动处理
// 类比场景：你借的书到期了，图书馆系统自动把书收回，注销你的许可证，无需你手动还书
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**
 * @brief The move constructor for `WritePageGuard`.
 *        （翻译：`WritePageGuard`类的移动构造函数。移动构造函数的核心作用是“转移资源所有权”，而非“复制资源”，就像把别人手里的东西“拿过来自己用”，而不是再做一个一模一样的复制品）
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *        （翻译：如果不熟悉移动语义，请先查阅网上的学习资料。有很多优质资源（文章、微软教程、YouTube视频）会深入讲解这一概念）
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *        （翻译：务必将“另一个guard（被移动的对象）”设为“无效状态”；否则可能会出现“双重释放”问题！对于当前对象（接收资源的对象）和被移动的对象，各自至少需要更新5个成员变量）
 *
 * TODO(P1): Add implementation.
 *        （翻译：待办事项（优先级1）：补充实现代码）
 *
 * @param that The other page guard.
 *        （翻译：参数`that`：被移动的“另一个页保护对象”。可以类比为“你要从别人手里拿过来的那套工具”，`that`就是“别人手里的工具”）
 */
// 移动构造函数：参数是“右值引用”（&&that），表示`that`是一个“即将被销毁的临时对象”或“明确允许转移资源的对象”
// noexcept：表示这个构造函数不会抛出异常，这是移动构造函数的常见优化（因为转移资源通常是简单的指针赋值，不会出错）
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { *this = std::move(that); }
/**
 * @brief WritePageGuard（写页面守卫）类的移动赋值运算符
 *
 * 现实逻辑类比：
 * 把这个过程想象成"移交租房合同"——你（当前WritePageGuard对象）之前租了一套房（持有页面资源：frame指针、脏页状态等），
 * 现在要把租房合同完全移交给另一个人（that对象）。首先你得退掉自己原来的房（释放当前持有的资源，避免重复释放），
 * 然后把对方的租房信息（页面ID、frame指针、有效性标记等）全部拿过来，最后还要把对方的合同作废（将that的有效性设为false），
 * 确保对方不能再用这份合同操作房子（避免双重释放问题）。
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that 要移动的另一个WritePageGuard对象（右值，类似"待移交的合同"）
 * @return WritePageGuard& 移动后有效的当前对象（类似"持有新合同的自己"）
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  this->Drop();
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->frame_ = std::move(that.frame_);
  this->disk_scheduler_ = std::move(that.disk_scheduler_);
  this->replacer_ = std::move(that.replacer_);
  this->lock_ = std::move(that.lock_); // 关键修复：移动锁对象
  this->page_id_ = that.page_id_; // TODO(wwz) 这个资源转移之后 被转移的对象要怎么处理？？
  this->is_valid_ = that.is_valid_;
  return *this;
}

/**
 * @brief 获取当前守卫保护的页面的ID
 *
 * 现实逻辑类比：
 * 相当于你拿着租房合同（WritePageGuard对象），想知道自己租的房子门牌号（page_id），
 * 调用这个方法就会检查合同是否有效（没过期/没作废），如果有效就返回门牌号，无效就报错。
 *
 * @return page_id_t 页面的唯一标识（类似"房子门牌号"）
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  // 断言：如果当前守卫无效（合同作废），直接触发错误，避免无效操作
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_; // 返回存储的页面ID（门牌号）
}

/**
 * @brief 获取当前守卫保护的页面数据的常量指针（只读）
 *
 * 现实逻辑类比：
 * 相当于你拿着租房合同（WritePageGuard），想查看房子里的东西（页面数据），
 * 但只能看不能改（const指针）。首先会检查合同是否有效，有效就拿到房子的钥匙（frame指针），
 * 通过钥匙打开门（调用frame->GetData()）查看里面的东西，无效就报错。
 *
 * @return const char* 页面数据的只读指针（类似"房子里物品的只读查看权限"）
 */
auto WritePageGuard::GetData() const -> const char * {
  // 断言：无效守卫不能访问数据（作废的合同不能开门）
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  // 通过frame指针获取页面数据的只读地址（frame是"房子实体"，GetData()是"获取只读查看权限"）
  return frame_->GetData();
}

/**
 * @brief 获取当前守卫保护的页面数据的可变指针（可读写）
 *
 * 现实逻辑类比：
 * 相当于你拿着租房合同（WritePageGuard），不仅想查看房子里的东西（页面数据），
 * 还想改动（比如刷墙、换家具）。检查合同有效后，拿到能修改房子的钥匙（frame指针），
 * 通过钥匙获得修改权限（调用frame->GetDataMut()），无效合同则报错。
 *
 * @return char* 页面数据的可读写指针（类似"房子里物品的修改权限"）
 */
auto WritePageGuard::GetDataMut() -> char * {
  // 断言：无效守卫不能修改数据（作废的合同没有修改房子的权限）
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  // 通过frame指针获取页面数据的可写地址（GetDataMut()是"获取修改权限"）
  return frame_->GetDataMut();
}

/**
 * @brief 判断当前守卫保护的页面是否为脏页（被修改过但未写入磁盘）
 *
 * 现实逻辑类比：
 * 相当于你拿着租房合同（WritePageGuard），想知道房子里的东西是否被你改动过（比如换了窗帘），
 * 且改动还没告诉房东（没写入磁盘）。检查合同有效后，查看房子的"改动记录"（frame->is_dirty_），
 * 有改动就是"脏页"，没改动就是"干净页"。
 *
 * @return bool 若页面被修改且未刷盘则返回true（脏页），否则返回false（干净页）
 */
auto WritePageGuard::IsDirty() const -> bool {
  // 断言：无效守卫不能查询脏页状态（作废的合同不用关心房子是否被改动）
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  // 读取frame的脏页标记（frame是"房子实体"，is_dirty_是"房子改动记录"）
  return frame_->is_dirty_;
}

/**
 * @brief 将当前页面的数据安全地刷写到磁盘（持久化）
 *
 * 现实逻辑类比：
 * 相当于你改动了租来的房子（修改了页面数据），现在要把改动告诉房东，
 * 让房东更新房屋档案（将数据写入磁盘）。这个过程需要确保操作安全（比如先加锁防止并发修改），
 * 刷写完成后还要把房子的"改动记录"（脏页标记）清除，告诉系统"改动已存档"。
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Flush() {
  if (IsDirty()) {
    std::promise<bool> promise = disk_scheduler_->CreatePromise();
    auto task = std::make_optional(DiskRequest(false, frame_->data_.data(), page_id_, std::move(promise)));

    // 3. 传递左值引用给Write函数
    disk_scheduler_->Write(task);
    frame_->is_dirty_ = false;
  }
}

/*
 * @brief 手动释放一个有效的WritePageGuard持有的资源；若守卫无效，则不做任何操作
 *
 * 现实逻辑类比：
 * 相当于你提前退租（手动调用Drop），需要完成一系列流程：先确认合同有效（守卫有效），
 * 然后把房子里的改动报备给房东（如果是脏页则刷盘，类似"退租前修复改动"），
 * 再把房屋钥匙还给中介（将frame归还给缓冲池管理器，类似"钥匙回收"），
 * 最后作废自己的合同（标记守卫为无效，避免后续操作）。
 * 顺序很重要：如果先还钥匙再报备改动，会导致无法访问房子数据；如果不还钥匙，中介会以为房子还在租（资源泄漏）。
 */
void WritePageGuard::Drop() {
  if (lock_.owns_lock()) {
    lock_.unlock();
  }

  // 使用BPM锁保护对frame的修改，防止并发冲突
  if (bpm_latch_) {
    std::lock_guard<std::mutex> bpm_lock(*bpm_latch_);
    if (frame_ && frame_->pin_count_.load() != 0) {
      frame_->pin_count_.fetch_sub(1);
    }
    if (frame_) {
      replacer_->SetEvictable(frame_->frame_id_, true);
      frame_->is_dirty_=true;
    }
  } else {
    // 如果没有bpm_latch_，直接执行（向后兼容）
    if (frame_ && frame_->pin_count_.load() != 0) {
      frame_->pin_count_.fetch_sub(1);
    }
    if (frame_) {
      replacer_->SetEvictable(frame_->frame_id_, true);
      frame_->is_dirty_=true;
    }
  }
  is_valid_ = false;
}

WritePageGuard::~WritePageGuard() { Drop(); }

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * 【现实逻辑类比】：相当于"图书馆管理员准备让读者修改某本书"的初始化流程。
 *  - 这里的`WritePageGuard`就像"修改书籍的专属授权卡"，只有拿到这张卡，才能合法修改书籍；
 *  - 图书馆管理员（对应`buffer pool manager`）是唯一能发放这张卡的人，确保修改行为受管控；
 *  - 构造这个Guard的过程，就是管理员收集"修改书籍所需的全部工具和信息"，并封装到授权卡里的过程。
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 * 【现实逻辑类比】：就像只有图书馆管理员能发放"书籍修改授权卡"，普通读者（其他模块）不能自己造卡，
 *  避免未经允许的修改操作，保证书籍（内存页）的安全。
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 *  【现实逻辑类比】：要修改的书籍的唯一编号（比如图书馆里每本书的ISBN号），通过这个编号能精准定位到目标书籍。
 *  作用：告诉Guard"我们要修改的是哪一个具体的内存页"，是定位目标的核心标识。
 *
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 *  【现实逻辑类比】：存放目标书籍的"书架格子"（frame是内存池中的固定存储单元，相当于书架格子），
 *  这个格子里当前正放着我们要修改的那本书（内存页）。
 *  作用：Guard通过这个指针直接访问到"要修改的内存页所在的内存单元"，是操作的载体。
 *
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 *  【现实逻辑类比】：图书馆的"书籍替换规则管理器"（比如当书架满了，要按规则把不常用的书放回仓库）。
 *  作用：当当前要修改的内存页需要被替换出内存时，Guard会通过这个replacer执行替换逻辑，确保内存池的高效利用。
 *
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 *  【现实逻辑类比】：图书馆的"书架区域门锁"，用来控制对书架区域的访问（防止多人同时修改同一本书造成混乱）。
 *  作用：Guard持有这个锁的指针，在进行写操作时会锁定这个 latch，保证对内存页的写操作是线程安全的，避免数据竞争。
 *
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 *  【现实逻辑类比】：图书馆的"仓库存取调度员"（负责把书从仓库运到书架，或从书架运回仓库）。
 *  作用：当需要将修改后的内存页写回磁盘（相当于把修改后的书放回仓库），或从磁盘读取数据到内存时，
 *  Guard会通过这个disk_scheduler调度磁盘IO操作，是内存与磁盘数据交互的"调度中介"。
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
  : page_id_(page_id),
    frame_(std::move(frame)),
    replacer_(std::move(replacer)),
    bpm_latch_(std::move(bpm_latch)),
    disk_scheduler_(std::move(disk_scheduler)) {
  lock_ = std::unique_lock<std::shared_mutex>(frame_->rwlatch_);
  replacer_->RecordAccess(frame_->frame_id_, page_id);
  is_valid_ = true;
}
} // namespace bustub