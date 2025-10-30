//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.h
//
// Identification: src/include/storage/page/page_guard.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

// 引入缓冲区相关依赖：ARC替换策略（用于缓存淘汰）、缓冲池管理器（核心组件）
#include "buffer/arc_replacer.h"
#include "buffer/buffer_pool_manager.h"
// 引入磁盘调度器（负责磁盘读写）和页（数据存储单元）的定义
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"

namespace bustub
{
    // 前向声明：避免循环引用，告诉编译器这些类后续会定义
    class BufferPoolManager;
    class FrameHeader;
    class ArcReplacer;

    enum class AccessType;
    /**
 * @brief 一个RAII对象，提供对页数据的线程安全读访问。
 *
 * 类比现实场景：就像图书馆的"只读图书借阅证"——
 * 1. 你通过图书馆管理员（BufferPoolManager）拿到这张证（ReadPageGuard）；
 * 2. 持有证期间，你只能读这本书（页数据），不能涂改；
 * 3. 多人可以同时持有同本书的"只读借阅证"（多线程共享读权限）；
 * 4. 只要有一个人持有"只读借阅证"，就没人能拿到"可修改借阅证"（写操作被阻塞）；
 * 5. 你还书时（Guard析构），借阅证自动失效，权限释放。
 *
 * BusTub系统只能通过页守卫（PageGuard）与缓冲池的页数据交互。由于ReadPageGuard是RAII对象，
 * 系统无需手动加锁/解锁页的 latch（锁）。多个线程可共享同一页的读访问，但只要存在ReadPageGuard，
 * 就不允许任何线程修改该页数据。
 */
    class ReadPageGuard
    {
        /** @brief 只有缓冲池管理器（BufferPoolManager）能构造有效的ReadPageGuard。
   * 类比：只有图书馆管理员能发"只读借阅证"，读者自己不能造证。 */
        friend class BufferPoolManager;

    public:
        /**
   * @brief ReadPageGuard的默认构造函数。
   *
   * 注意：我们绝对不希望使用仅通过默认构造的守卫。定义默认构造的唯一目的，
   * 是允许在栈上创建一个"未初始化"的守卫，之后通过`=`进行移动赋值。
   *
   * **使用未初始化的页守卫属于未定义行为。**
   *
   * 类比：就像图书馆提供一张空白卡片（默认构造的Guard），你不能直接用空白卡片借书；
   * 但你可以先拿空白卡片，之后找管理员把有效借阅信息（移动赋值）写上去。
   *
   * 换句话说，获取有效ReadPageGuard的唯一方式是通过缓冲池管理器。
   */
        ReadPageGuard() = default;

        /** @brief 禁用拷贝构造。
   * 类比：你不能把自己的"只读借阅证"复印一份给别人——每张证对应唯一的读权限，不能共享。 */
        ReadPageGuard(const ReadPageGuard&) = delete;
        /** @brief 禁用拷贝赋值。
   * 类比：你不能把自己的借阅权限"复制"给另一张空白卡片，只能把自己的证"转移"给别人。 */
        auto operator=(const ReadPageGuard&) -> ReadPageGuard& = delete;
        /** @brief 移动构造函数（带 noexcept 保证不抛异常）。
   * 类比：你把自己的"只读借阅证"转给同学，转完后你手里的证失效，同学的证生效。 */
        ReadPageGuard(ReadPageGuard&& that) noexcept;
        /** @brief 移动赋值运算符（带 noexcept 保证）。
   * 类比：你手里有一张空白卡片（未初始化Guard），同学把他的有效借阅证转给你，
   * 转完后同学的证失效，你的空白卡片变成有效证。 */
        auto operator=(ReadPageGuard&& that) noexcept -> ReadPageGuard&;
        /** @brief 获取当前守卫保护的页的ID。
   * 类比：查看借阅证上写的"图书编号"（page_id）。 */
        auto GetPageId() const -> page_id_t;
        /** @brief 获取页的只读数据指针。
   * 类比：拿到借阅证后，凭证找到图书，只能看（读）书里的内容（数据），不能改。 */
        auto GetData() const -> const char*;
        /** @brief 模板方法：将页数据指针转换为指定类型的只读指针。
   * 类比：把一本"通用图书"（char*）当作"数学课本"（T*）来看，方便按学科（类型）解读内容。 */
        template <class T>
        auto As() const -> const T*
        {
            return reinterpret_cast<const T*>(GetData());
        }

        /** @brief 判断当前页是否为"脏页"（数据被修改过但未写入磁盘）。
   * 类比：检查你借的书是否有涂改痕迹（脏页）——只读证不能改，但可能之前有人用写证改过。 */
        auto IsDirty() const -> bool;

        /** @brief 将页的脏数据刷写到磁盘（若为脏页）。
   * 类比：如果书有涂改（脏页），你可以请求管理员把涂改后的内容"抄录"到图书馆的存档本（磁盘）里。 */
        void Flush();

        /** @brief 主动释放当前守卫的所有资源（提前析构）。
   * 类比：你提前还书，主动把借阅证交给管理员，证失效，读权限释放。 */
        void Drop();

        /** @brief 析构函数：RAII核心，自动释放资源。
   * 类比：借阅证到期自动失效，书被收回，其他读者可以申请借阅。 */
        ~ReadPageGuard();

    private:
        /** @brief 私有构造函数：只有缓冲池管理器能创建有效ReadPageGuard。
   * 类比：管理员在后台创建借阅证，读者看不到创建过程，只能通过前台领取。
   * @param page_id 要保护的页的ID（图书编号）
   * @param frame 存储该页的帧（相当于"书架上的书"，帧是缓冲池中的内存单元）
   * @param replacer 缓冲池的替换器（相当于图书馆的"藏书淘汰策略"，比如旧书优先下架）
   * @param bpm_latch 缓冲池的全局锁（相当于图书馆的"前台互斥锁"，防止多人同时抢一本书）
   * @param disk_scheduler 磁盘调度器（相当于图书馆的"存档室管理员"，负责读写存档本）
   */
        explicit ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer,
                               std::shared_ptr<std::mutex> bpm_latch, std::shared_ptr<DiskScheduler> disk_scheduler);

        /** @brief 当前守卫保护的页的ID。
   * 类比：借阅证上记录的"图书编号"，唯一标识一本书。 */
        page_id_t page_id_;

        /**
   * @brief 存储当前页的帧（FrameHeader）的智能指针。
   * 类比：指向"书架上那本书"的指针——通过这个指针能找到书的位置、内容、是否被涂改等信息。
   *
   * 页守卫的几乎所有操作都通过这个FrameHeader智能指针完成（比如读数据、判断脏页）。
   */
        std::shared_ptr<FrameHeader> frame_;

        /**
   * @brief 指向缓冲池替换器的智能指针。
   * 类比：指向图书馆"藏书淘汰策略手册"的指针——还书时（Guard析构），需要根据手册判断这本书是否可以下架（帧是否可淘汰）。
   *
   * 由于缓冲池不知道ReadPageGuard何时析构，守卫自身需要持有替换器指针，以便在析构时将帧标记为"可淘汰"。
   */
        std::shared_ptr<ArcReplacer> replacer_;

        /**
   * @brief 指向缓冲池全局锁的智能指针。
   * 类比：指向图书馆"前台互斥锁"的指针——还书时需要先锁前台（防止并发操作），再更新藏书状态。
   *
   * 由于缓冲池不知道ReadPageGuard何时析构，守卫自身需要持有缓冲池锁指针，以便在更新帧的淘汰状态时加锁保护。
   */
        std::shared_ptr<std::mutex> bpm_latch_;
        std::shared_lock<std::shared_mutex> lock_;
        /**
   * @brief 指向缓冲池磁盘调度器的智能指针。
   * 类比：指向图书馆"存档室管理员"的联系方式——需要刷写数据（抄录存档）时，通过这个指针联系管理员。
   *
   * 用于将页数据刷写到磁盘时调度磁盘操作。
   */
        std::shared_ptr<DiskScheduler> disk_scheduler_;

        /**
   * @brief 当前ReadPageGuard的有效性标志。
   * 类比：借阅证上的"有效/失效"印章——默认是失效（false），管理员创建时盖有效章（true）。
   *
   * 由于我们必须允许创建无效守卫（见默认构造函数文档），需要用这个标志判断守卫是否有效。
   * 默认构造函数会自动将该字段设为false。如果没有这个标志，移动构造/赋值时可能会操作无效成员，导致段错误。
   * （比如：试图给一张已经失效的借阅证执行"还书"操作，会导致逻辑错误）
   */
        bool is_valid_{false};

        /**
   * TODO(P1): 你可以在这里添加任何你认为必要的字段。
   *
   * 如果你想追求更好的代码风格（虽然没有额外分数），可以研究`std::shared_lock`类型，
   * 用它来管理latch（锁）机制，而不是手动调用lock和unlock。
   * 类比：用"自动开关的门"（std::shared_lock）代替"手动开关的门"（手动lock/unlock），更安全不易出错。
   */
    };

    /**
 * @brief 一个RAII对象，提供对页数据的线程安全写访问。
 *
 * 类比现实场景：就像图书馆的"唯一修改借阅证"——
 * 1. 你通过管理员拿到这张证（WritePageGuard），全场只有你能改这本书；
 * 2. 持有证期间，你可以随意涂改书的内容（写操作），其他人既不能读也不能改；
 * 3. 只要你没还书（Guard没析构），其他人拿不到任何类型的借阅证；
 * 4. 还书时，管理员会检查是否有涂改（脏页），如果有就抄录到存档本（刷盘），然后证失效。
 *
 * BusTub系统只能通过页守卫与缓冲池的页数据交互。由于WritePageGuard是RAII对象，
 * 系统无需手动加锁/解锁页的latch。同一时间只能有一个线程持有WritePageGuard（独占写权限），
 * 该线程可任意修改页数据，但只要存在WritePageGuard，就不允许其他线程持有同页的任何守卫（读或写）。
 */
    class WritePageGuard
    {
        /** @brief 只有缓冲池管理器能构造有效的WritePageGuard。
   * 类比：只有图书馆管理员能发"修改借阅证"，读者自己不能造证。 */
        friend class BufferPoolManager;

    public:
        /**
   * @brief WritePageGuard的默认构造函数。
   *
   * 注意：我们绝对不希望使用仅通过默认构造的守卫。定义默认构造的唯一目的，
   * 是允许在栈上创建一个"未初始化"的守卫，之后通过`=`进行移动赋值。
   *
   * **使用未初始化的页守卫属于未定义行为。**
   *
   * 类比：同ReadPageGuard的默认构造——空白卡片不能直接用，只能后续通过管理员赋值有效信息。
   *
   * 换句话说，获取有效WritePageGuard的唯一方式是通过缓冲池管理器。
   */
        WritePageGuard() = default;

        /** @brief 禁用拷贝构造。
   * 类比："修改借阅证"不能复印——如果两个人都能改同一本书，会导致内容冲突（比如同时改同一行字）。 */
        WritePageGuard(const WritePageGuard&) = delete;
        /** @brief 禁用拷贝赋值。
   * 类比：不能把修改权限复制给别人，只能转移权限。 */
        auto operator=(const WritePageGuard&) -> WritePageGuard& = delete;
        /** @brief 移动构造函数
   * 类比：你把"修改借阅证"转给同学，转完后你不能改了，同学可以改。 */
        WritePageGuard(WritePageGuard&& that) noexcept;
        /** @brief 移动赋值运算符（带 noexcept 保证）。
   * 类比：用空白卡片接收别人转移的修改权限，接收后空白卡片变成有效修改证。 */
        auto operator=(WritePageGuard&& that) noexcept -> WritePageGuard&;
        /** @brief 获取当前守卫保护的页的ID。
   * 类比：查看修改证上的"图书编号"。 */
        auto GetPageId() const -> page_id_t;
        /** @brief 获取页的只读数据指针（兼容读操作）。
   * 类比：持有修改证时，当然也能读这本书的内容。 */
        auto GetData() const -> const char*;
        /** @brief 模板方法：将页数据指针转换为指定类型的只读指针（兼容读操作）。
   * 类比：把修改中的书当作特定学科的课本来看。 */
        template <class T>
        auto As() const -> const T*
        {
            return reinterpret_cast<const T*>(GetData());
        }

        /** @brief 获取页的可修改数据指针（写操作核心）。
   * 类比：持有修改证时，拿到书的"可涂改版本"，可以直接在上面写字。 */
        auto GetDataMut() -> char*;
        /** @brief 模板方法：将页数据指针转换为指定类型的可修改指针（写操作核心）。
   * 类比：把可涂改的书当作"数学作业本"（T*），可以直接在上面写解题过程。 */
        template <class T>
        auto AsMut() -> T*
        {
            return reinterpret_cast<T*>(GetDataMut());
        }

        /** @brief 判断当前页是否为"脏页"（数据被修改过但未写入磁盘）。
   * 类比：检查你修改过的书是否有未存档的涂改痕迹。 */
        auto IsDirty() const -> bool;

        /** @brief 将页的脏数据刷写到磁盘（若为脏页）。
   * 类比：把你修改后的内容抄录到图书馆的存档本里，确保修改不会丢失。 */
        void Flush();

        /** @brief 主动释放当前守卫的所有资源（提前析构）。
   * 类比：提前还书，主动交回修改证，释放独占修改权限。 */
        void Drop();
        /** @brief 析构函数：RAII核心，自动释放资源。
   * 类比：修改证到期自动失效，书被收回并检查是否需要存档，其他人可以申请借阅。 */
        ~WritePageGuard();

    private:
        /** @brief 私有构造函数：只有缓冲池管理器能创建有效WritePageGuard。
   * 类比：管理员在后台创建修改证，读者只能通过前台领取，看不到创建过程。
   * 参数含义同ReadPageGuard的私有构造函数，区别是该构造函数会获取页的写锁（独占锁）。 */
        explicit WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                                std::shared_ptr<ArcReplacer> replacer,
                                std::shared_ptr<std::mutex> bpm_latch, std::shared_ptr<DiskScheduler> disk_scheduler);

        /** @brief 当前守卫保护的页的ID（图书编号）。 */
        page_id_t page_id_;

        /**
   * @brief 存储当前页的帧的智能指针（指向书架上的书）。
   * 写守卫的所有操作（读、写、判断脏页）都通过这个指针完成。
   */
        std::shared_ptr<FrameHeader> frame_;

        /**
   * @brief 指向缓冲池替换器的智能指针（藏书淘汰策略手册）。
   * 析构时需通过替换器将帧标记为"可淘汰"（还书后允许下架）。
   */
        std::shared_ptr<ArcReplacer> replacer_;

        /**
   * @brief 指向缓冲池全局锁的智能指针（前台互斥锁）。
   * 更新帧状态时需加锁，防止并发冲突。
   */
        std::shared_ptr<std::mutex> bpm_latch_;

        std::unique_lock<std::shared_mutex> lock_;
        /**
   * @brief 指向缓冲池磁盘调度器的智能指针（存档室管理员联系方式）。
   * 刷写脏页时调度磁盘操作，将修改写入存档。
   */
        std::shared_ptr<DiskScheduler> disk_scheduler_;

        /**
   * @brief 当前WritePageGuard的有效性标志（修改证的有效/失效印章）。
   * 默认false（无效），构造时设为true（有效），防止操作无效守卫导致错误。
   */
        bool is_valid_{false};

        /**
如果你想获得额外的（并非必需的）风格加分，并且想做得更精致一些，那么你可以研究一下
std::unique_lock 类型，并使用它来实现闭锁机制，而不是手动调用 lock 和 unlock。
*/
    };
} // namespace bustub