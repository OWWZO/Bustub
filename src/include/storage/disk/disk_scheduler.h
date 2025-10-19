//===----------------------------------------------------------------------===//
//
//                         BusTub
//  （BusTub是一个数据库系统框架，这段代码属于它的磁盘调度模块，负责协调磁盘的读写请求）
//
// disk_scheduler.h
//  （文件名：disk_scheduler.h，是磁盘调度器的头文件，用于声明磁盘调度相关的类和结构）
//
// Identification: src/include/storage/disk/disk_scheduler.h
//  （文件路径标识：在源代码的storage/disk目录下，属于存储层的磁盘模块）
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//  （版权信息：2015-2025年由卡内基梅隆大学数据库组所有）
//
//===----------------------------------------------------------------------===//

#pragma once  // （编译器指令：确保当前头文件只被编译一次，避免重复包含导致的语法错误，类似"#ifndef ... #define ...
              // #endif"的简化版）

// 引入依赖的头文件，提供后续代码需要的功能支持
#include <condition_variable>
#include <future>  // NOLINT （提供std::promise等异步编程相关类，用于请求完成后的回调通知；NOLINT是忽略编译器可能的警告）
#include <optional>  // （提供std::optional类，用于表示一个值"可能存在或不存在"，比如队列中可能没有请求，或线程可能未创建）
#include <thread>  // NOLINT （提供std::thread类，用于创建和管理后台工作线程；NOLINT忽略编译器警告）
#include <vector>  // （提供std::vector容器，用于存储多个磁盘请求）

#include "common/channel.h"  // （引入BusTub自定义的Channel类，这是一个线程安全的消息队列，用于在调度线程和后台线程间传递请求）
#include "common/config.h"
#include "storage/disk/disk_manager.h"  // （引入磁盘管理器头文件，DiskScheduler需要通过DiskManager实际操作磁盘）

namespace bustub {  // （属于bustub命名空间，避免与其他代码的类/函数名冲突，是C++中组织代码的常用方式）

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 *  （磁盘请求结构体：代表一个需要DiskManager执行的"读"或"写"请求，类比现实中的"快递单"——记录了快递的类型、内容、目的地等关键信息）
 */
struct DiskRequest {
  /** Flag indicating whether the request is a write or a read. */
  bool is_write_;  // （请求类型标识：true表示"写请求"，false表示"读请求"；类比快递单上的"寄件"或"收件"标记）

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   */
  char *data_;  // （数据缓冲区指针：指向内存中存储数据的起始位置，类比快递的"包裹存放地址"）
                // 读请求时：磁盘上的页面数据会被读取到这个内存地址
                // 写请求时：这个内存地址中的数据会被写入到磁盘

  /** ID of the page being read from / written to disk. */
  page_id_t
      page_id_;  // （页面ID：磁盘上的最小数据单位是"页面"，这个ID是页面在磁盘上的唯一标识；类比快递单上的"收件人地址"，明确数据要到磁盘的哪个位置）

  /** Callback used to signal to the request issuer when the request has been completed. */
  std::promise<bool> callback_;

  DiskRequest(bool is_write, char *data, page_id_t page_id) : is_write_(is_write), data_(data), page_id_(page_id) {}
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 *  （磁盘调度器类：负责调度磁盘的读/写操作，类比"快递站调度员"——接收快递单（请求），安排快递员（后台线程）按顺序处理，最终通过快递员（DiskManager）完成实际配送（磁盘操作））
 */
class DiskScheduler {
 public:
  /**
   * 构造函数：初始化DiskScheduler，需要传入一个DiskManager指针
   * 类比：创建快递站调度员时，必须先指定合作的快递员团队（DiskManager），否则调度员无法完成实际配送
   */
  explicit DiskScheduler(DiskManager *disk_manager);

  /**
   * 析构函数：释放DiskScheduler的资源，主要是停止后台线程并回收
   * 类比：快递站关闭时，调度员需要先让所有快递员（后台线程）停止工作，再结束自己的任务
   */
  ~DiskScheduler();
  /**
   * 调度请求函数：接收一个DiskRequest向量（多个请求），将其加入调度队列
   * 类比：快递站接收一批快递单（多个请求），调度员将这些快递单按顺序放进待处理队列，等待快递员处理
   */
  void Schedule(std::vector<DiskRequest> &requests);

  /**
   * 启动后台线程函数：创建并启动后台工作线程，用于处理队列中的请求
   * 类比：快递站开门后，调度员通知快递员（后台线程）开始工作，随时准备从队列中取快递单处理
   */
  void StartWorkerThread();

  void Write(std::optional<DiskRequest> &task);
  void Read(std::optional<DiskRequest> &task);
  /**
   * 类型别名：将std::promise<bool>简化为DiskSchedulerPromise，方便后续使用
   * 类比：给"快递签收通知单"起一个简称，比如"签收单"，后续提到"签收单"就知道指的是完整的通知单
   */
  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   *  （创建Promise对象的函数：默认返回一个std::promise<bool>，允许用户自定义Promise实现；类比：提供一个"创建签收单"的模板，默认是标准签收单，用户也可以自己设计签收单格式，只要能完成通知功能即可）
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

  /**
   * @brief Deallocates a page on disk.
   *
   * Note: You should look at the documentation for `DeletePage` in `BufferPoolManager` before using this method.
   *
   * @param page_id The page ID of the page to deallocate from disk.
   *  （释放磁盘页面函数：删除磁盘上指定page_id的页面，类比"快递站删除无效的收件地址"——当某个地址（页面）不再需要时，通过这个函数标记为无效，避免后续请求访问错误地址；实际操作由DiskManager完成，这里只是转发请求）
   */
  void DeallocatePage(page_id_t page_id) { disk_manager_->DeletePage(page_id); }

 private:
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((
      __unused__));  // （DiskManager指针：指向实际操作磁盘的对象，类比快递站的"快递员团队"；__attribute__((__unused__))是告诉编译器这个变量可能暂时未被使用，避免警告）
  /**
   * A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution.
   *  （请求队列：线程安全的Channel（消息队列），用于存储待处理的DiskRequest；类比快递站的"待处理快递单货架"——调度员（Schedule函数）往货架上放快递单，快递员（后台线程）从货架上取单处理；当快递站关闭（析构函数）时，往货架上放一个"停止信号"（std::nullopt），告诉快递员停止工作）
   */
  Channel<std::optional<DiskRequest>> request_queue_;

  /**
   * The background thread responsible for issuing scheduled requests to the disk manager.
   *  （后台线程：负责从请求队列中取请求，并通过DiskManager执行实际磁盘操作；类比快递站的"快递员"——一直循环从货架（request_queue_）取快递单，按单完成配送（磁盘读/写），直到收到停止信号；用std::optional是因为线程可能还没创建（比如构造函数中未启动），需要表示"线程可能不存在"的状态）
   */
  std::optional<std::thread> background_thread_;
};

}  // namespace bustub