//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_concurrent_test.cpp
//
// Identification: test/storage/b_plus_tree_concurrent_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>  // NOLINT
#include <cstdio>
#include <filesystem>
#include <functional>
#include <future>  // NOLINT
#include <thread>  // NOLINT

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

// helper function to launch multiple threads

// helper function to insert
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  // 定义一个线程向量，用于存储所有创建的工作线程
  // 类比：这是一个"工人名单表"，记录所有参与任务的工人信息
  std::vector<std::thread> thread_group;

  // 启动一组线程（为每个线程分配任务）
  // 类比：根据"工人数（num_threads）"逐个招聘工人，给每个工人分配具体工作（args）和唯一编号（thread_itr）
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    // 向线程向量中添加一个新线程，线程执行的任务由args指定，同时传入当前线程的编号thread_itr
    // 类比：招聘一个新工人，把任务内容（args）和工人编号（thread_itr）交给TA，然后将工人加入"工人名单表"
    // args...是可变参数展开，比如如果args是InsertHelper和树对象、键列表，就会让线程执行InsertHelper(树, 键列表, thread_itr)
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // 将所有线程与主线程合并（等待所有线程执行完毕）
  // 类比：工厂老板等待所有工人（子线程）都完成自己的任务，才继续进行后续的总装或检查工作
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    // 调用线程的join()方法，主线程会阻塞在这里，直到当前子线程执行完成
    // 类比：老板找到"工人名单表"中的第thread_itr个工人，等待TA把手里的活干完
    thread_group[thread_itr].join();
  }
}

// 模板函数：B+树插入辅助函数（非分片版本）
// 模板参数Tombs：B+树中" Tombstone（墓碑）"的数量（用于标记已删除但未清理的节点）
// 功能：将传入的所有键（keys）逐个插入到指定的B+树中
// 类比：好比仓库管理员（该函数）要把一批货物（keys中的每个key）逐个放到货架（B+树）上，
// 每个货物需要先包装成货架能识别的格式（转换为GenericKey和RID），再放到对应位置
template <ssize_t Tombs>
void InsertHelper(
    // 指向目标B+树的指针（要插入数据的货架）
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> *tree,
    // 存储待插入键的向量（要放入货架的所有货物编号）
    const std::vector<int64_t> &keys,
    // 线程编号（__attribute__((unused))表示该参数可能未使用，编译器不会报警）
    __attribute__((unused)) uint64_t thread_itr = 0) {
  // 定义B+树的索引键对象（货架识别货物的专属标签格式）
  GenericKey<8> index_key;
  // 定义RID对象（记录货物在仓库中的具体位置，类似"货架层号+货架格号"）
  RID rid;

  // 遍历所有待插入的键（逐个处理每个货物）
  for (auto key : keys) {
    // 从key中提取低32位作为RID的"格号"（value）
    // 类比：从货物编号（key）中截取后几位作为货物在货架层内的具体格子号
    int64_t value = key & 0xFFFFFFFF;
    // 从key中提取高32位作为RID的"层号"（page_id），并设置到RID对象中
    // 类比：从货物编号（key）中截取前几位作为货架的层号，然后把"层号+格号"写入位置标签（rid）
    rid.Set(static_cast<int32_t>(key >> 32), value);
    // 将原始key转换为B+树索引键的格式（把货物编号转换成货架能识别的标签）
    index_key.SetFromInteger(key);
    // 调用B+树的Insert方法，将索引键和对应的RID插入到树中（把货物按标签放到货架的对应位置）
    tree->Insert(index_key, rid);
  }
}

// 模板函数：B+树插入辅助函数（分片版本）
// 与InsertHelper的区别：会根据线程总数和线程编号，只处理属于当前线程的键（避免线程处理重复数据）
// 类比：仓库有多个管理员（多个线程）共同放货，提前约定好"每个管理员只处理编号模工人数等于自己编号的货物"，
// 比如3个管理员，管理员0处理编号%3==0的货物，管理员1处理%3==1的货物，避免抢活或漏活
template <ssize_t Tombs>
void InsertHelperSplit(
    // 指向目标B+树的指针（要插入数据的货架）
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> *tree,
    // 存储所有待插入键的向量（所有要放入货架的货物编号）
    const std::vector<int64_t> &keys,
    // 参与插入任务的总线程数（仓库管理员的总人数）
    int total_threads,
    // 当前线程的编号（当前管理员的专属编号）
    __attribute__((unused)) uint64_t thread_itr) {
  // 定义B+树的索引键对象（货架识别货物的专属标签格式）
  GenericKey<8> index_key;
  // 定义RID对象（记录货物在仓库中的具体位置）
  RID rid;

  // 遍历所有待插入的键（逐个检查每个货物是否属于自己的处理范围）
  for (auto key : keys) {
    // 判断当前key是否属于当前线程的处理范围：key的无符号值模总线程数，等于当前线程编号
    // 类比：检查货物编号%管理员总数是否等于自己的编号，是则处理，否则跳过
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      // 以下逻辑与InsertHelper完全一致：提取RID信息、转换索引键、插入B+树
      // 类比：处理属于自己的货物，包装成货架格式后放到对应位置
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid);
    }
  }
}

// 模板函数：B+树删除辅助函数（非分片版本）
// 功能：将传入的所有待删除键（remove_keys）逐个从B+树中删除
// 类比：仓库管理员要把一批过期货物（remove_keys中的每个key）逐个从货架（B+树）上取下，
// 先根据货物编号找到对应的货架标签（转换为GenericKey），再找到货物位置并删除
template <ssize_t Tombs>
void DeleteHelper(
    // 指向目标B+树的指针（要删除数据的货架）
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> *tree,
    // 存储待删除键的向量（要从货架取下的过期货物编号）
    const std::vector<int64_t> &remove_keys,
    // 线程编号（未使用，编译器不报警）
    __attribute__((unused)) uint64_t thread_itr = 0) {
  // 定义B+树的索引键对象（货架识别货物的专属标签格式）
  GenericKey<8> index_key;

  // 遍历所有待删除的键（逐个处理每个过期货物）
  for (auto key : remove_keys) {
    // 将原始key转换为B+树索引键的格式（把货物编号转换成货架能识别的标签）
    index_key.SetFromInteger(key);
    // 调用B+树的Remove方法，根据索引键删除对应的节点（从货架上取下对应标签的货物）
    tree->Remove(index_key);
  }
}

// 模板函数：B+树删除辅助函数（分片版本）
// 与DeleteHelper的区别：按线程总数和线程编号分片处理，只删除属于当前线程的键
// 类比：多个管理员共同清理过期货物，每个管理员只处理编号模工人数等于自己编号的货物，分工协作
template <ssize_t Tombs>
void DeleteHelperSplit(
    // 指向目标B+树的指针（要删除数据的货架）
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> *tree,
    // 存储所有待删除键的向量（所有要清理的过期货物编号）
    const std::vector<int64_t> &remove_keys,
    // 参与删除任务的总线程数（管理员总人数）
    int total_threads,
    // 当前线程的编号（当前管理员的专属编号）
    __attribute__((unused)) uint64_t thread_itr) {
  // 定义B+树的索引键对象（货架识别货物的专属标签格式）
  GenericKey<8> index_key;

  // 遍历所有待删除的键（逐个检查是否属于自己的处理范围）
  for (auto key : remove_keys) {
    // 判断当前key是否属于当前线程的处理范围（货物编号%管理员总数 == 自己的编号）
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      // 转换索引键格式，并调用Remove方法删除对应节点（处理属于自己的过期货物）
      index_key.SetFromInteger(key);
      tree->Remove(index_key);
    }
  }
}

// 模板函数：B+树查找辅助函数（非分片版本）
// 功能：检查传入的所有键（keys）是否存在于B+树中，且对应的RID是否正确（通过断言验证）
// 类比：仓库管理员要核对一批货物（keys中的每个key）是否在货架上，且位置是否正确，
// 找到货物后要检查"位置标签（RID）"是否与预期一致，不一致则报错（断言失败）
template <ssize_t Tombs>
void LookupHelper(
    // 指向目标B+树的指针（要核对货物的货架）
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> *tree,
    // 存储待查找键的向量（要核对的货物编号）
    const std::vector<int64_t> &keys,
    // 线程编号（未使用，编译器不报警）
    __attribute__((unused)) uint64_t thread_itr = 0) {
  // 定义B+树的索引键对象（货架识别货物的专属标签格式）
  GenericKey<8> index_key;
  // 定义RID对象（预期的货物位置标签）
  RID rid;
  // 遍历所有待查找的键（逐个核对每个货物）
  for (auto key : keys) {
    // 从key中提取低32位作为预期RID的"格号"
    // 类比：根据货物编号计算出预期的货架格号
    int64_t value = key & 0xFFFFFFFF;
    // 从key中提取高32位作为预期RID的"层号"，设置到预期RID对象中
    // 类比：根据货物编号计算出预期的货架层号，生成预期的位置标签
    rid.Set(static_cast<int32_t>(key >> 32), value);
    // 将原始key转换为B+树索引键的格式（生成货架能识别的货物标签）
    index_key.SetFromInteger(key);
    // 定义结果向量，用于存储B+树查找返回的RID（实际找到的货物位置标签）
    std::vector<RID> result;
    // 调用B+树的GetValue方法查找键，返回值res表示是否找到
    bool res = tree->GetValue(index_key, &result);

    // 断言1：查找结果必须为true（必须找到对应的货物，否则断言失败，程序报错）
    // 类比：核对时必须找到货物，没找到则说明货架管理出错
    ASSERT_EQ(res, true);
    // 断言2：查找返回的RID数量必须为1（每个键对应一个唯一的货物位置，否则报错）
    // 类比：一个货物编号只能对应一个货架位置，找到多个或零个都说明出错
    ASSERT_EQ(result.size(), 1);
    // 断言3：找到的RID必须与预期的RID一致（实际位置和预期位置必须相同，否则报错）
    // 类比：实际找到的货物位置必须和预期位置一致，否则说明货物放错位置
    ASSERT_EQ(result[0], rid);
  }
}

// 定义测试的核心常量，类比现实场景：相当于规定"快递分拣测试"要重复的次数、包裹暂存区大小等基础规则
// 1. 普通测试迭代次数：50次（比如同一套快递分拣流程重复测试50遍，确保稳定性）
const size_t NUM_ITERS = 50;
// 2. 混合测试迭代次数：20次（比如更复杂的快递分拣+派送混合流程测试20遍，此处代码未直接使用，预留扩展）
const size_t MIXTEST_NUM_ITERS = 20;
// 3. 缓冲池管理器（BPM）的页面数量：50个（相当于快递站的"临时货架"有50个格子，每个格子存一个快递包裹箱）
static const size_t BPM_SIZE = 50;

// 模板函数：InsertTest1Call（插入测试1），Tombs参数控制是否支持"墓碑标记"（类比快递是否支持"暂存标记"）
// 功能：测试B+树的基础插入功能，验证插入后数据的正确性
template <ssize_t Tombs>
void InsertTest1Call() {
  // 循环NUM_ITERS（50）次，重复执行测试（类比同一套快递分拣流程重复测50遍，排除偶然错误）
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // 1. 创建键比较器和索引结构描述（类比定义"快递分拣规则"：按快递单号（bigint类型）排序）
    // ParseCreateStatement("a bigint")：解析SQL语句，确定索引的键是"名为a的长整型字段"
    auto key_schema = ParseCreateStatement("a bigint");
    // GenericComparator<8>：创建比较器，8表示键的长度（8字节），用于比较两个快递单号的大小
    GenericComparator<8> comparator(key_schema.get());

    // 2. 创建磁盘管理器（类比快递站的"仓库管理员"）
    // DiskManagerUnlimitedMemory：内存无限的磁盘管理器，相当于仓库有无限存储空间，不用考虑磁盘满的问题
    auto *disk_manager = new DiskManagerUnlimitedMemory();
    // 3. 创建缓冲池管理器（类比快递站的"临时货架"）
    // BPM_SIZE（50）：货架有50个格子；disk_manager：货架的货物最终由仓库管理员（磁盘管理器）管理
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // 4. 创建B+树的"头页面"（类比为快递分拣系统的"总控台页面"）
    // bpm->NewPage()：从临时货架（BPM）申请一个新格子（页面），作为总控台的载体，返回页面ID（格子编号）
    page_id_t page_id = bpm->NewPage();

    // 5. 创建B+树实例（类比搭建完整的"快递分拣系统"）
    // 参数说明：
    // - "foo_pk"：树的名称（类比分拣系统的名称，比如"foo快递分拣线"）
    // - page_id：总控台页面的编号（分拣系统总控台在货架的哪个格子）
    // - bpm：临时货架（BPM），分拣过程中临时存放快递
    // - comparator：快递单号比较规则（按单号排序）
    // - 3：B+树非叶子节点的最小子节点数（类比分拣节点最少要管理3个下级分拣口）
    // - 5：B+树非叶子节点的最大子节点数（类比分拣节点最多管理5个下级分拣口）
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> tree("foo_pk", page_id, bpm, comparator, 3, 5);

    // 6. 准备要插入的键（类比准备100个快递单号，编号从1到99）
    std::vector<int64_t> keys;  // 存储快递单号的列表
    int64_t scale_factor = 100; // 要插入的快递数量（100个，编号1~99）
    for (int64_t key = 1; key < scale_factor; key++) {
      keys.push_back(key); // 把单号1到99依次加入列表
    }

    // 7. 启动并行测试插入数据（类比安排2个快递员同时往分拣系统里放快递）
    // LaunchParallelTest：并行测试工具，参数说明：
    // - 2：并行线程数（2个快递员）
    // - InsertHelper<Tombs>：插入数据的辅助函数（快递员放快递的操作流程）
    // - &tree：目标B+树（要放快递的分拣系统）
    // - keys：要插入的键（要放的快递单号列表）
    LaunchParallelTest(2, InsertHelper<Tombs>, &tree, keys);

    // 8. 验证插入结果（类比检查每个快递是否都正确放进了分拣系统，位置是否对）
    std::vector<RID> rids;       // 存储查询到的"记录ID"（类比快递在货架上的具体位置：格子号+槽位号）
    GenericKey<8> index_key;     // 用于查询的键（类比要查询的快递单号）
    for (auto key : keys) {      // 遍历每个插入的快递单号
      rids.clear();              // 清空上一次查询的位置记录
      index_key.SetFromInteger(key); // 把当前快递单号转换成B+树能识别的键格式（类比把单号写在查询单上）
      tree.GetValue(index_key, &rids); // 从B+树中查询该键对应的位置（类比根据单号查快递在货架的位置）

      // 断言1：查询到的位置数量必须是1（类比每个快递只能有一个存放位置，不能多也不能少）
      ASSERT_EQ(rids.size(), 1);

      // 计算预期的槽位号（类比根据快递单号计算它应该在格子里的第几个槽位）
      // key & 0xFFFFFFFF：取单号的低32位（因为RID的槽位号是32位）
      int64_t value = key & 0xFFFFFFFF;
      // 断言2：查询到的槽位号必须和预期一致（类比实际存放的槽位和计算的槽位要匹配）
      ASSERT_EQ(rids[0].GetSlotNum(), value);
    }

    // 9. 验证B+树的遍历顺序（类比从分拣系统的第一个快递开始，依次检查所有快递的顺序是否正确）
    int64_t start_key = 1;       // 预期的第一个快递单号（从1开始）
    int64_t current_key = start_key; // 当前检查到的快递单号（初始为1）

    // 遍历B+树的所有节点（类比从分拣系统的起点到终点，依次查看每个快递）
    for (auto iter1 = tree.Begin(); iter1 != tree.End(); ++iter1) {
      const auto &pair = *iter1; // 获取当前节点的键-位置对（类比当前快递的单号和位置）
      auto location = pair.second; // 提取位置信息（类比快递在货架的位置）

      // 断言3：位置的页面ID必须是0（类比所有快递都存放在编号0的货架格子里，此处是测试简化）
      ASSERT_EQ(location.GetPageId(), 0);
      // 断言4：当前快递的槽位号必须等于当前预期的单号（类比快递单号和槽位号要一一对应，顺序不能乱）
      ASSERT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1; // 准备检查下一个快递（单号+1）
    }

    // 断言5：遍历结束后，当前检查的单号必须等于总快递数+1（类比所有快递都检查完了，没多没少）
    // 比如插入99个快递（1~99），current_key最终应该是100（99+1）
    ASSERT_EQ(current_key, keys.size() + 1);

    // 10. 清理资源（类比测试结束后，销毁分拣系统、清空货架、删除临时记录）
    delete disk_manager; // 销毁仓库管理员
    delete bpm;          // 销毁临时货架
    remove("test.db");   // 删除测试用的数据库文件（类比删除快递记录文件）
    remove("test.log");  // 删除测试日志文件（类比删除分拣系统的操作日志）
  }
}

// 模板函数：InsertTest2Call（插入测试2），与InsertTest1Call类似，但测试更大量数据和拆分场景
// 功能：测试B+树在插入大量数据时的节点拆分功能（类比快递太多时，分拣节点需要拆分出新的分拣口）
template <ssize_t Tombs>
void InsertTest2Call() {
  // 循环NUM_ITERS（50）次，重复执行测试（类比复杂分拣流程重复测50遍）
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // 1. 创建键比较器和索引结构描述（和InsertTest1一致，类比相同的快递分拣规则）
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    // 2. 创建磁盘管理器和缓冲池管理器（和InsertTest1一致，类比相同的仓库和货架）
    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // 3. 创建B+树头页面（和InsertTest1一致，类比相同的总控台页面）
    page_id_t page_id = bpm->NewPage();

    // 4. 创建B+树实例（和InsertTest1一致，类比相同的分拣系统）
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> tree("foo_pk", page_id, bpm, comparator, 3, 5);

    // 5. 准备要插入的键（类比准备1000个快递单号，编号从1到999，比InsertTest1多10倍）
    std::vector<int64_t> keys;
    int64_t scale_factor = 1000; // 插入999个快递（1~999）
    for (int64_t key = 1; key < scale_factor; key++) {
      keys.push_back(key);
    }

    // 6. 启动并行测试插入数据（区别：用InsertHelperSplit，支持节点拆分）
    // 类比：2个快递员同时放快递，当某个分拣节点的快递太多（超过最大子节点数5）时，自动拆分成新节点
    LaunchParallelTest(2, InsertHelperSplit<Tombs>, &tree, keys, 2);

    // 7. 验证插入结果（和InsertTest1完全一致，类比检查每个快递的位置是否正确）
    std::vector<RID> rids;
    GenericKey<8> index_key;
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      tree.GetValue(index_key, &rids);
      ASSERT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      ASSERT_EQ(rids[0].GetSlotNum(), value);
    }

    // 8. 验证B+树的遍历顺序（和InsertTest1一致，类比检查所有快递的顺序是否正确）
    int64_t start_key = 1;
    int64_t current_key = start_key;

    for (auto iter1 = tree.Begin(); iter1 != tree.End(); ++iter1) {
      const auto &pair = *iter1;
      auto location = pair.second;
      ASSERT_EQ(location.GetPageId(), 0);
      ASSERT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
    }

    // 断言：遍历结束后，当前单号等于总快递数+1（999+1=1000）
    ASSERT_EQ(current_key, keys.size() + 1);

    // 9. 清理资源（和InsertTest1一致，类比销毁系统、清空记录）
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

// 模板函数：DeleteTest1Call（删除测试1）
// 功能：测试B+树的基础删除功能，验证删除后剩余数据的正确性（类比删除部分快递后，检查剩余快递是否正确）
template <ssize_t Tombs>
void DeleteTest1Call() {
  // 循环NUM_ITERS（50）次，重复执行测试（类比删除流程重复测50遍）
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // 1. 创建键比较器和索引结构描述（和插入测试一致，类比相同的快递分拣规则）
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    // 2. 创建磁盘管理器和缓冲池管理器（和插入测试一致，类比相同的仓库和货架）
    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // 3. 创建B+树头页面（和插入测试一致，类比相同的总控台页面）
    page_id_t page_id = bpm->NewPage();

    // 4. 创建B+树实例（和插入测试一致，类比相同的分拣系统）
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> tree("foo_pk", page_id, bpm, comparator, 3, 5);

    // 5. 顺序插入数据（类比先往分拣系统里放5个快递，单号1~5）
    std::vector<int64_t> keys = {1, 2, 3, 4, 5};
    InsertHelper(&tree, keys); // 单线程插入（类比1个快递员放快递，确保初始数据正确）

    // 6. 准备要删除的键（类比要从分拣系统中拿走的4个快递，单号1、5、3、4）
    std::vector<int64_t> remove_keys = {1, 5, 3, 4};
    // 启动并行测试删除数据（类比2个快递员同时拿走指定快递）
    LaunchParallelTest(2, DeleteHelper<Tombs>, &tree, remove_keys);

    // 7. 验证删除结果（类比检查剩余快递是否正确，只剩单号2）
    int64_t start_key = 2;       // 预期剩余的第一个快递单号（2）
    int64_t current_key = start_key; // 当前检查的单号（初始为2）
    int64_t size = 0;            // 统计剩余快递的数量（初始为0）

    // 遍历B+树的所有节点（类比查看分拣系统中剩余的所有快递）
    for (auto iter1 = tree.Begin(); iter1 != tree.End(); ++iter1) {
      const auto &pair = *iter1; // 获取当前快递的单号和位置
      auto location = pair.second;

      // 断言1：位置的页面ID必须是0（类比剩余快递仍在编号0的货架格子里）
      ASSERT_EQ(location.GetPageId(), 0);
      // 断言2：当前快递的槽位号必须等于预期单号（2）
      ASSERT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1; // 准备检查下一个（此处只有1个，不会执行）
      size = size + 1; // 剩余快递数量+1（最终应为1）
    }

    // 断言3：剩余快递数量必须是1（类比删除4个后，只剩1个快递）
    ASSERT_EQ(size, 1);

    // 8. 清理资源（和插入测试一致，类比销毁系统、清空记录）
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

template <ssize_t Tombs>
void DeleteTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> tree("foo_pk", page_id, bpm, comparator, 3, 5);

    // sequential insert
    std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    InsertHelper(&tree, keys);

    std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
    LaunchParallelTest(2, DeleteHelperSplit<Tombs>, &tree, remove_keys, 2);

    int64_t start_key = 7;
    int64_t current_key = start_key;
    int64_t size = 0;

    for (auto iter1 = tree.Begin(); iter1 != tree.End(); ++iter1) {
      const auto &pair = *iter1;
      auto location = pair.second;
      ASSERT_EQ(location.GetPageId(), 0);
      ASSERT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
      size = size + 1;
    }

    ASSERT_EQ(size, 4);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

template <ssize_t Tombs>
void MixTest1Call() {
  for (size_t iter = 0; iter < MIXTEST_NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> tree("foo_pk", page_id, bpm, comparator, 3, 5);

    // first, populate index
    std::vector<int64_t> for_insert;
    std::vector<int64_t> for_delete;
    int64_t sieve = 2;  // divide evenly
    int64_t total_keys = 1000;
    for (int64_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        for_insert.push_back(i);
      } else {
        for_delete.push_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper(&tree, for_delete);

    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete); };
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 10;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(tasks[i % tasks.size()], i);
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    int64_t size = 0;

    for (auto iter1 = tree.Begin(); iter1 != tree.End(); ++iter1) {
      const auto &pair = *iter1;
      ASSERT_EQ((pair.first).ToString(), for_insert[size]);
      size++;
    }

    ASSERT_EQ(size, for_insert.size());

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

template <ssize_t Tombs>
void MixTest2Call() {
  for (size_t iter = 0; iter < MIXTEST_NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, Tombs> tree("foo_pk", page_id, bpm, comparator);

    // Add preserved_keys
    std::vector<int64_t> preserved_keys;
    std::vector<int64_t> dynamic_keys;
    int64_t total_keys = 1000;
    int64_t sieve = 10;
    for (int64_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        preserved_keys.push_back(i);
      } else {
        dynamic_keys.push_back(i);
      }
    }
    InsertHelper(&tree, preserved_keys);

    size_t size;

    auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys); };
    auto lookup_task = [&](int tid) { LookupHelper(&tree, preserved_keys); };

    std::vector<std::thread> threads;
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    tasks.emplace_back(lookup_task);

    size_t num_threads = 6;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(tasks[i % tasks.size()], i);
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    // Check all reserved keys exist
    size = 0;

    for (auto iter1 = tree.Begin(); iter1 != tree.End(); ++iter1) {
      const auto &pair = *iter1;
      if ((pair.first).ToString() % sieve == 0) {
        size++;
      }
    }

    ASSERT_EQ(size, preserved_keys.size());

    delete disk_manager;
    delete bpm;
  }
}

TEST(BPlusTreeConcurrentTest, InsertTest1) {  // NOLINT
  InsertTest1Call<0>();
  InsertTest1Call<3>();
}

TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest2) {  // NOLINT
  InsertTest2Call<0>();
  InsertTest2Call<3>();
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest1) {  // NOLINT
  DeleteTest1Call<0>();
  DeleteTest1Call<3>();
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest2) {  // NOLINT
  DeleteTest2Call<0>();
  DeleteTest2Call<3>();
}

TEST(BPlusTreeConcurrentTest, DISABLED_MixTest1) {  // NOLINT
  MixTest1Call<0>();
  MixTest1Call<3>();
}

TEST(BPlusTreeConcurrentTest, DISABLED_MixTest2) {  // NOLINT
  MixTest2Call<0>();
  MixTest2Call<3>();
}
}  // namespace bustub
