#pragma once

// 引入标准库头文件，提供基础数据结构和功能支持
// 类比：就像厨师准备食材，这些是烹饪（编写代码）所需的基础工具
#include <algorithm>    // 提供排序、查找等算法（如食材切割、整理工具）
#include <deque>        // 双端队列容器（如既能从前端拿也能从后端拿的食材筐）
#include <filesystem>   // 文件系统操作（如食材仓库的出入库记录管理）
#include <iostream>     // 输入输出流（如厨师与外界的沟通工具，比如报菜名）
#include <memory>       // 智能指针（如带自动归还功能的工具，用完自动放回工具箱）
#include <optional>     // 可选值容器（如可能有也可能没有的备用食材）
#include <queue>        // 队列容器（如按顺序排队取用的食材）
#include <shared_mutex> // 共享互斥锁（如厨房工具的共享使用权限管理，多人可同时看，一人用）
#include <string>       // 字符串处理（如食材名称标签）
#include <vector>       // 动态数组容器（如可伸缩的食材托盘）

// 引入项目内部头文件，使用项目自定义的配置和工具
// 类比：引入餐厅内部的规章制度和专用厨具
#include "common/config.h"               // 项目通用配置（如餐厅的营业时间、卫生标准）
#include "common/macros.h"               // 项目通用宏定义（如餐厅的固定术语缩写）
#include "storage/index/index_iterator.h"// 索引迭代器（如食材架的移动取物工具）
#include "storage/page/b_plus_tree_header_page.h"// B+树头页面（如食材仓库的总台账）
#include "storage/page/b_plus_tree_internal_page.h"// B+树内部页面（如食材仓库的分区货架标签）
#include "storage/page/b_plus_tree_leaf_page.h"// B+树叶子页面（如食材仓库的具体食材存放格）
#include "storage/page/page_guard.h"// 页面守卫（如食材货架的锁，防止别人乱拿）

// 定义项目命名空间，避免与其他代码的命名冲突
// 类比：给餐厅起个独特的名字，避免和其他餐厅重名
namespace bustub {

// 前向声明可打印B+树结构，告诉编译器该结构存在（后续会定义）
// 类比：提前告诉厨师"有一个专门用来展示食材的架子"，具体样子后面再说
struct PrintableBPlusTree;

/**
 * @brief Context类的定义
 *
 * 提示：这个类用于帮助你跟踪正在修改或访问的页面
 * 类比：就像餐厅服务员的"点单记录本"，记录当前正在处理的桌号、点的菜品、使用的餐具，
 *       方便服务员跟踪整个服务流程，不会搞混不同桌的订单
 */
class Context {
 public:
  // 当你向B+树插入/删除数据时，将头页面的写守卫存在这里
  // 注意：当你想解锁所有页面时，要释放头页面的守卫并将其设为nullopt（无值状态）
  // 类比：记录本里专门留一栏记"仓库总钥匙"，拿了总钥匙才能进仓库；
  //       用完仓库后要把总钥匙还回去，这一栏就清空（表示钥匙已归还）
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // 把根页面ID存在这里，方便判断当前页面是不是根页面
  // 类比：记录本里记着"仓库大门的编号"，看到某个门的编号和这个一样，就知道是仓库大门
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // 把正在修改的页面的写守卫存在这里
  // 类比：记录本里记着"正在使用的货架钥匙"，比如当前在改1号货架的食材，就把1号货架钥匙存这里
  std::deque<WritePageGuard> write_set_;

  // 读取数据时可能会用到这个（但不是必须的）
  // 类比：记录本里记着"正在查看的货架钥匙"，只是看食材不修改，就把对应的钥匙存这里
  std::deque<ReadPageGuard> read_set_;

  // 判断某个页面ID是不是根页面（通过和存好的根页面ID对比）
  // 类比：检查当前拿的货架编号是不是和"仓库大门编号"一样，一样就是大门（根页面）
  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

// 定义B+树的类型别名，简化后续代码中的类型书写
// 类比：给"豪华海鲜B+树"起个简称"海B树"，后面提到"海B树"就知道是指前者
#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator, NumTombs>

// B+树主类，提供交互式B+树的API（对外提供的功能接口）
// 类比：这是"食材仓库管理系统"的核心模块，对外提供"存食材""取食材""查食材"等功能按钮
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class BPlusTree {
  // 定义内部页面类型别名：B+树内部页面（存键和子页面ID）
  // 类比：给"仓库分区货架标签页"起简称"分区页"，方便内部称呼
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  // 定义叶子页面类型别名：B+树叶子页面（存键值对）
  // 类比：给"食材存放格标签页"起简称"食材页"，方便内部称呼
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // B+树构造函数：初始化一个B+树实例
  // 参数说明：
  // - name：B+树的名字（如"生鲜食材树"）
  // - header_page_id：头页面的ID（如仓库总台账的编号）
  // - buffer_pool_manager：缓冲池管理器（如食材暂存区的管理员，负责食材的存取）
  // - comparator：键比较器（如判断食材编号大小的规则）
  // - leaf_max_size：叶子页面最大容量（如每个食材存放格最多放多少种食材）
  // - internal_max_size：内部页面最大容量（如每个分区货架标签页最多记多少个分区）
  // 类比：创建一个"生鲜食材仓库"，需要指定仓库名、台账编号、暂存区管理员、食材排序规则、
  //       每个存放格容量、每个分区标签页容量
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // 判断当前B+树是否为空（没有键值对）
  // 类比：检查仓库里是不是没有任何食材
  auto IsEmpty() const -> bool;
  void UpdateFather(KeyType first_key, KeyType second_key, WritePageGuard &write_guard);
  void RecursiveUpdateKeyForRedistribute(KeyType old_key, std::pair<KeyType, page_id_t> new_pair,
                                         BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* father_write);

  auto LocateKey(const KeyType &key, const BPlusTreeHeaderPage* header_page) -> page_id_t;

  // 向B+树中插入一个键值对（key是键，value是值）
  // 返回值：插入成功返回true，失败返回false
  // 类比：把一个标注了编号（key）的食材（value）放进仓库，放成功了返回true
  /*
   *步骤重复性分析：
   * 插入元素导致 叶子页满 则分裂 分裂之后要建立新内页来管理
   *内页满 则分裂 分裂之后要新内页来管理
   *而且 刚好分裂函数里的移动操作 内页和内页的移动（无论是下层为叶子页还是内页）都能共用一个函数
   *所以 递归设计能行的通 按上面步骤来写
   *本质上是由插入导致的分裂 由自下而上引起 所以要写个pushup 递归自下而上实现守恒
   *所以时刻以根页为点 检测是否为满 满了 就固定执行右建立加上建立逻辑 再以根为点
   *
  */
  auto Insert(const KeyType &key, const ValueType &value) -> bool;

  //原理 插入导致的变动 写个递归来逐层检验父页的变动是否要进行处理
  void PushUp(page_id_t id, WritePageGuard& write_guard);

  void RedistributeForLeaf(page_id_t page_id, B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_write);
  void RedistributeForInternal(page_id_t page_id,
                               BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *internal_write);

  void MergeForLeaf(WritePageGuard &leaf_guard);
  void MergeForInternal(WritePageGuard &internal_guard);
  void CheckForInternal(WritePageGuard &internal_guard);

  auto IsDistributeForLeaf(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_write) -> page_id_t;
  auto IsDistributeForInternal(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal_write, int to_size) -> page_id_t;

  void CheckForLeaf(WritePageGuard &leaf_guard);

  void DeepDeleteOrUpdate(const KeyType &key, std::optional<KeyType> update_key, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *internal_write, bool
                          is_update);

  void DeepUpdate(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_write, KeyType temp_key);
  // 从B+树中删除一个键及其对应的值
  // 类比：根据食材编号（key），从仓库里把对应的食材（value）拿走
  void Remove(const KeyType &key);

  // 根据给定的键，获取其对应的值，结果存到result向量中
  // 返回值：找到键返回true，没找到返回false
  // 类比：根据食材编号（key）找对应的食材，找到后放进食材篮（result），找到返回true
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // 返回根页面的ID
  // 类比：返回仓库大门的编号
  auto GetRootPageId() -> page_id_t;

  // 索引迭代器相关：获取B+树的起始迭代器（指向第一个键值对）
  // 类比：获取"食材架取物工具"，并把工具定位到第一个食材的位置
  auto Begin() -> INDEXITERATOR_TYPE;

  // 获取B+树的结束迭代器（指向最后一个键值对的下一个位置）
  // 类比：获取"食材架取物工具"，并把工具定位到最后一个食材的后面（表示没有更多食材）
  auto End() -> INDEXITERATOR_TYPE;

  // 获取从指定键开始的迭代器（指向第一个大于等于该键的键值对）
  // 类比：获取"食材架取物工具"，并把工具定位到第一个编号大于等于指定编号的食材位置
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // 打印B+树的内容（需要缓冲池管理器协助获取页面）
  // 类比：让暂存区管理员帮忙把仓库里的食材分布情况打印出来
  void Print(BufferPoolManager *bpm);

  // 绘制B+树结构到指定文件（outf是输出文件路径）
  // 类比：把仓库的货架布局画成图，保存到指定的文件里
  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);

  // 生成B+树的绘制字符串（用于后续绘图）
  // 类比：生成描述仓库货架布局的文字说明，方便后续根据说明画图
  auto DrawBPlusTree() -> std::string;

  // 从文件中读取数据，逐个插入B+树
  // 类比：从食材订单文件里读取每一种要入库的食材，逐个放进仓库
  void InsertFromFile(const std::filesystem::path &file_name);

  // 从文件中读取数据，逐个从B+树中删除
  // 类比：从食材出库文件里读取每一种要出库的食材，逐个从仓库拿走
  void RemoveFromFile(const std::filesystem::path &file_name);

  // 从文件中读取批量操作指令（插入/删除），并执行
  // 类比：从批量食材处理文件里读取一堆操作（有的要入库，有的要出库），逐个执行
  void BatchOpsFromFile(const std::filesystem::path &file_name);

  auto SplitForInternal(BPlusTreeInternalPage<
                          KeyType, page_id_t, KeyComparator> *first_internal_write, BPlusTreeInternalPage<
                          KeyType, page_id_t, KeyComparator> *second_internal_write) -> KeyType;
  // 缓冲池管理器（注意：不能改成普通的BufferPoolManager类型）
  // 类比：带操作记录功能的食材暂存区管理员（会记录每一次存取操作，方便追溯）
  std::shared_ptr<TracedBufferPoolManager> bpm_;

  auto GetLeafMaxSize()->int;

  auto GetInternalMaxSize()->int;
 private:
  // 辅助绘图函数：将指定页面（page_id）和页面数据（page）写入输出流（out）
  // 类比：把某个货架（page_id）的布局和里面的食材（page）画到图纸上（out）
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  // 辅助打印函数：打印指定页面（page_id）和页面数据（page）的内容
  // 类比：把某个货架（page_id）的布局和里面的食材（page）列成清单
  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  // 将B+树（根页面为root_id）转换为可打印的B+树结构（PrintableBPlusTree）
  // 类比：把实际的仓库货架布局，转换成适合展示给别人看的简化布局（比如去掉复杂的内部标签）
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // B+树的成员变量（属性）
  std::string index_name_;         // B+树的名字（如"生鲜食材树"）
  KeyComparator comparator_;       // 键比较器（如食材编号的排序规则）
  std::vector<std::string> log;    // 操作日志（记录每一次对B+树的操作，如"插入了食材A"）
  int leaf_max_size_;              // 叶子页面的最大容量（每个食材存放格最多放多少种食材）
  int internal_max_size_;          // 内部页面的最大容量（每个分区标签页最多记多少个分区）
  page_id_t header_page_id_;       // 头页面的ID（仓库总台账的编号）
};

/**
 * @brief 仅用于测试。PrintableBPlusTree是可打印的B+树结构。
 * 我们先把普通B+树转换成可打印B+树，然后再打印它。
 * 类比：这是"仓库简化展示模型"，专门用来给别人看的。先把实际仓库按比例缩小成模型，
 *       再展示这个模型（避免直接展示复杂的实际仓库）
 */
struct PrintableBPlusTree {
  int size_;                      // 当前节点的展示宽度（如模型中某个货架的宽度）
  std::string keys_;              // 当前节点的键（用字符串表示，如"食材1,食材2"）
  std::vector<PrintableBPlusTree> children_; // 当前节点的子节点（如模型中某个货架的子分区）

  /**
   * @brief 广度优先遍历（BFS）可打印B+树，并把结果打印到输出流（out_buf）
   * 广度优先遍历：先看当前层的所有节点，再看下一层的节点（比如先看所有一级货架，再看二级货架）
   *
   * @param out_buf 输出流（如打印纸、控制台）
   * 类比：按"从外到内、从上层到下层"的顺序，把仓库模型的每一层都展示在纸上（out_buf）
   */
  void Print(std::ostream &out_buf) {
    // 初始化队列，把当前可打印B+树节点（根节点）加入队列
    // 类比：先把仓库模型的大门（根节点）放到待展示的队列里
    std::vector<PrintableBPlusTree *> que = {this};
    // 队列不为空时，继续遍历（还有节点没展示）
    while (!que.empty()) {
      // 存储下一层的节点（当前层展示完后，要展示下一层）
      // 类比：准备一个新的篮子，装下一层要展示的货架
      std::vector<PrintableBPlusTree *> new_que;

      // 遍历当前层的所有节点（逐个展示当前层的货架）
      for (auto &t : que) {
        // 计算需要填充的空格数：让当前节点的键在展示宽度中居中
        // 类比：让货架标签（keys_）在货架宽度（size_）中居中，两边补空格
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' '); // 左边补空格
        out_buf << t->keys_;                  // 打印当前节点的键（货架标签）
        out_buf << std::string(padding, ' '); // 右边补空格

        // 把当前节点的所有子节点加入下一层队列（准备展示下一层货架）
        // 类比：把当前货架的所有子分区，放到新的待展示篮子里
        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n"; // 当前层展示完，换行准备展示下一层
      que = new_que;   // 把下一层节点作为当前层，继续循环
    }
  }
};

}  // namespace bustub