#pragma once

// 引入标准库头文件，提供字符串、pair（键值对）、向量（动态数组）的功能支持
// 类比：就像厨师做菜前准备好各种基础食材（蔬菜、肉类、调料），这些头文件提供了代码中要用到的基础数据结构工具
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

// 引入B+树页面的基类头文件，当前叶子页面类会继承这个基类的属性和方法
// 类比：就像"汽车"类是"SUV汽车"类的基类，SUV会继承汽车的基本属性（轮子、发动机）和方法（行驶、刹车）
#include "storage/page/b_plus_tree_page.h"

// 定义命名空间bustub，用于隔离代码模块，避免不同模块间的命名冲突
// 类比：就像不同公司的产品用不同的品牌名，比如"苹果"的手机和"华为"的手机，即使都叫"Mate系列"也不会混淆
namespace bustub {

// 宏定义：给BPlusTreeLeafPage这个长类名起一个简短的别名B_PLUS_TREE_LEAF_PAGE_TYPE
#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>

// 宏定义：B+树叶子页面的头部大小为16字节
// 类比：就像快递盒的"面单区域"固定占10cm×5cm的面积，这个区域专门用来写收件人信息，大小是提前规定好的
#define LEAF_PAGE_HEADER_SIZE 16

// 宏定义：叶子页面中墓碑（标记删除的条目）的默认数量为0
// 类比：就像刚出厂的笔记本，默认没有任何页面被画上"删除线"，墓碑数量就是0
#define LEAF_PAGE_DEFAULT_TOMB_CNT 0

// 宏定义：计算叶子页面的实际墓碑数量。如果NumTombs小于0（非法值），就用默认的0；否则用NumTombs
// 类比：就像商店规定"购买数量不能为负数，若输入负数则按0处理"，确保墓碑数量始终是合法的非负值
#define LEAF_PAGE_TOMB_CNT ((NumTombs < 0) ? LEAF_PAGE_DEFAULT_TOMB_CNT : NumTombs)


#define LEAF_PAGE_FIXED_METADATA_SIZE                                                 \
  (LEAF_PAGE_HEADER_SIZE + 3 * sizeof(page_id_t) + sizeof(size_t) + sizeof(KeyType) + \
   sizeof(uint64_t) + (LEAF_PAGE_TOMB_CNT * sizeof(size_t)))

#define LEAF_PAGE_SLOT_CNT \
  ((BUSTUB_PAGE_SIZE - LEAF_PAGE_FIXED_METADATA_SIZE) / (sizeof(KeyType) + sizeof(ValueType)))  // NOLINT（忽略编译器的某些警告）

/**
 * B+树叶子页面类的功能说明：
 * 1. 存储索引键（Key）和记录ID（RID，由页面ID和插槽ID组成，具体定义见include/common/rid.h）
 * 2. 只支持唯一键（即同一个键不能重复存储）
 * 3. 叶子页面还包含一个固定大小的"墓碑缓冲区"，用于记录已被删除的条目的索引
 * 
 * 叶子页面的内存格式（键按顺序存储，墓碑的顺序可自定义）：
 *  --------------------
 * | 头部（HEADER） | 墓碑数量（TOMB_SIZE，即num_tombstones_的值） |
 *  --------------------
 *  -----------------------------------
 * | 墓碑0（TOMB(0)） | 墓碑1（TOMB(1)） | ... | 墓碑k（TOMB(k)） |
 *  -----------------------------------
 *  ---------------------------------
 * | 键1（KEY(1)） | 键2（KEY(2)） | ... | 键n（KEY(n)） |
 *  ---------------------------------
 *  ---------------------------------
 * | 记录ID1（RID(1)） | 记录ID2（RID(2)） | ... | 记录IDn（RID(n)） |
 *  ---------------------------------
 * 
 * 头部（HEADER）的格式（单位：字节，总大小16字节）：
 *  -----------------------------------------------
 * | 页面类型（PageType，4字节） | 当前大小（CurrentSize，4字节） | 最大大小（MaxSize，4字节） |
 *  -----------------------------------------------
 *  -----------------
 * | 下一个页面ID（NextPageId，4字节） |
 *  -----------------
 * 
 * 类比理解：叶子页面就像一本"电话簿"：
 * - 头部（HEADER）：电话簿的封面信息（比如"2024年通讯录"（页面类型）、当前记录了50个人（当前大小）、最多能记100个人（最大大小）、下一本电话簿的编号（下一页ID））
 * - 墓碑缓冲区：电话簿中被画上"删除线"的条目编号（比如第3条、第15条记录已失效）
 * - 键数组（KEY数组）：每个人的名字（用于索引查找）
 * - RID数组：每个人的电话号码（对应实际数据的位置）
 */
// FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN：是一个模板参数定义的宏（通常在其他头文件中定义），用于简化模板类的声明
// 类比：就像一个"通用包装盒模板"，可以指定盒子装的物品类型（KeyType=水果、ValueType=苹果），这个宏就是提前定义好的模板参数格式
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
// 定义B+树叶子页面类，继承自BPlusTreePage（基类），拥有基类的所有属性和方法
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // 删除默认构造函数和拷贝构造函数，防止通过这些方式创建对象，确保内存安全（避免浅拷贝等问题）
  // 类比：就像一个特殊的保险箱，不允许用"默认钥匙"（默认构造）或"复制钥匙"（拷贝构造）打开，只能通过指定的初始化方法（Init）启用
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  /**
   * @brief 初始化叶子页面
   * @param max_size：页面的最大插槽数量（默认使用LEAF_PAGE_SLOT_CNT计算出的默认值）
   * 功能：设置页面的最大大小、初始化当前大小为0、墓碑数量为0、下一页ID为无效值等
   * 类比：就像给新电话簿初始化：设定最多能记100个人（max_size）、当前记录数0、没有删除条目、下一本电话簿暂时不存在（下一页ID无效）
   */
  void Init(int max_size = LEAF_PAGE_SLOT_CNT);

  /**
   * @brief 获取所有墓碑对应的键
   * @return 存储墓碑键的向量（vector）
   * 功能：根据tombstones_数组中记录的索引，到key_array_中找到对应的键，组成向量返回
   * 类比：根据电话簿中"删除线"的条目编号（比如第3条），找到对应的名字（键），整理成一个"已删除名单"
   */
  auto GetTombstones() const -> std::vector<KeyType>;
  auto SetNumTombstones(size_t num) -> void;
  auto GetNumTombstones() const -> size_t;

  // 辅助方法（Helper methods）：提供简单的属性访问或设置功能

  /**
   * @brief 获取下一个页面的ID
   * @return 下一个页面的ID（page_id_t类型）
   * 功能：读取next_page_id_变量的值并返回
   * 类比：查看电话簿封面上写的"下一本电话簿编号"
   */
  auto GetNextPageId() const -> page_id_t;
  auto GetPrePageId() const -> page_id_t;

  /**
   * @brief 设置下一个页面的ID
   * @param next_page_id：要设置的下一个页面ID
   * 功能：将next_page_id_变量的值更新为传入的参数
   * 类比：在电话簿封面上填写"下一本电话簿编号"
   */
  void SetNextPageId(page_id_t next_page_id);

  void SetPrePageId(page_id_t pre_page_id);

  /**
   * @brief 获取指定索引位置的键
   * @param index：要获取的键的索引（从0开始）
   * @return 对应索引位置的键（KeyType类型）
   * 功能：读取key_array_数组中index位置的元素并返回
   * 类比：在电话簿中翻到第index+1条记录，获取对应的名字（键）
   */
  auto KeyAt(int index) const -> KeyType;

  auto ValueAt(int index) const -> ValueType;

  KeyType Absorb(B_PLUS_TREE_LEAF_PAGE_TYPE *page);
  void MarkTomb(int index);
  bool IsTombstone(int index) const;
  void RemoveTombstone(int index);
  void ProcessOldestTombstone();  // 处理最早的墓碑：物理删除并调整索引
  bool IsUpdate();
  bool IsEmpty();
  void CleanupTombs();
  /**
   * @brief 仅用于测试：将当前叶子页面的所有键和墓碑键格式化为字符串
   * @return 格式化后的字符串，格式为"(tombkey1, tombkey2, ...|key1,key2,key3,...)"
   * 功能：
   * 1. 先获取所有墓碑键，拼接成"tombkey1,tombkey2,..."的形式
   * 2. 再获取当前页面的所有有效键，拼接成"key1,key2,..."的形式
   * 3. 用"|"分隔两部分，外层加括号组成最终字符串
   * 类比：将电话簿的"已删除名单"和"有效名单"整理成字符串："(已删1,已删2|有效1,有效2)"，方便测试时查看页面内容
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";  // 初始化字符串，开头加左括号
    bool first = true;       // 标记是否为第一个有效键，用于控制是否加逗号

    // 1. 处理墓碑键：获取所有墓碑键并拼接到字符串中
    auto tombs = GetTombstones();  // 调用GetTombstones()获取墓碑键向量
    for (size_t i = 0; i < tombs.size(); i++) {
      // 将墓碑键转换为字符串并追加到结果中（这里原代码可能有误，应该是tombs[i]而非tombs[i].ToString()，假设KeyType有ToString()方法）
      kstr.append(std::to_string(tombs[i].ToString()));
      // 如果不是最后一个墓碑键，追加逗号分隔
      if ((i + 1) < tombs.size()) {
        kstr.append(",");
      }
    }

    // 在墓碑键和有效键之间加"|"分隔
    kstr.append("|");

    // 2. 处理有效键：遍历当前页面的所有有效键并拼接到字符串中
    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);  // 获取第i个有效键
      if (first) {
        first = false;  // 第一个键不需要加前置逗号，标记为非首次
      } else {
        kstr.append(",");  // 非第一个键，先加逗号分隔
      }
      // 将有效键转换为字符串并追加到结果中
      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");  // 字符串结尾加右括号

    return kstr;  // 返回格式化后的字符串
  }

  auto GetMinKey()->KeyType;
  auto IsBegin() -> bool;
  auto SetBegin(bool set) -> void;
  auto InsertKeyValue(const KeyComparator& comparator, const KeyType& key, const ValueType& value) -> bool;
  
  /**
   * @brief 获取逻辑大小（有效键数量）
   * @return 逻辑大小 = GetSize() + num_tombstones_（因为删除时size减1但墓碑加1）
   */
  auto GetRealSize() const -> int;

  auto BinarySearch(const KeyComparator& comparator,const KeyType &key)->int;

  int MatchKey(KeyType key, const KeyComparator &comparator);

  void Delete(const KeyType key, const KeyComparator &comparator);

  void FindAndPush(const KeyComparator &comparator, const KeyType &key, std::vector<ValueType> *result) const;
  void InsertBegin(std::pair<KeyType, ValueType> pair);
  void InsertBack(std::pair<KeyType, ValueType> pair);

  auto PopBack() -> std::pair<KeyType, ValueType>;
  std::pair<KeyType, ValueType> PopFront();
  auto GetBeforeFirstKey() const -> KeyType;

  void Split(B_PLUS_TREE_LEAF_PAGE_TYPE* new_leaf_page);

  void SetIsUpdate(bool set);
 private:
  // 私有成员变量：只能在类内部访问，保证数据安全性
  // 注意：成员变量顺序很重要，影响内存对齐和布局
  // 顺序原则：先放置固定大小的成员，再放置数组，最后放置小的bool类型

  /**
   * 下一个叶子页面的ID
   * 作用：B+树的叶子页面通过这个ID组成链表，方便顺序遍历所有叶子页面（比如全表扫描）
   * 类比：电话簿封面上写的"下一本电话簿编号"，通过这个编号可以找到下一本电话簿，实现所有电话簿的顺序查阅
   */
  page_id_t next_page_id_;

  /**
   * 当前页面中墓碑的实际数量
   * 作用：记录tombstones_数组中已使用的元素个数（即已删除的条目数量）
   * 类比：电话簿中"已删除条目"的计数，比如"共5条已删除记录"
   * 注意：size_t 在64位系统上是8字节，需要8字节对齐。将pre_page_id_放在前面，这样next_page_id_和pre_page_id_共8字节，正好对齐到8字节边界
   */
  size_t num_tombstones_;

  /**
   * 固定大小的墓碑缓冲区：存储已删除条目的索引（对应key_array_和rid_array_中的位置）
   * 大小由LEAF_PAGE_TOMB_CNT宏定义决定
   * 类比：电话簿中专门记录"已删除条目编号"的列表，比如"已删除条目编号：3、5、8"
   */
  size_t tombstones_[LEAF_PAGE_TOMB_CNT];

  /**
   * 存储键（Key）的数组：大小由LEAF_PAGE_SLOT_CNT宏定义决定，键按顺序存储（便于二分查找）
   * 类比：电话簿中存储"姓名"的列表，所有姓名按拼音排序（便于快速查找）
   */
  KeyType key_array_[LEAF_PAGE_SLOT_CNT];

  /**
   * 存储记录ID（RID）的数组：与key_array_一一对应，每个键对应一个RID（指向实际数据的位置）
   * 类比：电话簿中存储"电话号码"的列表，每个姓名（键）对应一个电话号码（RID）
   */
  ValueType rid_array_[LEAF_PAGE_SLOT_CNT];

  page_id_t pre_page_id_;

  bool is_begin;

  bool is_update_;//用于remove操作

  KeyType before_first_key_;

  bool need_deep_update;
  // 注释：2025年春季学期补充，允许根据需要添加更多私有成员变量和辅助函数
  // (Spring 2025) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub  // 命名空间结束