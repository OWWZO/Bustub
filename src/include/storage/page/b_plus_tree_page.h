#pragma once

// 包含必要的标准库头文件：
// cassert：提供断言功能（类比：质检人员的检查清单，用于验证生产过程中关键环节是否符合预期，不符合则立即报警）
// climits：提供系统级别的数值限制（如INT_MAX，类比：快递包裹的最大重量限制标准，明确单个包裹能承载的重量上限）
// cstdlib：提供通用实用函数（类比：工具箱，包含各种基础维修工具，按需取用）
// string：提供字符串处理功能（类比：记事本，用于存储和处理文本信息）
#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

// 包含缓冲池管理器头文件：缓冲池管理器负责数据库页面在内存和磁盘间的调度（类比：图书馆的图书管理员，负责图书在书架（磁盘）和阅览区（内存）的存取管理）
#include "buffer/buffer_pool_manager.h"
// 包含通用键头文件：定义数据库索引中通用的键类型（类比：快递包裹上的快递单号格式标准，确保所有包裹的单号遵循统一规则）
#include "storage/index/generic_key.h"

// 定义命名空间bustub：将所有索引相关代码封装在该命名空间下，避免与其他模块命名冲突（类比：公司的部门划分，研发部的代码不会和财务部的代码混淆）
namespace bustub {

// 宏定义：将键值对类型定义为MappingType（类比：给"快递包裹（包含单号和收件人信息）"起一个简称"快递件"，后续使用更简洁）
#define MappingType std::pair<KeyType, ValueType>

// 宏定义：FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
// 用于定义完整的B+树模板参数（包含键类型、值类型、键比较器、墓碑数量）（类比：定义"定制快递服务"的完整参数清单，包括单号格式、收件信息格式、排序规则、异常包裹标记数量）
#define FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN \
  template <typename KeyType, typename ValueType, typename KeyComparator, ssize_t NumTombs = 0>

// 宏定义：FULL_INDEX_TEMPLATE_ARGUMENTS
// 用于声明使用完整模板参数的B+树相关类/函数（类比：在使用"定制快递服务"时，明确告知需要使用之前定义的完整参数清单）
#define FULL_INDEX_TEMPLATE_ARGUMENTS \
  template <typename KeyType, typename ValueType, typename KeyComparator, ssize_t NumTombs>

// 宏定义：INDEX_TEMPLATE_ARGUMENTS
// 用于声明简化版模板参数的B+树相关类/函数（不含墓碑数量，适用于不需要标记异常数据的场景）（类比："标准快递服务"的参数清单，比定制版少了异常包裹标记数量这一项）
#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// 枚举类：定义B+树页面的类型（类比：给快递仓库的货架分类，分为"无效货架"、"存储最终包裹的货架"、"存储货架指引信息的货架"）
enum class IndexPageType {
  INVALID_INDEX_PAGE = 0,  // 无效页面（类比：损坏的、无法使用的货架）
  LEAF_PAGE,               // 叶子页面（类比：直接存放快递包裹的货架，每个包裹都能直接取到）
  INTERNAL_PAGE            // 内部页面（类比：存放"货架指引表"的货架，表中记录"某个范围的单号对应哪个货架"，不直接存包裹）
};

/**
 * BPlusTreePage类：B+树的内部页面和叶子页面的基类
 * 作用：封装两种页面共有的头部信息（类比：所有快递货架都有的"货架信息卡"，记录货架的基本属性，不管是存包裹的货架还是存指引表的货架都需要）
 *
 * 头部格式（单位：字节，共12字节）：
 * ---------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |  ...   |
 * ---------------------------------------------------------
 * 解释：
 * - PageType (4字节)：页面类型（无效/叶子/内部）（类比：货架信息卡上的"货架类型"，标注是存包裹还是存指引表）
 * - CurrentSize (4字节)：当前页面中键值对的数量（类比：货架信息卡上的"当前存放数量"，记录货架上现在有多少个物品）
 * - MaxSize (4字节)：页面最多能容纳的键值对数量（类比：货架信息卡上的"最大容量"，记录货架最多能放多少个物品）
 */
class BPlusTreePage {
 public:
  // 删除默认构造函数、拷贝构造函数和析构函数：
  // 1. 避免默认构造：B+树页面必须通过缓冲池管理器创建（类比：快递货架不能自己凭空出现，必须由仓库管理员（缓冲池）分配）
  // 2. 避免拷贝构造：页面是内存中的唯一实例，不能复制（类比：货架不能复制，仓库里每个货架都是唯一的）
  // 3. 避免自定义析构：页面的内存由缓冲池管理器管理，无需手动释放（类比：货架的销毁由仓库管理员负责，不用自己处理）
  BPlusTreePage() = delete;
  BPlusTreePage(const BPlusTreePage &other) = delete;
  ~BPlusTreePage() = delete;

  /**
   * 判断当前页面是否为叶子页面
   * 逻辑：比较页面类型是否等于LEAF_PAGE
   * 类比：查看货架信息卡上的"货架类型"是否为"存包裹的货架"
   * @return 是叶子页面返回true，否则返回false
   */
  auto IsLeafPage() const -> bool;

  /**
   * 设置当前页面的类型
   * 逻辑：将传入的页面类型赋值给page_type_成员变量
   * 类比：在货架信息卡上填写"货架类型"（比如标记为"存指引表的货架"）
   * @param page_type 要设置的页面类型（无效/叶子/内部）
   */
  void SetPageType(IndexPageType page_type);

  /**
   * 获取当前页面中键值对的数量
   * 逻辑：返回size_成员变量的值
   * 类比：查看货架信息卡上的"当前存放数量"（比如显示当前有5个物品）
   * @return 当前页面的键值对数量
   */
  auto GetSize() const -> int;

  /**
   * 设置当前页面中键值对的数量
   * 逻辑：将传入的数值赋值给size_成员变量
   * 类比：更新货架信息卡上的"当前存放数量"（比如从5个改为6个）
   * @param size 要设置的键值对数量
   */
  void SetSize(int size);

  /**
   * 调整当前页面的键值对数量
   * 逻辑：将size_成员变量加上指定的调整量（可正可负）
   * 类比：货架上增加/减少物品后，更新"当前存放数量"（比如增加2个就加2，减少1个就减1）
   * @param amount 调整量（正数表示增加，负数表示减少）
   */
  void ChangeSizeBy(int amount);

  /**
   * 获取当前页面的最大键值对容量
   * 逻辑：返回max_size_成员变量的值
   * 类比：查看货架信息卡上的"最大容量"（比如显示最多能放10个物品）
   * @return 页面的最大键值对容量
   */
  auto GetMaxSize() const -> int;

  /**
   * 设置当前页面的最大键值对容量
   * 逻辑：将传入的数值赋值给max_size_成员变量
   * 类比：在货架信息卡上设定"最大容量"（比如新货架设定最多放15个物品）
   * @param max_size 要设置的最大容量
   */
  void SetMaxSize(int max_size);

  /**
   * 获取当前页面的最小键值对数量（B+树的特性：页面元素数量不能低于最小值，否则需要合并/ redistribution）
   * 逻辑：根据max_size_计算最小容量（通常为max_size_的一半，向上取整，具体实现需子类或后续补充）
   * 类比：货架的"最低库存"，比如最大容量10的货架，最低要保持5个物品，低于则需要从其他货架调配
   * @return 页面的最小键值对数量
   */
  auto GetMinSize() const -> int;

  void SetPageId(page_id_t id);
  auto GetPageId() const -> page_id_t;

  void SetFatherPageId(page_id_t id);
  auto GetFatherPageId()->page_id_t;
 private:

  // 页面类型：标记当前页面是无效/叶子/内部页面（类比：货架类型标签）
  IndexPageType page_type_;

  // 当前大小：记录页面中已存储的键数量（类比：货架当前存放物品数量）
  int size_;

  // 最大大小：记录页面最多能存储的键值对数量（类比：货架最大能存放物品数量）
  int max_size_;

  page_id_t page_id_;

  page_id_t father_page_id;
};

}  // namespace bustub