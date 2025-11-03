//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.h
//
// Identification: src/include/storage/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// 编译指令：确保当前头文件只被编译一次，避免重复包含（类似给一本书盖个章，确保同一本书不会被多次复印）
#pragma once

// 包含标准库头文件：queue（队列，暂未在本类直接使用，可能为后续扩展预留）、string（字符串，用于ToString方法）
#include <queue>
#include <string>

// 包含自定义头文件：B+树页面的基类，当前内部页类会继承这个基类（类似先学习"图书管理基础"，再学习"小说类图书管理"）
#include "storage/page/b_plus_tree_page.h"

// 定义命名空间bustub：将所有相关代码封装在这个空间下，避免与其他代码命名冲突（类似给图书分类到"科技类-计算机-数据库"专区）
namespace bustub {

// 宏定义：给BPlusTreeInternalPage类起一个简短别名，方便后续使用（类似给"北京大学计算机科学与技术学院"简称"北大计算机学院"）
#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
// 宏定义：B+树内部页的头部大小为12字节（类似图书的"前言+目录"固定占12页，这部分不存储实际内容）
#define INTERNAL_PAGE_HEADER_SIZE 12
// 宏定义：计算内部页能存储的"键-值（子页指针）"对数量（SLOT_CNT）
// 计算逻辑：(页面总大小 - 头部大小) / 每个槽位的大小（键的大小 + 子页指针的大小）
// 类比：(一本书总页数 - 前言目录页数) / 每篇文章占用的页数，得到这本书能放多少篇文章
#define INTERNAL_PAGE_SLOT_CNT \
  ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / ((int)(sizeof(KeyType) + sizeof(ValueType))))  // NOLINT

/**
 * B+树内部页的功能说明：
 * 1. 存储n个索引键（Key）和n+1个子页指针（ValueType，实际是page_id）
 * 2. 子页指针的规则：PAGE_ID(i)指向的子树中，所有键K满足 K(i) ≤ K < K(i+1)
 *    类比：图书馆的"小说分类架"（内部页），每个架子标签（Key）对应一个子书架（子页），
 *          比如标签"2000-2010"对应子书架A，意味着A里所有小说的出版年份≥2000且<2010
 * 3. 关键注意点：键的数量≠子页指针数量（键n个，指针n+1个），所以key_array_的第一个键永远无效
 *    类比：分类架上有3个标签（键：2000-2010、2010-2020、2020-2030），但对应4个子书架（指针），
 *          第一个标签前其实还有一个"默认子书架"（放2000年前的书），所以第一个标签位置留空（无效）
 *
 * 内部页的存储格式（键按升序排列）：
 *  ---------
 * | 头部区域 |  // 固定12字节，存储页面元数据（如页面类型、当前存储的键数量等，类似图书的"版权页"）
 *  ---------
 *  ------------------------------------------
 * | 键1（无效） | 键2 | ... | 键n |  // 键数组，从第二个键开始有效（类似分类架的标签，第一个标签位留空）
 *  ------------------------------------------
 *  ---------------------------------------------
 * | 子页指针1 | 子页指针2 | ... | 子页指针n+1 |  // 子页指针数组，每个指针对应一个子页（类似标签对应的子书架位置）
 *  ---------------------------------------------
 */
// 定义B+树内部页类：继承自BPlusTreePage基类，使用模板（KeyType：键类型，ValueType：子页指针类型，KeyComparator：键比较器）
// 类比：定义"小说分类架"类，继承"通用书架"基类，支持不同标签类型（如年份、作者名）、不同子书架标识（如编号、位置）
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // 删除默认构造函数和拷贝构造函数：确保只能通过Init方法初始化，避免内存安全问题
  // 类比：禁止直接"创建分类架"，必须通过"分类架初始化流程"（如设置标签数量、子书架位置）创建，防止架子没搭好就用
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  /**
   * 初始化内部页：设置页面的最大存储槽位数量（默认是INTERNAL_PAGE_SLOT_CNT）
   * 类比：搭建分类架时，先确定这个架子最多能放多少个"标签-子书架"对（比如最多放50个）
   * @param max_size 最大槽位数量，默认使用宏定义的计算值
   */
  void Init(int max_size = INTERNAL_PAGE_SLOT_CNT);

  /**
   * 获取指定索引位置的键
   * 类比：根据分类架上的标签位置（如第3个位置），取出对应的标签内容（如"2010-2020"）
   * @param index 键的索引（注意：index=0时返回的键无效）
   * @return 对应索引的键值
   */
  auto KeyAt(int index) const -> KeyType;

  /**
   * 在指定索引位置设置键
   * 类比：在分类架的第3个标签位置，贴上"2010-2020"的新标签（覆盖旧标签）
   * @param index 键的索引（注意：index=0的位置设置了也无效）
   * @param key 要设置的键值
   */
  void SetKeyAt(int index, const KeyType &key);

  /**
   * 查找指定子页指针在指针数组中的索引
   * 类比：已知一个子书架的编号（如"子架A"），找到它在分类架的指针列表中处于第几个位置
   * @param value 要查找的子页指针（ValueType类型，实际是page_id）
   * @return 该指针在page_id_array_中的索引；若未找到，可能返回-1或其他无效值（取决于具体实现）
   */
  auto ValueIndex(const ValueType &value) const -> int;

  /**
   * 获取指定索引位置的子页指针
   * 类比：根据分类架上的指针位置（如第2个位置），找到对应的子书架编号（如"子架B"）
   * @param index 子页指针的索引
   * @return 对应索引的子页指针（page_id）
   */
  auto ValueAt(int index) const -> ValueType;
  auto BinarySearch(const KeyComparator &comparator, const KeyType &key) -> int;

  /**
   * 仅用于测试：将当前内部页的所有有效键转换为字符串，格式为"(key1,key2,key3,...)"
   * 类比：把分类架上所有有效标签（跳过第一个无效标签）整理成字符串，如"(2000-2010,2010-2020,2020-2030)"
   * @return 包含所有有效键的字符串
   */
  auto ToString() const -> std::string {
    // 初始化字符串，开头加"("（类似整理标签时先写左括号）
    std::string kstr = "(";
    // 标记是否为第一个有效键（用于控制是否加逗号）
    bool first = true;

    // 循环遍历所有有效键：从索引1开始（跳过索引0的无效键），到当前页面存储的键数量结束
    // 类比：从分类架的第2个标签开始，到最后一个有效标签结束
    for (int i = 1; i < GetSize(); i++) {
      // 获取当前索引的键（如"2010-2020"）
      KeyType key = KeyAt(i);
      // 如果是第一个有效键，不需要加逗号；否则先加逗号（避免开头或结尾有多余逗号）
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      // 将当前键转换为字符串，添加到结果中（如把"2010-2020"拼接到"(2000-2010,"后面）
      kstr.append(std::to_string(key.ToString()));
    }
    // 字符串结尾加")"（类似整理标签时最后写右括号）
    kstr.append(")");

    // 返回最终的字符串（如"(2000-2010,2010-2020,2020-2030)"）
    return kstr;
  }

  void FirstInsert(const KeyType& key, const ValueType& left_page_id, const ValueType& right_page_id);

  bool InsertKeyValue(const KeyComparator &comparator, const KeyType &key,
                      const ValueType &value);
  auto Find(const KeyComparator& comparator, const KeyType& key) ->page_id_t;



  KeyType Split(B_PLUS_TREE_INTERNAL_PAGE_TYPE*new_internal_page, std::vector<page_id_t>& v);
 private:
  // 存储键的数组：大小为最大槽位数量（INTERNAL_PAGE_SLOT_CNT）
  // 类比：分类架上的标签列表，最多能放INTERNAL_PAGE_SLOT_CNT个标签（第一个标签位无效）
  KeyType key_array_[INTERNAL_PAGE_SLOT_CNT];
  // 存储子页指针的数组：大小与键数组相同（因为n个键对应n+1个指针，数组大小足够容纳n+1个指针）
  // 类比：分类架上的子书架编号列表，最多能放INTERNAL_PAGE_SLOT_CNT个子书架编号（满足n+1个指针的需求）
  ValueType page_id_array_[INTERNAL_PAGE_SLOT_CNT];
  // 预留扩展字段：2025年春季学期可根据需求添加更多字段或辅助函数（类似分类架预留一些空间，后续可加新功能）
  // (Spring 2025) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub  // 命名空间结束
