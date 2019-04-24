/*
 * Project: curve
 * Created Date: Tuesday November 27th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef SRC_COMMON_BITMAP_H_
#define SRC_COMMON_BITMAP_H_

#include <stdint.h>
#include <vector>

namespace curve {
namespace common {

using std::vector;

const int BITMAP_UNIT_SIZE = 8;
const int ALIGN_FACTOR = 3;  // 2 ^ ALIGN_FACTOR = BITMAP_UNIT_SIZE

/**
 * 表示bitmap中的一段连续区域，为闭区间
 */
struct BitRange {
    // 连续区域起始位置在bitmap中的索引
    uint32_t beginIndex;
    // 连续区域结束位置在bitmap中的索引
    uint32_t endIndex;
};

class Bitmap {
 public:
    /**
     * 新建bitmap时的构造函数
     * @param bits: 要构造的bitmap的位数
     */
    explicit Bitmap(uint32_t bits);
    /**
     * 从已有的快照文件初始化时的构造函数
     * 构造函数内部会再new一个新的bitmap，然后从参数中的bitmap memcpy过去
     * @param bits: bitmap的位数
     * @param bitmap: 外部提供的用于初始化的bitmap
     */
    explicit Bitmap(uint32_t bits, const char* bitmap);
    virtual ~Bitmap();
    Bitmap() = delete;
    /**
     * 拷贝构造，使用深拷贝
     * @param bitmap：从该对象拷贝内容
     */
    Bitmap(const Bitmap& bitmap);
    /**
     * 赋值函数，使用深拷贝
     * @param bitmap：从该对象拷贝内容
     * @reutrn：返回拷贝后对象引用
     */
    Bitmap& operator = (const Bitmap& bitmap);
    /**
     * 比较两个bitmap是否相同
     * @param bitmap：待比较的bitmap
     * @return：如果相同返回true，如果不同返回false
     */
    bool operator == (const Bitmap& bitmap) const;
    /**
     * 比较两个bitmap是否不同
     * @param bitmap：待比较的bitmap
     * @return：如果不同返回true，如果相同返回false
     */
    bool operator != (const Bitmap& bitmap) const;
    /**
     * 将所有位置1
     */
    void Set();
    /**
     * 将指定位置1
     * @param index: 指定位的位置
     */
    void Set(uint32_t index);
    /**
     * 将指定范围的位置为1
     * @param startIndex: 范围起始位置,包括此位置
     * @param endIndex: 范围结束位置，包括此位置
     */
    void Set(uint32_t startIndex, uint32_t endIndex);
    /**
     * 将所有位置0
     */
    void Clear();
    /**
     * 将指定位置0
     * @param index: 指定位的位置
     */
    void Clear(uint32_t index);
    /**
     * 将指定范围的位置为0
     * @param startIndex: 范围起始位置,包括此位置
     * @param endIndex: 范围结束位置，包括此位置
     */
    void Clear(uint32_t startIndex, uint32_t endIndex);
    /**
     * 获取指定位置位的状态
     * @param index: 指定位的位置
     * @return: true表示当前位状态为1，false表示为0
     */
    bool Test(uint32_t index) const;
    /**
     * 获取指定位置及之后的首个位为1的位置
     * @param index: 指定位的位置，包含此位置
     * @return: 首个位为1的位置，如果不存在返回NO_POS
     */
    uint32_t NextSetBit(uint32_t index) const;
    /**
     * 获取指定起始位置到结束位置之间的的首个位为1的位置
     * @param startIndex: 起始位置，包含此位置
     * @param endIndex: 结束位置，包含此位置
     * @return: 首个位为1的位置，如果指定范围内不存在则返回NO_POS
     */
    uint32_t NextSetBit(uint32_t startIndex, uint32_t endIndex) const;
    /**
     * 获取指定位置及之后的首个位为0的位置
     * @param index: 指定位的位置，包含此位置
     * @return: 首个位为0的位置，如果不存在返回NO_POS
     */
    uint32_t NextClearBit(uint32_t index) const;
    /**
     * 获取指定起始位置到结束位置之间的的首个位为0的位置
     * @param startIndex: 起始位置，包含此位置
     * @param endIndex: 结束位置，包含此位置
     * @return: 首个位为0的位置，如果指定范围内不存在则返回NO_POS
     */
    uint32_t NextClearBit(uint32_t startIndex, uint32_t endIndex) const;
    /**
     * 将bitmap的指定区域分割成若干连续区域，划分依据为位状态，连续区域内的位状态一致
     * 例如：00011100会被划分为三个区域，[0,2]、[3,5]、[6,7]
     * @param startIndex: 指定区域的起始索引
     * @param endIndex: 指定范围的结束索引
     * @param clearRanges: 存放位状态为0的连续区域的向量,可以指定为nullptr
     * @param setRanges: 存放位状态为1的连续区域的向量,可以指定为nullptr
     */
    void Divide(uint32_t startIndex,
                uint32_t endIndex,
                vector<BitRange>* clearRanges,
                vector<BitRange>* setRanges) const;
    /**
     * bitmap的有效位数
     * @return: 返回位数
     */
    uint32_t Size() const;
    /**
     * 获取bitmap的内存指针，用于持久化bitmap
     * @return: bitmap的内存指针
     */
    const char* GetBitmap() const;

 private:
    // bitmap的字节数
    int unitCount() const {
        // 同 (bits_ + BITMAP_UNIT_SIZE - 1) / BITMAP_UNIT_SIZE
        return (bits_ + BITMAP_UNIT_SIZE - 1) >> ALIGN_FACTOR;
    }
    // 指定位置的bit在其所在字节中的偏移
    int indexOfUnit(uint32_t index) const {
        // 同 index / BITMAP_UNIT_SIZE
        return index >> ALIGN_FACTOR;
    }
    // 逻辑计算掩码值
    char mask(uint32_t index) const {
        int indexInUnit =  index % BITMAP_UNIT_SIZE;
        char mask = 0x01 << indexInUnit;
        return mask;
    }

 public:
    // 表示不存在的位置，值为0xffffffff
    static const uint32_t NO_POS;

 private:
    uint32_t    bits_;
    char*       bitmap_;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_BITMAP_H_
