/*
 * Project: curve
 * Created Date: Tuesday November 27th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_COMMON_BITMAP_H
#define CURVE_COMMON_BITMAP_H

#include <stdint.h>

namespace curve {
namespace common {

const int BITMAP_UNIT_SIZE = 8;
const int ALIGN_FACTOR = 3;  // 2 ^ ALIGN_FACTOR = BITMAP_UNIT_SIZE

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
     * @return: 首个位为1的位置，如果不存在返回END_POSITION
     */
    uint32_t NextSetBit(uint32_t index) const;
    /**
     * 获取指定位置及之后的首个位为0的位置
     * @param index: 指定位的位置，包含此位置
     * @return: 首个位为0的位置，如果不存在返回END_POSITION
     */
    uint32_t NextClearBit(uint32_t index) const;
    /**
     * bitmap的有效位数
     * @return: 返回位数
     */
    uint32_t Size() const;
    /**
     * 获取bitmap的内存指针，用于持久化bitmap
     * @return: bitmap的内存指针
     */
    const char* GetBitmap();

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
    static int END_POSITION;

 private:
    uint32_t    bits_;
    char*       bitmap_;
};

}  // namespace common
}  // namespace curve

#endif  // CURVE_COMMON_BITMAP_H
