/*
 * Project: curve
 * Created Date: Wednesday November 28th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <memory.h>
#include "src/common/bitmap.h"

namespace curve {
namespace common {

const uint32_t Bitmap::NO_POS = 0xFFFFFFFF;

Bitmap::Bitmap(uint32_t bits) : bits_(bits) {
    int count = unitCount();
    bitmap_ = new(std::nothrow) char[count];
    CHECK(bitmap_ != nullptr) << "allocate bitmap failed.";
    memset(bitmap_, 0, count);
}

Bitmap::Bitmap(uint32_t bits, const char* bitmap) : bits_(bits) {
    int count = unitCount();
    bitmap_ = new(std::nothrow) char[count];
    CHECK(bitmap_ != nullptr) << "allocate bitmap failed.";
    if (bitmap != nullptr) {
        memcpy(bitmap_, bitmap, count);
    } else {
        memset(bitmap_, 0, count);
    }
}

Bitmap::~Bitmap() {
    if (nullptr != bitmap_) {
        delete[] bitmap_;
        bitmap_ = nullptr;
    }
}

Bitmap::Bitmap(const Bitmap& bitmap) {
    bits_ = bitmap.Size();
    int count = unitCount();
    bitmap_ = new(std::nothrow) char[count];
    CHECK(bitmap_ != nullptr) << "allocate bitmap failed.";
    memcpy(bitmap_, bitmap.GetBitmap(), count);
}

Bitmap& Bitmap::operator = (const Bitmap& bitmap) {
    if (this == &bitmap)
        return *this;
    delete[] bitmap_;
    bits_ = bitmap.Size();
    int count = unitCount();
    bitmap_ = new(std::nothrow) char[count];
    CHECK(bitmap_ != nullptr) << "allocate bitmap failed.";
    memcpy(bitmap_, bitmap.GetBitmap(), count);
    return *this;
}

void Bitmap::Set() {
    memset(bitmap_, 0xff, unitCount());
}

void Bitmap::Set(uint32_t index) {
    if (index < bits_)
        bitmap_[indexOfUnit(index)] |= mask(index);
}

void Bitmap::Set(uint32_t startIndex, uint32_t endIndex) {
    for (uint32_t index = startIndex; index <= endIndex; ++index) {
        Set(index);
    }
}

void Bitmap::Clear() {
    memset(bitmap_, 0, unitCount());
}

void Bitmap::Clear(uint32_t index) {
    if (index < bits_)
        bitmap_[indexOfUnit(index)] &= ~mask(index);
}

void Bitmap::Clear(uint32_t startIndex, uint32_t endIndex) {
    for (uint32_t index = startIndex; index <= endIndex; ++index) {
        Clear(index);
    }
}

bool Bitmap::Test(uint32_t index) const {
    if (index < bits_)
        return bitmap_[indexOfUnit(index)] & mask(index);
    else
        return false;
}

uint32_t Bitmap::NextSetBit(uint32_t index) const {
    for (; index < bits_; ++index) {
        if (Test(index))
            break;
    }
    if (index >= bits_)
        index = NO_POS;
    return index;
}

uint32_t Bitmap::NextSetBit(uint32_t startIndex, uint32_t endIndex) const {
    uint32_t index = startIndex;
    // bitmap中最后一个bit的index值
    uint32_t lastIndex = bits_ - 1;
    // endIndex值不能超过lastIndex
    if (endIndex > lastIndex)
        endIndex = lastIndex;
    for (; index <= endIndex; ++index) {
        if (Test(index))
            break;
    }
    if (index > endIndex)
        index = NO_POS;
    return index;
}

uint32_t Bitmap::NextClearBit(uint32_t index) const {
    for (; index < bits_; ++index) {
        if (!Test(index))
            break;
    }
    if (index >= bits_)
        index = NO_POS;
    return index;
}

uint32_t Bitmap::NextClearBit(uint32_t startIndex, uint32_t endIndex) const {
    uint32_t index = startIndex;
    uint32_t lastIndex = bits_ - 1;
    // endIndex值不能超过lastIndex
    if (endIndex > lastIndex)
        endIndex = lastIndex;
    for (; index <= endIndex; ++index) {
        if (!Test(index))
            break;
    }
    if (index > endIndex)
        index = NO_POS;
    return index;
}

void Bitmap::Divide(uint32_t startIndex,
                    uint32_t endIndex,
                    vector<BitRange>* clearRanges,
                    vector<BitRange>* setRanges) const {
    // endIndex的值不能小于startIndex
    if (endIndex < startIndex)
        return;

    // endIndex值不能超过lastIndex
    uint32_t lastIndex = bits_ - 1;
    if (endIndex > lastIndex)
        endIndex = lastIndex;

    BitRange clearRange;
    BitRange setRange;
    vector<BitRange> tmpClearRanges;
    vector<BitRange> tmpSetRanges;
    // 下一个位为0的index
    uint32_t nextClearIndex;
    // 下一个位为1的index
    uint32_t nextSetIndex;

    // 划分所有range
    while (startIndex != NO_POS) {
        nextClearIndex = NextClearBit(startIndex, endIndex);
        // 1.存放当前clear index之前的 set range
        //   nextClearIndex如果等于startIndex说明前面没有 set range
        if (nextClearIndex != startIndex) {
            setRange.beginIndex = startIndex;
            // nextClearIndex等于NO_POS说明已经找到末尾
            // 最后一块连续区域是 set range
            setRange.endIndex = nextClearIndex == NO_POS
                              ? endIndex
                              : nextClearIndex - 1;
            tmpSetRanges.push_back(setRange);
        }
        if (nextClearIndex == NO_POS)
            break;

        nextSetIndex = NextSetBit(nextClearIndex, endIndex);
        // 2.存放当前set index之前的 clear range
        //   能到这一步说明前面肯定存在clear range，所以不用像第1步一样做判断
        clearRange.beginIndex = nextClearIndex;
        clearRange.endIndex = nextSetIndex == NO_POS
                            ? endIndex
                            : nextSetIndex - 1;
        tmpClearRanges.push_back(clearRange);
        startIndex = nextSetIndex;
    }

    // 根据参数中的clearRanges和setRanges指针是否为空返回结果
    if (clearRanges != nullptr) {
        *clearRanges = std::move(tmpClearRanges);
    }
    if (setRanges != nullptr) {
        *setRanges = std::move(tmpSetRanges);
    }
}

uint32_t Bitmap::Size() const {
    return bits_;
}

const char* Bitmap::GetBitmap() const {
    return bitmap_;
}

}  // namespace common
}  // namespace curve
