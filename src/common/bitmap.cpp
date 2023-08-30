/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Wednesday November 28th 2018
 * Author: yangyaokai
 */

#include <glog/logging.h>
#include <memory.h>
#include <utility>
#include <string>
#include "src/common/bitmap.h"

namespace curve {
namespace common {

std::string BitRangeVecToString(const std::vector<BitRange> &ranges) {
    std::stringstream ss;
    for (uint32_t i = 0; i < ranges.size(); ++i) {
        if (i != 0) {
            ss <<  ", ";
        }
        ss << "(" << ranges[i].beginIndex << "," << ranges[i].endIndex << ")";
    }
    return ss.str();
}

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

Bitmap::Bitmap(uint32_t bits, char* bitmap, bool transfer) : bits_(bits) {
    int count = unitCount();

    if (!transfer) {
        bitmap_ = new(std::nothrow) char[count];
        CHECK(bitmap_ != nullptr) << "allocate bitmap failed.";
        if (bitmap != nullptr) {
            memcpy(bitmap_, bitmap, count);
        } else {
            memset(bitmap_, 0, count);
        }
    } else {
        bitmap_ = bitmap;
        CHECK(bitmap_ != nullptr) << "transfer bitmap with nullptr is invalid";
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

Bitmap::Bitmap(Bitmap&& other) noexcept
    : bits_(other.bits_), bitmap_(other.bitmap_) {
    other.bits_ = 0;
    other.bitmap_ = nullptr;
}

Bitmap& Bitmap::operator=(Bitmap&& other) noexcept {
    using std::swap;
    swap(bits_, other.bits_);
    swap(bitmap_, other.bitmap_);

    return *this;
}

bool Bitmap::operator == (const Bitmap& bitmap) const {
    if (bits_ != bitmap.Size())
        return false;
    return 0 == memcmp(bitmap_, bitmap.GetBitmap(), unitCount());
}

bool Bitmap::operator != (const Bitmap& bitmap) const {
    return !(*this == bitmap);
}

void Bitmap::Set() {
    memset(bitmap_, 0xff, unitCount());
}

void Bitmap::Set(uint32_t index) {
    if (index < bits_)
        bitmap_[indexOfUnit(index)] |= mask(index);
}

void Bitmap::Set(uint32_t startIndex, uint32_t endIndex) {
    // TODO(wuhanqing): implement fast algorithm if one byte is all set
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
    // TODO(wuhanqing): implement fast algorithm if one byte is all clear
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
    //The index value of the last bit in the bitmap
    uint32_t lastIndex = bits_ - 1;
    //The endIndex value cannot exceed lastIndex
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
    //The endIndex value cannot exceed lastIndex
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
    //The value of endIndex cannot be less than startIndex
    if (endIndex < startIndex)
        return;

    //The endIndex value cannot exceed lastIndex
    uint32_t lastIndex = bits_ - 1;
    if (endIndex > lastIndex)
        endIndex = lastIndex;

    BitRange clearRange;
    BitRange setRange;
    vector<BitRange> tmpClearRanges;
    vector<BitRange> tmpSetRanges;
    //Next index with 0 bits
    uint32_t nextClearIndex;
    //Next index with bit 1
    uint32_t nextSetIndex;

    //Divide all ranges
    while (startIndex != NO_POS) {
        nextClearIndex = NextClearBit(startIndex, endIndex);
        //1. Store the set range before the current clear index
        //If nextClearIndex is equal to startIndex, it indicates that there is no set range before it
        if (nextClearIndex != startIndex) {
            setRange.beginIndex = startIndex;
            //NextClearIndex equals NO_ POS description has found the end
            //The last continuous area is set range
            setRange.endIndex = nextClearIndex == NO_POS
                              ? endIndex
                              : nextClearIndex - 1;
            tmpSetRanges.push_back(setRange);
        }
        if (nextClearIndex == NO_POS)
            break;

        nextSetIndex = NextSetBit(nextClearIndex, endIndex);
        //2. Store the clear range before the current set index
        //Being able to reach this step indicates that there must be a clear range ahead, so there is no need to make a judgment like in step 1
        clearRange.beginIndex = nextClearIndex;
        clearRange.endIndex = nextSetIndex == NO_POS
                            ? endIndex
                            : nextSetIndex - 1;
        tmpClearRanges.push_back(clearRange);
        startIndex = nextSetIndex;
    }

    //Returns a result based on whether the clearRanges and setRanges pointers in the parameters are empty
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
