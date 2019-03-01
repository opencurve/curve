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

int Bitmap::END_POSITION = -1;

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
        index = END_POSITION;
    return index;
}

uint32_t Bitmap::NextClearBit(uint32_t index) const {
    for (; index < bits_; ++index) {
        if (!Test(index))
            break;
    }
    if (index >= bits_)
        index = END_POSITION;
    return index;
}
uint32_t Bitmap::Size() const {
    return bits_;
}

const char* Bitmap::GetBitmap() {
    return bitmap_;
}

}  // namespace common
}  // namespace curve
