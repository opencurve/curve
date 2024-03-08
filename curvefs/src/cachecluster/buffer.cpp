/*
 *  Copyright (c) 2021 NetEase Inc.
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


#include "cachelib/datatype/Buffer.h"

namespace curvefs {
namespace cachelib {
namespace detail {
constexpr uint32_t Buffer::kInvalidOffset;

bool Buffer::canAllocate(uint32_t size) const {
  return Slot::getAllocSize(size) <= (remainingBytes() + wastedBytes());
}

bool Buffer::canAllocateWithoutCompaction(uint32_t size) const {
  return Slot::getAllocSize(size) <= remainingBytes();
}

Buffer::Buffer(uint32_t capacity, const Buffer& other)
    : capacity_(capacity),
      deletedBytes_(other.deletedBytes_),
      nextByte_(other.nextByte_) {
  std::memcpy(&data_, &other.data_, other.nextByte_);
}

uint32_t Buffer::allocate(uint32_t size) {
  const uint32_t allocSize = static_cast<uint32_t>(sizeof(Slot)) + size;
  if (nextByte_ + allocSize > capacity_) {
    return kInvalidOffset;
  }

  auto* slot = new (&data_[nextByte_]) Slot(size);
  XDCHECK_EQ(allocSize, slot->getAllocSize());

  nextByte_ += slot->getAllocSize();

  const uint32_t offset = static_cast<uint32_t>(
      reinterpret_cast<uint8_t*>(slot->getData()) - data_);
  return offset;
}

void Buffer::remove(uint32_t offset) {
  auto* slot = getSlot(offset);
  if (slot && !slot->isRemoved()) {
    slot->markRemoved();
    deletedBytes_ += slot->getAllocSize();
  }
}

void* Buffer::getData(uint32_t offset) { return data_ + offset; }

const void* Buffer::getData(uint32_t offset) const { return data_ + offset; }

void Buffer::compact(Buffer& dest) const {
  XDCHECK_EQ(dest.capacity(), dest.remainingBytes());
  if (dest.capacity() < capacity() - wastedBytes() - remainingBytes()) {
    throw std::invalid_argument(folly::sformat(
        "destination buffer is too small. Dest Capacity: {}, "
        "Current Capacity: {}, Current Wasted Space: {}, Current Remaining "
        "Capacity: {}",
        dest.capacity(), capacity(), wastedBytes(), remainingBytes()));
  }

  for (uint32_t srcOffset = 0; srcOffset < nextByte_;) {
    auto* slot = reinterpret_cast<const Slot*>(&data_[srcOffset]);
    if (!slot->isRemoved()) {
      const uint32_t offset = dest.allocate(slot->getSize());
      XDCHECK_NE(kInvalidOffset, offset);

      std::memcpy(dest.getData(offset), slot->getData(), slot->getSize());
    }

    srcOffset += slot->getAllocSize();
  }
}

Buffer::Slot* Buffer::getSlot(uint32_t dataOffset) {
  return const_cast<Slot*>(getSlotImpl(dataOffset));
}

const Buffer::Slot* Buffer::getSlot(uint32_t dataOffset) const {
  return getSlotImpl(dataOffset);
}

const Buffer::Slot* Buffer::getSlotImpl(uint32_t dataOffset) const {
  if (dataOffset >= nextByte_ || dataOffset < sizeof(Slot)) {
    // Need this to compile due to alignment requirement for uint32_t& in
    // folly::sformat()
    const auto tmp = nextByte_;
    throw std::invalid_argument(folly::sformat(
        "invliad dataOffset. dataOffset: {}, nextByte: {}, sizeof(Slot): {}",
        dataOffset, tmp, sizeof(Slot)));
  }
  const uint32_t slotOffset = dataOffset - static_cast<uint32_t>(sizeof(Slot));
  const auto* slot = reinterpret_cast<const Slot*>(&data_[slotOffset]);
  return slot;
}

Buffer::Slot* Buffer::getFirstSlot() {
  if (nextByte_ < Slot::getAllocSize(0)) {
    return nullptr;
  }
  return getSlot(Slot::getAllocSize(0));
}

Buffer::Slot* Buffer::getNextSlot(const Slot& curSlot) {
  const uint32_t curDataOffset = getDataOffset(curSlot);
  const uint32_t nextDataOffset = curDataOffset + curSlot.getAllocSize();
  if (nextDataOffset >= nextByte_) {
    return nullptr;
  }

  return getSlot(nextDataOffset);
}

uint32_t Buffer::getDataOffset(const Slot& slot) const {
  return static_cast<uint32_t>(
      reinterpret_cast<const uint8_t*>(slot.getData()) -
      reinterpret_cast<const uint8_t*>(data_));
}

constexpr uint32_t BufferAddr::kInvalidOffset;
constexpr uint32_t BufferAddr::kByteOffsetBits;
constexpr uint32_t BufferAddr::kByteOffsetMask;
constexpr uint32_t BufferAddr::kMaxNumChainedItems;
} // namespace detail
} // namespace cachelib
} // namespace curvefs