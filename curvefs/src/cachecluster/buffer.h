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



namespace curvefs {
namespace cachelib {
namespace detail {

template <typename C>
constexpr uint32_t BufferManager<C>::kMaxBufferCapacity;

template <typename C>
constexpr uint32_t BufferManager<C>::kExpansionFactor;

template <typename C>
BufferAddr BufferManager<C>::allocate(uint32_t size) {
  XDCHECK((*parent_)->hasChainedItem());

  const uint32_t requiredSize = Buffer::getAllocSize(size);
  if (requiredSize >= kMaxBufferCapacity) {
    return nullptr;
  }

  // First we try iterating through all the items in the chain to look for
  // available storage.
  for (uint32_t i = 0; i < buffers_.size(); i++) {
    const auto addr =
        allocateFrom(buffers_[i]->template getMemoryAs<Buffer>(), size, i);
    if (addr) {
      return addr;
    }
  }
  return nullptr;
}

template <typename C>
bool BufferManager<C>::expand(uint32_t size) {
  XDCHECK((*parent_)->hasChainedItem());

  const uint32_t requiredSize = Buffer::getAllocSize(size);
  if (requiredSize > kMaxBufferCapacity) {
    return false;
  }

  Item* itemToExpand = *buffers_.rbegin();
  uint32_t numChainedItems = buffers_.size();

  // For the first item, double the capacity or expand to fit the required size
  auto* expandedBuffer = expandBuffer(*itemToExpand, requiredSize);
  if (expandedBuffer) {
    return true;
  }

  // Ensure we've not reached upper bound of chained items yet
  if (numChainedItems >= BufferAddr::kMaxNumChainedItems) {
    return false;
  }

  // Since we already maxed out at least one chained item, we'll
  // start at max capacity right away
  Buffer* newBuffer = addNewBuffer(kMaxBufferCapacity);

  return newBuffer ? true : false;
}

template <typename C>
void BufferManager<C>::remove(BufferAddr addr) {
  if (!addr) {
    throw std::invalid_argument("cannot remove null address");
  }

  const uint32_t itemOffset = addr.getItemOffset();
  const uint32_t byteOffset = addr.getByteOffset();
  getBuffer(itemOffset)->remove(byteOffset);
}

template <typename C>
template <typename T>
T* BufferManager<C>::get(BufferAddr addr) const {
  if (!addr) {
    throw std::invalid_argument("cannot get null address");
  }
  return reinterpret_cast<T*>(getImpl(addr));
}

template <typename C>
BufferManager<C> BufferManager<C>::clone(WriteHandle& newParent) const {
  for (auto item : buffers_) {
    auto handle = cache_->allocateChainedItem(newParent, item->getSize());
    if (!handle) {
      return BufferManager(nullptr);
    }
    std::memcpy(
        handle->getMemory(), item->getMemory(), cache_->getUsableSize(*item));
    cache_->addChainedItem(newParent, std::move(handle));
  }
  BufferManager<C> newManager(*cache_, newParent);
  return newManager;
}

template <typename C>
void* BufferManager<C>::getImpl(BufferAddr addr) const {
  const uint32_t itemOffset = addr.getItemOffset();
  const uint32_t byteOffset = addr.getByteOffset();
  return getBuffer(itemOffset)->getData(byteOffset);
}

template <typename C>
Buffer* BufferManager<C>::getBuffer(uint32_t index) const {
  return buffers_.at(index)->template getMemoryAs<Buffer>();
}

template <typename C>
void BufferManager<C>::materializeChainedAllocs() {
  // Copy in reverse order since then the index into the vector will line up
  // with our chained item indices given out in BufferAddr
  auto allocs = cache_->viewAsWritableChainedAllocs(*parent_);
  buffers_.clear();
  for (auto& item : allocs.getChain()) {
    buffers_.push_back(&item);
  }
  std::reverse(buffers_.begin(), buffers_.end());
}

template <typename C>
BufferAddr BufferManager<C>::allocateFrom(Buffer* buffer,
                                          uint32_t size,
                                          uint32_t nthChainedItem) {
  if (buffer->canAllocateWithoutCompaction(size)) {
    return BufferAddr{nthChainedItem, buffer->allocate(size)};
  }
  return nullptr;
}

template <typename C>
Buffer* BufferManager<C>::addNewBuffer(uint32_t capacity) {
  auto chainedItem = cache_->allocateChainedItem(
      *parent_, Buffer::computeStorageSize(capacity));
  if (!chainedItem) {
    return nullptr;
  }

  Buffer* buffer = new (chainedItem->getMemory()) Buffer(capacity);
  cache_->addChainedItem(*parent_, std::move(chainedItem));

  materializeChainedAllocs();

  return buffer;
}

template <typename C>
Buffer* BufferManager<C>::expandBuffer(Item& itemToExpand,
                                       uint32_t minAdditionalSize) {
  Buffer* oldBuffer = itemToExpand.template getMemoryAs<Buffer>();

  const uint32_t currentCapacity = oldBuffer->capacity();
  if ((currentCapacity + minAdditionalSize) >= kMaxBufferCapacity) {
    return nullptr;
  }

  // We try to grow by kExpansionFactor unless minAdditionalSize is greater
  // than currentCapacity;
  const uint32_t normalDesiredCapacity =
      std::min(kMaxBufferCapacity, currentCapacity * kExpansionFactor);
  const uint32_t minDesiredCapacity = currentCapacity + minAdditionalSize;
  const uint32_t desiredCapacity =
      std::max(minDesiredCapacity, normalDesiredCapacity);

  auto chainedItem = cache_->allocateChainedItem(
      *parent_, Buffer::computeStorageSize(desiredCapacity));
  if (!chainedItem) {
    return nullptr;
  }

  Buffer* buffer =
      new (chainedItem->getMemory()) Buffer(desiredCapacity, *oldBuffer);
  cache_->replaceChainedItem(itemToExpand, std::move(chainedItem), **parent_);

  materializeChainedAllocs();

  return buffer;
}

template <typename C>
void BufferManager<C>::compact() {
  // O(M + N) where M is the number of allocations in the map to compact
  // and N is the number of chained items to compact.
  auto allocs = cache_->viewAsWritableChainedAllocs(*parent_);
  for (auto& item : allocs.getChain()) {
    Buffer* buffer = item.template getMemoryAs<Buffer>();

    if (buffer->wastedBytes() == 0) {
      continue;
    }

    auto tmpBufferStorage = std::make_unique<uint8_t[]>(item.getSize());
    Buffer* tmpBuffer = new (tmpBufferStorage.get()) Buffer(buffer->capacity());
    XDCHECK_EQ(buffer->capacity(), tmpBuffer->capacity());

    buffer->compact(*tmpBuffer);

    new (buffer) Buffer(tmpBuffer->capacity(), *tmpBuffer);
  }
}

template <typename C>
size_t BufferManager<C>::remainingBytes() const {
  size_t remainingBytes = 0;
  for (const auto& b : buffers_) {
    remainingBytes += b->template getMemoryAs<Buffer>()->remainingBytes();
  }
  return remainingBytes;
}

template <typename C>
size_t BufferManager<C>::wastedBytes() const {
  size_t wastedBytes = 0;
  for (const auto& b : buffers_) {
    wastedBytes += b->template getMemoryAs<Buffer>()->wastedBytes();
  }
  return wastedBytes;
}

template <typename C>
size_t BufferManager<C>::wastedBytesPct() const {
  size_t wastedBytes = 0;
  size_t capacity = 0;
  for (const auto& b : buffers_) {
    Buffer* buffer = b->template getMemoryAs<Buffer>();
    wastedBytes += buffer->wastedBytes();
    capacity += buffer->capacity();
  }
  return wastedBytes * 100 / capacity;
}
} // namespace detail
} // namespace cachelib
} // namespace curvefs
