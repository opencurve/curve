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

#ifndef CURVEFS_SRC_SPACE_ALLOCATOR_S3_ALLOCATOR_H_
#define CURVEFS_SRC_SPACE_ALLOCATOR_S3_ALLOCATOR_H_

#include <atomic>

namespace curvefs {
namespace space {

class S3Allocator {
 public:
    explicit S3Allocator(uint64_t startChunkId) : chunkId_(startChunkId) {}

    uint64_t NextChunkId() {
        auto id = chunkId_.fetch_add(1, std::memory_order_relaxed);
        return id;
    }

 private:
    std::atomic<uint64_t> chunkId_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_ALLOCATOR_S3_ALLOCATOR_H_
