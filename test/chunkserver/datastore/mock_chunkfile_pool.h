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
 * Created Date: Thursday December 20th 2018
 * Author: yangyaokai
 */

#ifndef TEST_CHUNKSERVER_DATASTORE_MOCK_CHUNKFILE_POOL_H_
#define TEST_CHUNKSERVER_DATASTORE_MOCK_CHUNKFILE_POOL_H_

#include <gmock/gmock.h>
#include <string>
#include <memory>

#include "src/chunkserver/datastore/chunkfile_pool.h"

namespace curve {
namespace chunkserver {

class MockChunkfilePool : public ChunkfilePool {
 public:
    explicit MockChunkfilePool(std::shared_ptr<LocalFileSystem> lfs)
        : ChunkfilePool(lfs) {}
    ~MockChunkfilePool() {}
    MOCK_METHOD1(Initialize, bool(ChunkfilePoolOptions));
    MOCK_METHOD2(GetChunk, int(const std::string&, char*));
    MOCK_METHOD1(RecycleChunk, int(const std::string&  chunkpath));
    MOCK_METHOD0(UnInitialize, void());
    MOCK_METHOD0(Size, size_t());
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_DATASTORE_MOCK_CHUNKFILE_POOL_H_
