/*
 * Project: curve
 * Created Date: Thursday December 20th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef TEST_CHUNKSERVER_MOCK_CHUNKFILE_POOL_H
#define TEST_CHUNKSERVER_MOCK_CHUNKFILE_POOL_H

#include <gmock/gmock.h>
#include <string>

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

#endif  // TEST_CHUNKSERVER_MOCK_CHUNKFILE_POOL_H
