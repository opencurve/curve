/*
 * Project: curve
 * Created Date: Saturday January 5th 2019
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include "test/integration/chunkserver/datastore/datastore_integration_base.h"

namespace curve {
namespace chunkserver {

const string baseDir = "./data_int_con";    // NOLINT
const string poolDir = "./chunkfilepool_int_con";  // NOLINT
const string poolMetaPath = "./chunkfilepool_int_con.meta";  // NOLINT

class ConcurrencyTestSuit : public DatastoreIntegrationBase {
 public:
    ConcurrencyTestSuit() {}
    ~ConcurrencyTestSuit() {}
};

TEST_F(ConcurrencyTestSuit, ConcurrencyTest) {
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[PAGE_SIZE] = {0};
    Atomic<SequenceNum> sn(1);
    static unsigned int seed = 1;

    const int kLoopNum = 10;
    const int kThreadNum = 10;

    auto readFunc = [&](ChunkID id) {
        // 五分之一概率增加版本号
        if (rand_r(&seed) % 5 == 0)
            ++sn;
        uint64_t pageIndex = rand_r(&seed) % (CHUNK_SIZE / PAGE_SIZE);
        offset = pageIndex * PAGE_SIZE;
        dataStore_->ReadChunk(id, sn, buf, offset, length);
    };

    auto writeFunc = [&](ChunkID id) {
        uint64_t pageIndex = rand_r(&seed) % (CHUNK_SIZE / PAGE_SIZE);
        offset = pageIndex * PAGE_SIZE;
        dataStore_->WriteChunk(id, sn, buf, offset, length, nullptr);
    };

    auto deleteFunc = [&](ChunkID id) {
        dataStore_->DeleteChunk(id, sn);
    };

    auto deleteSnapFunc = [&](ChunkID id) {
        dataStore_->DeleteSnapshotChunkOrCorrectSn(id, sn);
    };

    auto readSnapFunc = [&](ChunkID id) {
        dataStore_->ReadSnapshotChunk(id, sn, buf, offset, length);
    };

    auto createCloneFunc = [&](ChunkID id) {
        dataStore_->CreateCloneChunk(id, sn, 0, CHUNK_SIZE, "test@cs");
    };

    auto pasteFunc = [&](ChunkID id) {
        dataStore_->PasteChunk(id, buf, offset, length);
    };

    auto getInfoFunc = [&](ChunkID id) {
        CSChunkInfo info;
        dataStore_->GetChunkInfo(id, &info);
    };

    vector<std::function<void(ChunkID)>> funcs;
    funcs.push_back(readFunc);
    funcs.push_back(writeFunc);
    funcs.push_back(deleteFunc);
    funcs.push_back(deleteSnapFunc);
    funcs.push_back(readSnapFunc);
    funcs.push_back(createCloneFunc);
    funcs.push_back(pasteFunc);
    funcs.push_back(getInfoFunc);

    auto Run = [&](int idRange, int loopNum) {
        for (int i = 0; i < loopNum; ++i) {
            ChunkID id = rand_r(&seed) % idRange + 1;
            int funcSize = funcs.size();
            auto randFunc = funcs.at(rand_r(&seed) % funcSize);
            randFunc(id);
        }
    };

    Thread threads[kThreadNum];

    printf("===============TEST CHUNK1===================\n");
    // 测试并发对同一chunk进行随机操作
    for (int i = 0; i < kThreadNum; ++i) {
        threads[i] = std::thread(Run, 1, kLoopNum);
    }

    for (auto& t : threads) {
        t.join();
    }

    printf("===============TEST RANDOM==================\n");

    // 测试并发对不同chunk进行随机操作
    int idRange = 10;
    for (int i = 0; i < kThreadNum; ++i) {
        threads[i] = std::thread(Run, idRange, kLoopNum);
    }

    for (auto& t : threads) {
        t.join();
    }
}

}  // namespace chunkserver
}  // namespace curve
