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
 * Created Date: Saturday January 5th 2019
 * Author: yangyaokai
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

//TODO: this unit test is suitable for the new version of datastore
//      and will be modified later dxiang@corp.netease.com
#if 0
TEST_F(ConcurrencyTestSuit, ConcurrencyTest) {
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[PAGE_SIZE] = {0};
    Atomic<SequenceNum> sn(1);
    static unsigned int seed = 1;

    uint64_t fileId = 1;
    uint64_t cloneFileId = 2;
    std::vector<SequenceNum> snaps;
    snaps.clear();

    std::shared_ptr<SnapContext> context = std::make_shared<SnapContext>(snaps);

    CloneContext clonectx;
    clonectx.rootId = fileId;
    std::vector<struct CloneInfos> infos;
    infos.clear();

    const int kLoopNum = 10;
    const int kThreadNum = 10;

    auto readFunc = [&](ChunkID id) {
        // 五分之一概率增加版本号
        if (rand_r(&seed) % 5 == 0) {
            snaps.push_back(sn);
            ++sn;
            context = std::make_shared<SnapContext>(snaps);
        }
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
        dataStore_->DeleteSnapshotChunk(id, sn);
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
#endif

}  // namespace chunkserver
}  // namespace curve
