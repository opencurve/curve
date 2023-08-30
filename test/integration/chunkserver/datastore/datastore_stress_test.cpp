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

const string baseDir = "./data_int_str";    // NOLINT
const string poolDir = "./chunkfilepool_int_str";  // NOLINT
const string poolMetaPath = "./chunkfilepool_int_str.meta";  // NOLINT

class StressTestSuit : public DatastoreIntegrationBase {
 public:
    StressTestSuit() {}
    ~StressTestSuit() {}
};

TEST_F(StressTestSuit, StressTest) {
    InitChunkPool(100);
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[PAGE_SIZE] = {0};

    Atomic<SequenceNum> sn(1);
    static unsigned int seed = 1;

    auto RunRead = [&](int idRange, int loopNum) {
        for (int i = 0; i < loopNum; ++i) {
            ChunkID id = rand_r(&seed) % idRange + 1;
            uint64_t pageIndex = rand_r(&seed) % (CHUNK_SIZE / PAGE_SIZE);
            offset = pageIndex * PAGE_SIZE;
            dataStore_->ReadChunk(id, sn, buf, offset, length);
        }
    };

    auto RunWrite = [&](int idRange, int loopNum) {
        for (int i = 0; i < loopNum; ++i) {
            ChunkID id = rand_r(&seed) % idRange + 1;
            uint64_t pageIndex = rand_r(&seed) % (CHUNK_SIZE / PAGE_SIZE);
            offset = pageIndex * PAGE_SIZE;
            dataStore_->WriteChunk(id, sn, buf, offset, length, nullptr);
        }
    };

    auto RunStress = [&](int threadNum, int rwPercent, int ioNum) {
        uint64_t beginTime = TimeUtility::GetTimeofDayUs();
        Thread *threads = new Thread[threadNum];
        int readThreadNum = threadNum * rwPercent / 100;
        int ioNumAvg = ioNum / threadNum;
        int idRange = 100;
        for (int i = 0; i < readThreadNum; ++i) {
            threads[i] = std::thread(RunRead, idRange, ioNumAvg);
        }

        for (int i = readThreadNum; i < threadNum; ++i) {
            threads[i] = std::thread(RunWrite, idRange, ioNumAvg);
        }

        for (int i = 0; i < threadNum; ++i) {
            threads[i].join();
        }

        uint64_t endTime = TimeUtility::GetTimeofDayUs();
        uint64_t iops = ioNum * 1000000L / (endTime - beginTime);
        printf("Total time used: %lu us\n", endTime - beginTime);
        printf("Thread number: %d\n", threadNum);
        printf("read write percent: %d\n", rwPercent);
        printf("io num: %d\n", ioNum);
        printf("iops: %lu\n", iops);
        delete[] threads;
    };

    printf("===============TEST WRITE==================\n");

    //Testing Single Thread Performance
    RunStress(1, 0, 10000);
    //10 threads
    RunStress(10, 0, 50000);
    //50 threads
    RunStress(50, 0, 100000);

    printf("===============TEST READ==================\n");
    //Testing Single Thread Performance
    RunStress(1, 100, 10000);
    //10 threads
    RunStress(10, 100, 50000);
    //50 threads
    RunStress(50, 100, 100000);

    printf("===============TEST READWRITE==================\n");
    //Testing Single Thread Performance
    RunStress(1, 50, 10000);
    //10 threads
    RunStress(10, 50, 50000);
    //50 threads
    RunStress(50, 50, 100000);
}

}  // namespace chunkserver
}  // namespace curve
