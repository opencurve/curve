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
 * File Created: 20200817
 * Author: lixiaocui
 */

#include <gtest/gtest.h>

#include <atomic>
#include <functional>

#include "proto/chunk.pb.h"
#include "src/common/timeutility.h"
#include "src/chunkserver/concurrent_apply/concurrent_apply.h"

using curve::chunkserver::concurrent::ConcurrentApplyModule;
using curve::chunkserver::concurrent::ConcurrentApplyOption;
using curve::chunkserver::CHUNK_OP_TYPE;

TEST(ConcurrentApplyModule, InitTest) {
    ConcurrentApplyModule concurrentapply;

    {
        // 1. init with invalid write-concurrentsize
        ConcurrentApplyOption opt{-1, 1, 1, 1};
        ASSERT_FALSE(concurrentapply.Init(opt));
    }

    {
        // 2. init with invalid write-concurrentdepth
        ConcurrentApplyOption opt{1, -1, 1, 1};
        ASSERT_FALSE(concurrentapply.Init(opt));
    }

    {
        // 3. init with invalid read-concurrentsize
        ConcurrentApplyOption opt{1, 1, -1, 1};
        ASSERT_FALSE(concurrentapply.Init(opt));
    }

    {
        // 4. init with invalid read-concurrentdepth
        ConcurrentApplyOption opt{1, 1, 1, -1};
        ASSERT_FALSE(concurrentapply.Init(opt));
    }

    {
        // 5. double init
        ConcurrentApplyOption opt{1, 1, 1, 1};
        // 5. init with vaild params
        ASSERT_TRUE(concurrentapply.Init(opt));
    }

    concurrentapply.Stop();
}


TEST(ConcurrentApplyModule, RunTest) {
    ConcurrentApplyModule concurrentapply;

    int testw = 0;
    auto wtask = [&testw]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        testw++;
    };

    int testr = 0;
    auto rtask = [&testr]() {
        testr++;
    };

    // push write and read tasks
    ConcurrentApplyOption opt{1, 1, 1, 1};
    ASSERT_TRUE(concurrentapply.Init(opt));

    ASSERT_TRUE(concurrentapply.Push(1, CHUNK_OP_TYPE::CHUNK_OP_READ, rtask));
    ASSERT_TRUE(concurrentapply.Push(1, CHUNK_OP_TYPE::CHUNK_OP_WRITE, wtask));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(1, testr);
    ASSERT_EQ(0, testw);
    concurrentapply.Flush();
    ASSERT_EQ(1, testw);

    concurrentapply.Stop();
}

TEST(ConcurrentApplyModule, FlushTest) {
    ConcurrentApplyModule concurrentapply;
    ConcurrentApplyOption opt{2, 5000, 1, 1};
    ASSERT_TRUE(concurrentapply.Init(opt));

    std::atomic<uint32_t> testnum(0);
    auto task = [&testnum]() {
        testnum.fetch_add(1);
    };

    for (int i = 0; i < 5000; i++) {
        concurrentapply.Push(i, CHUNK_OP_TYPE::CHUNK_OP_WRITE, task);
    }

    ASSERT_LT(testnum, 5000);
    concurrentapply.Flush();
    ASSERT_EQ(5000, testnum);

    concurrentapply.Stop();
}

TEST(ConcurrentApplyModule, ConcurrentTest) {
    // interval flush when push
    std::atomic<bool> stop(false);
    std::atomic<uint32_t> testnum(0);
    ConcurrentApplyModule concurrentapply;
    ConcurrentApplyOption opt{10, 1, 5, 2};
    ASSERT_TRUE(concurrentapply.Init(opt));

    auto push = [&concurrentapply, &stop, &testnum]() {
        auto task = [&testnum]() {
            testnum.fetch_add(1);
        };
        while (!stop.load()) {
            for (int i = 0; i < 10; i++) {
                concurrentapply.Push(i, CHUNK_OP_TYPE::CHUNK_OP_RECOVER, task);
                concurrentapply.Push(i, CHUNK_OP_TYPE::CHUNK_OP_WRITE, task);
            }
        }
    };

    auto flush = [&concurrentapply, &stop, &testnum]() {
        while (!stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            concurrentapply.Flush();
        }
    };

    std::thread t(push);
    std::thread f(flush);

    while (testnum.load() <= 1000000) {
    }

    stop.store(true);

    std::cout << "thread exit, join" << std::endl;
    t.join();
    f.join();

    concurrentapply.Flush();
    ASSERT_GT(testnum, 1000000);
    concurrentapply.Stop();
}

