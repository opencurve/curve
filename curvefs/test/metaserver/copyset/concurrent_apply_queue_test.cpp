/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * File Created: 20230521
 * Author: Xinlong-Chen
 */

#include <gtest/gtest.h>

#include <vector>
#include <atomic>
#include <functional>

#include "src/common/timeutility.h"
#include "curvefs/src/metaserver/copyset/concurrent_apply_queue.h"

using curvefs::metaserver::copyset::ApplyQueue;
using curvefs::metaserver::copyset::ApplyOption;
using curvefs::metaserver::copyset::OperatorType;

TEST(ApplyQueue, InitTest) {
    ApplyQueue concurrentapply;

    {
        // 1. init with invalid write-concurrentsize
        ApplyOption opt(-1, 1, 1, 1);
        ASSERT_FALSE(concurrentapply.Init(opt));
    }

    {
        // 2. init with invalid write-concurrentdepth
        ApplyOption opt(1, -1, 1, 1);
        ASSERT_FALSE(concurrentapply.Init(opt));
    }

    {
        // 3. init with invalid read-concurrentsize
        ApplyOption opt(1, 1, -1, 1);
        ASSERT_FALSE(concurrentapply.Init(opt));
    }

    {
        // 4. init with invalid read-concurrentdepth
        ApplyOption opt(1, 1, 1, -1);
        ASSERT_FALSE(concurrentapply.Init(opt));
    }

    {
        // 5. double init
        ApplyOption opt(1, 1, 1, 1);
        // init with vaild params
        ASSERT_TRUE(concurrentapply.Init(opt));
        // re-init
        ASSERT_TRUE(concurrentapply.Init(opt));
    }

    concurrentapply.Stop();
    // re-stop
    concurrentapply.Stop();
}

static void InitReadWriteTypeList(std::vector<OperatorType> *readTypeList,
                                  std::vector<OperatorType> *writeTypeList) {
    *readTypeList = {
        OperatorType::GetDentry,
        OperatorType::ListDentry,
        OperatorType::GetInode,
        OperatorType::BatchGetInodeAttr,
        OperatorType::BatchGetXAttr,
        OperatorType::GetVolumeExtent
    };

    auto IsRead = [&readTypeList](OperatorType type) -> bool {
        for (auto &readType : *readTypeList) {
            if (type == readType) {
                return true;
            }
        }
        return false;
    };

    for (uint32_t i = 0; i < static_cast<uint32_t>(OperatorType::OperatorTypeMax); ++i) {  // NOLINT
        if (!IsRead(static_cast<OperatorType>(i))) {
            writeTypeList->push_back(static_cast<OperatorType>(i));
        }
    }
}

OperatorType get_random_type(const std::vector<OperatorType> &type_list) {
    return type_list[rand() % type_list.size()];
}

TEST(ApplyQueue, RunTest) {
    std::vector<OperatorType> readTypeList;
    std::vector<OperatorType> writeTypeList;
    InitReadWriteTypeList(&readTypeList, &writeTypeList);

    auto read_type  = get_random_type(readTypeList);
    auto write_type = get_random_type(writeTypeList);

    ApplyQueue concurrentapply;

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
    ApplyOption opt(1, 1, 1, 1);

    ASSERT_TRUE(concurrentapply.Init(opt));

    ASSERT_TRUE(concurrentapply.Push(1, read_type, rtask));
    ASSERT_TRUE(concurrentapply.Push(1, write_type, wtask));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(1, testr);
    ASSERT_EQ(0, testw);
    concurrentapply.Flush();
    ASSERT_EQ(1, testw);

    concurrentapply.Stop();
}

TEST(ApplyQueue, FlushTest) {
    std::vector<OperatorType> readTypeList;
    std::vector<OperatorType> writeTypeList;
    InitReadWriteTypeList(&readTypeList, &writeTypeList);

    ApplyQueue concurrentapply;
    ApplyOption opt(2, 5000, 1, 1);
    ASSERT_TRUE(concurrentapply.Init(opt));

    std::atomic<uint32_t> testnum(0);
    auto task = [&testnum]() {
        testnum.fetch_add(1);
    };

    for (int i = 0; i < 5000; i++) {
        auto write_type = get_random_type(writeTypeList);
        concurrentapply.Push(i, write_type, task);
    }

    ASSERT_LE(testnum, 5000);
    concurrentapply.Flush();
    ASSERT_EQ(5000, testnum);

    concurrentapply.Stop();
}

TEST(ApplyQueue, ConcurrentTest) {
    std::vector<OperatorType> readTypeList;
    std::vector<OperatorType> writeTypeList;
    InitReadWriteTypeList(&readTypeList, &writeTypeList);

    // interval flush when push
    std::atomic<bool> stop(false);
    std::atomic<uint32_t> testnum(0);
    ApplyQueue concurrentapply;
    ApplyOption opt(10, 1, 5, 2);
    ASSERT_TRUE(concurrentapply.Init(opt));

    auto push = [&concurrentapply, &stop, &testnum,
                 &readTypeList, &writeTypeList]() {
        auto task = [&testnum]() {
            testnum.fetch_add(1);
        };
        while (!stop.load()) {
            for (int i = 0; i < 10; i++) {
                auto read_type  = get_random_type(readTypeList);
                auto write_type = get_random_type(writeTypeList);
                concurrentapply.Push(i, read_type, task);
                concurrentapply.Push(i, write_type, task);
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

