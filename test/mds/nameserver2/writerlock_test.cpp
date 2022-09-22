/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-09-16
 * Author: YangFan (fansehep)
 */

#include "src/mds/nameserver2/writerlock.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "src/common/uuid.h"
#include "test/mds/nameserver2/fakes.h"

using curve::common::UUIDGenerator;

namespace curve {
namespace mds {

class WriterLockTest : public ::testing::Test {
 public:
    WriterLockTest() {}
};

WriterLockTimeoutOption timeopt(5000000);
auto stg = std::make_shared<FakeNameServerStorage>();
TEST(WriterLockTest, prelock_test) {
    WriterLock mtx(stg, timeopt);
    const uint64_t fileid = 1;
    std::string uuid1 = UUIDGenerator().GenerateUUID();
    auto re = mtx.Lock(fileid, uuid1);
    ASSERT_EQ(true, re);
    std::vector<std::thread> workers;
    for (auto i = 0; i < 5; i++) {
        workers.emplace_back([&, i]() {
            std::string uuid = UUIDGenerator().GenerateUUID();
            auto res = mtx.Lock(fileid, uuid);
            ASSERT_EQ(false, res);
        });
    }
    for (auto& iter : workers) {
        if (iter.joinable()) {
            iter.join();
        }
    }
}

TEST(WriterLockTest, concurren_test) {
    WriterLock mtx(stg, timeopt);
    std::vector<std::thread> workers;
    const uint64_t fileid = 3;
    bool nums[5] = {false};
    for (auto i = 0; i < 5; i++) {
        workers.emplace_back([&, i]() {
            std::string uuid = UUIDGenerator().GenerateUUID();
            uuid += std::to_string(i);
            nums[i] = mtx.Lock(fileid, uuid);
        });
    }
    for (auto& iter : workers) {
        if (iter.joinable()) {
            iter.join();
        }
    }
    int cnt = 0;
    for (int i = 0; i < 5; i++) {
        if (nums[i]) {
            cnt++;
        }
    }
    ASSERT_EQ(cnt, 1);
}

TEST(WriterLockTest, unlock_test) {
    WriterLock mtx(stg, timeopt);
    const uint64_t fileid = 5;
    const std::string clientuuid = UUIDGenerator().GenerateUUID();
    auto re = mtx.Lock(fileid, clientuuid);
    ASSERT_EQ(true, re);
    re = mtx.Unlock(fileid, clientuuid);
    ASSERT_EQ(true, re);
    std::vector<std::thread> workers;
    bool nums[5] = {false};
    for (auto i = 0; i < 5; i++) {
        workers.emplace_back([&, i]() {
            std::string uuid = UUIDGenerator().GenerateUUID();
            nums[i] = mtx.Lock(fileid, uuid);
        });
    }
    for (auto& iter : workers) {
        if (iter.joinable()) {
            iter.join();
        }
    }
    int cnt = 0;
    for (int i = 0; i < 5; i++) {
        if (nums[i]) {
            cnt++;
        }
    }
    ASSERT_EQ(cnt, 1);
}

TEST(WriterLockTest, updatelock_test) {
    WriterLock mtx(stg, timeopt);
    const uint64_t fileid = 7;
    const std::string clientuuid = UUIDGenerator().GenerateUUID();
    ASSERT_EQ(mtx.Lock(fileid, clientuuid), true);
    std::atomic<bool> running(true);
    std::atomic<int> count(0);
    std::thread updatelockthread = std::thread([&]() {
        while (running.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            ASSERT_EQ(mtx.UpdateLockTime(fileid, clientuuid), true);
        }
    });
    std::vector<std::thread> workers;
    for (auto i = 0; i < 5; i++) {
        workers.emplace_back([&, i]() {
            while (running.load()) {
                std::string uuid = UUIDGenerator().GenerateUUID();
                uuid += std::to_string(i);
                ASSERT_EQ(mtx.Lock(fileid, uuid), false);
                std::this_thread::sleep_for(std::chrono::seconds(1));
                ++count;
                if (count >= 10) {
                    running = false;
                }
            }
        });
    }
    for (auto& thrd : workers) {
        if (thrd.joinable()) {
            thrd.join();
        }
    }
    if (updatelockthread.joinable()) {
        updatelockthread.join();
    }
}

}  // namespace mds
}  // namespace curve
