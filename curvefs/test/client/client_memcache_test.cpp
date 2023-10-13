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
 * File Created: 2022-09-26
 * Author: fansehep (YangFan)
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "absl/strings/str_cat.h"
#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include "curvefs/src/client/kvclient/memcache_client.h"
#include "src/common/concurrent/count_down_event.h"

using curve::common::CountDownEvent;

namespace curvefs {
namespace client {

class MemCachedTest : public ::testing::Test {
 public:
    MemCachedTest() = default;
    void SetUp() {
        auto hostname = "127.0.0.1";
        auto port = 18080;
        memcached_pid = ::fork();
        if (0 > memcached_pid) {
            ASSERT_FALSE(true);
        } else if (0 == memcached_pid) {
            std::string memcached_port =
                "-p " + std::to_string(port);
            ASSERT_EQ(0, execlp("memcached", "memcached", "-uroot",
                                memcached_port.c_str(), nullptr));
        }

        std::shared_ptr<MemCachedClient> client(new MemCachedClient());
        MemcacheClusterInfo info;
        client->Init(info, "test");
        ASSERT_EQ(true, client->AddServer(hostname, port));
        ASSERT_EQ(true, client->PushServer());
        KVClientManagerOpt opt;
        opt.setThreadPooln = 2;
        opt.getThreadPooln = 2;
        ASSERT_EQ(true, manager_.Init(opt, client, "test"));

        // wait memcached server start
        std::string errorlog;
        int retry = 0;
        bool ret = false;
        do {
            if ((ret = client->Set("1", "2", 1, &errorlog)) || retry > 100) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            retry++;
        } while (1);
        ASSERT_TRUE(ret)
            << absl::StrCat("memcache start failed, errorlog: ", errorlog);
        LOG(INFO) << "=============== memcache start ok";
    }

    void TearDown() {
        auto str = absl::StrCat("kill -9 ", memcached_pid);
        ::system(str.c_str());
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }
    pid_t memcached_pid;
    KVClientManager manager_;
};


TEST_F(MemCachedTest, MultiThreadTask) {
    // prepare data
    std::vector<std::thread> workers;
    std::vector<std::pair<std::string, std::string>> kvstr = {
        {"123", "1231"},
        {"456", "4561"},
        {"789", "7891"},
        {"234", "2341"},
        {"890", "8901"}
    };

    // set
    CountDownEvent taskEvent(5);
    for (int i = 0; i < 5; i++) {
        workers.emplace_back([&, i]() {
            auto task = std::make_shared<SetKVCacheTask>(
                kvstr[i].first, kvstr[i].second.c_str(),
                kvstr[i].second.length());
            task->done =
                [&taskEvent](const std::shared_ptr<SetKVCacheTask> &task) {
                    taskEvent.Signal();
                };
            manager_.Set(task);
        });
    }
    taskEvent.Wait();
    ASSERT_EQ(5, manager_.GetMetricForTesting()->set.latency.count());

    // get
    for (int i = 0; i < 5; i++) {
        workers.emplace_back([&, i]() {
            CountDownEvent taskEvent(1);
            char *result = new char[4];
            auto task =
                std::make_shared<GetKVCacheTask>(kvstr[i].first, result, 0, 4);
            task->done =
                [&taskEvent](const std::shared_ptr<GetKVCacheTask> &task) {
                    taskEvent.Signal();
                };
            manager_.Get(task);
            taskEvent.Wait();
            ASSERT_EQ(0, memcmp(result, kvstr[i].second.c_str(), 4));
            ASSERT_TRUE(task->res);
        });
    }
    for (auto &iter : workers) {
        if (iter.joinable()) {
            iter.join();
        }
    }
}
}  // namespace client
}  // namespace curvefs
