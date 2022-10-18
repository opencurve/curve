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

using ::curvefs::client::GetKvCacheContext;
using ::curvefs::client::KvClientManager;
using ::curvefs::client::KvClientManagerConfig;
using ::curvefs::client::MemCachedClient;
using ::curvefs::client::SetKVCacheTask;

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
            std::string memcached_config =
                "memcached -p " + std::to_string(port);
            ASSERT_EQ(0, execl("/bin/sh", "sh", "-c", memcached_config.c_str(),
                               nullptr));
            exit(0);
        }
        // wait memcached server start
        std::this_thread::sleep_for(std::chrono::seconds(4));
        std::unique_ptr<MemCachedClient> client(new MemCachedClient());
        ASSERT_EQ(true, client->AddServer(hostname, port));
        ASSERT_EQ(true, client->PushServer());
        KvClientManagerConfig conf;
        conf.kvclient = std::move(client);
        conf.threadPooln = 2;
        ASSERT_EQ(true, manager_.Init(&conf));
    }

    void TearDown() {
        manager_.Uninit();
        auto str = absl::StrCat("kill -9 ", memcached_pid);
        ::system(str.c_str());
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }
    pid_t memcached_pid;
    KvClientManager manager_;
};

TEST_F(MemCachedTest, MultiThreadGetSetTest) {
    std::vector<std::thread> workers;
    int i;
    std::vector<std::pair<std::string, std::string>> kvstr = {
        {"123", "1231"},
        {"456", "4561"},
        {"789", "7891"},
        {"234", "2341"},
        {"890", "8901"}
    };
    i = 0;
    for (; i < 5; i++) {
        workers.emplace_back([&, i]() {
            manager_.Set(kvstr[i].first, kvstr[i].second.c_str(),
                         kvstr[i].second.length());
        });
    }
    for (auto& iter : workers) {
        iter.join();
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
    i = 0;
    for (; i < 5; i++) {
        workers.emplace_back([&, i]() {
            std::string res;
            ASSERT_EQ(true, manager_.Get(kvstr[i].first, &res));
            ASSERT_EQ(res, kvstr[i].second);
        });
    }
    for (auto& iter : workers) {
        if (iter.joinable()) {
            iter.join();
        }
    }
}

TEST_F(MemCachedTest, MultiThreadTask) {
    std::vector<std::thread> workers;
    int i;
    std::vector<std::pair<std::string, std::string>> kvstr = {
        {"123", "1231"},
        {"456", "4561"},
        {"789", "7891"},
        {"234", "2341"},
        {"890", "8901"}
    };
    i = 0;
    for (; i < 5; i++) {
        workers.emplace_back([&, i]() {
            auto task = std::make_shared<SetKVCacheTask>(
                kvstr[i].first, kvstr[i].second.c_str(),
                kvstr[i].second.length());
            manager_.Enqueue(task);
        });
    }
    for (auto& iter : workers) {
        iter.join();
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
    i = 0;
    for (; i < 5; i++) {
        workers.emplace_back([&, i]() {
            std::string result;
            auto context =
                std::make_shared<GetKvCacheContext>(kvstr[i].first, &result);
            ASSERT_EQ(true, manager_.Get(context));
            ASSERT_EQ(result, kvstr[i].second);
        });
    }
    for (auto& iter : workers) {
        if (iter.joinable()) {
            iter.join();
        }
    }
}
