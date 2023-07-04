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
 * Project: Curve
 * Created Date: 2023-09-18
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include <vector>
#include <string>

#include "curvefs/src/client/vfs/config.h"
#include "curvefs/test/client/vfs/helper/helper.h"

namespace curvefs {
namespace client {
namespace vfs {

class ConfigTest : public ::testing::Test {
 protected:
    struct TestGet {
        std::string key;
        std::string value;
    };

     void TEST_GET(std::shared_ptr<Configure> cfg,
                   std::vector<TestGet> tests) {
        std::string value;
        for (const auto& t : tests) {
            ASSERT_TRUE(cfg->Get(t.key, &value));
            ASSERT_EQ(value, t.value);
        }
     }
};

TEST_F(ConfigTest, Default) {
    auto cfg = Configure::Default();
    ASSERT_TRUE(cfg != nullptr);

    std::vector<TestGet> tests {
        { "vfs.userPermission.uid", "0" },
        { "vfs.userPermission.gids", "0" },
        { "vfs.userPermission.umask", "0022" },
        { "vfs.entryCache.lruSize", "2000000" },
        { "vfs.attrCache.lruSize", "2000000" },
    };
    TEST_GET(cfg, tests);
}

TEST_F(ConfigTest, LoadString) {
    std::string data = R"(
        vfs.userPermission.uid=100
        vfs.userPermission.gids=0,1000
        vfs.userPermission.umask=0022
    )";
    auto cfg = std::make_shared<Configure>();
    cfg->LoadString(data);

    std::vector<TestGet> tests {
        { "vfs.userPermission.uid", "100" },
        { "vfs.userPermission.gids", "0,1000" },
        { "vfs.userPermission.umask", "0022" },
    };
    TEST_GET(cfg, tests);
}

TEST_F(ConfigTest, Set) {
    auto cfg = std::make_shared<Configure>();
    cfg->Set("foo", "bar");
    cfg->Set("hello", "world");
    std::vector<TestGet> tests {
        { "foo", "bar" },
        { "hello", "world" },
    };
    TEST_GET(cfg, tests);

    // Set same keys with different value.
    cfg->Set("foo", "bar2");
    cfg->Set("hello", "world2");
    tests = std::vector<TestGet> {
        { "foo", "bar2" },
        { "hello", "world2" },
    };
    TEST_GET(cfg, tests);
}

TEST_F(ConfigTest, Iterate) {
    auto cfg = std::make_shared<Configure>();
    cfg->Set("foo", "bar");
    cfg->Set("hello", "world");

    std::map<std::string, std::string> m;
    cfg->Iterate([&](const std::string& key, const std::string& value){
        m[key] = value;
    });
    ASSERT_EQ(m.size(), 2);
    ASSERT_EQ(m["foo"], "bar");
    ASSERT_EQ(m["hello"], "world");
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
