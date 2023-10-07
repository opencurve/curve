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

#include "curvefs/src/client/vfs/cache.h"
#include "curvefs/test/client/vfs/helper/helper.h"

namespace curvefs {
namespace client {
namespace vfs {

class EntryCacheTest : public ::testing::Test {
 protected:
    struct TestGet {
        Ino parent;
        std::string name;
        bool yes;
        Ino ino;
    };

    struct TestPut {
        Ino parent;
        std::string name;
        Ino ino;
        uint64_t timeoutSec;
        bool yes;
    };

    struct TestDelete {
        Ino parent;
        std::string name;
        bool yes;
    };

    void TEST_GET(std::shared_ptr<EntryCache> entryCache,
                  std::vector<TestGet> tests) {
        Ino ino;
        for (const auto& t : tests) {
            bool yes = entryCache->Get(t.parent, t.name, &ino);
            ASSERT_EQ(yes, t.yes);
            if (t.yes) {
                ASSERT_EQ(ino, t.ino);
            }
        }
    }

    void TEST_PUT(std::shared_ptr<EntryCache> entryCache,
                  std::vector<TestPut> tests) {
        for (const auto& t : tests) {
            bool yes = entryCache->Put(t.parent, t.name, t.ino, t.timeoutSec);
            ASSERT_EQ(yes, t.yes);
        }
    }

    void TEST_DELETE(std::shared_ptr<EntryCache> entryCache,
                     std::vector<TestDelete> tests) {
        for (const auto& t : tests) {
            bool yes = entryCache->Delete(t.parent, t.name);
            ASSERT_EQ(yes, t.yes);
        }
    }
};

TEST_F(EntryCacheTest, Get) {
    auto entryCache = std::make_shared<EntryCache>(10);

    {
        std::vector<TestGet> tests {
            { 1, "f1", false },
            { 1, "f2", false },
            { 1, "f3", false }
        };
        TEST_GET(entryCache, tests);
    }

    {
        std::vector<TestPut> tests {
            { 1, "f1", 2, 3600, true },
            { 1, "f2", 3, 3600, true },
            { 1, "f3", 4, 3600, true }
        };
        TEST_PUT(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 3);
    }

    {
        std::vector<TestGet> tests {
            { 1, "f1", true, 2 },
            { 1, "f2", true, 3 },
            { 1, "f3", true, 4 }
        };
        TEST_GET(entryCache, tests);
    }
}

TEST_F(EntryCacheTest, Put) {
    auto entryCache = std::make_shared<EntryCache>(100);

    {
        std::vector<TestPut> tests {
            { 1, "f1", 2, 3600, true },
            { 1, "f2", 3, 3600, true },
            { 1, "f3", 4, 3600, true },
        };
        TEST_PUT(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 3);
    }

    {
        std::vector<TestGet> tests {
            { 1, "f1", true, 2 },
            { 1, "f2", true, 3 },
            { 1, "f3", true, 4 }
        };
        TEST_GET(entryCache, tests);
    }

    {
        std::vector<TestPut> tests {
            { 1, "f2", 30, 3600, true },
            { 1, "f3", 40, 3600, true },
        };
        TEST_PUT(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 3);
    }

    {
        std::vector<TestGet> tests {
            { 1, "f1", true, 2 },
            { 1, "f2", true, 30 },
            { 1, "f3", true, 40 }
        };
        TEST_GET(entryCache, tests);
    }
}

TEST_F(EntryCacheTest, Delete) {
    auto entryCache = std::make_shared<EntryCache>(100);

    {
        std::vector<TestPut> tests {
            { 1, "f1", 2, 3600, true },
            { 1, "f2", 3, 3600, true },
            { 1, "f3", 4, 3600, true },
        };
        TEST_PUT(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 3);
    }

    {
        std::vector<TestDelete> tests {
            { 1, "f1", true },
            { 1, "f2", true },
            { 1, "f4", true }
        };
        TEST_DELETE(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 1);
    }

    {
        std::vector<TestGet> tests {
            { 1, "f1", false },
            { 1, "f2", false },
            { 1, "f3", true, 4 }
        };
        TEST_GET(entryCache, tests);
    }
}

TEST_F(EntryCacheTest, Size) {
    auto entryCache = std::make_shared<EntryCache>(100);
    ASSERT_EQ(entryCache->Size(), 0);

    {
        std::vector<TestPut> tests {
            { 1, "f1", 2, 3600, true },
            { 1, "f2", 3, 3600, true },
            { 1, "f3", 4, 3600, true }
        };
        TEST_PUT(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 3);
    }

    {
        std::vector<TestDelete> tests {
            { 1, "f1", true },
            { 1, "f2", true }
        };
        TEST_DELETE(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 1);
    }

    {
        std::vector<TestPut> tests {
            { 1, "f4", 5, 3600, true },
            { 1, "f5", 6, 3600, true },
            { 1, "f6", 7, 3600, true }
        };
        TEST_PUT(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 4);
    }
}

TEST_F(EntryCacheTest, Disable) {
    auto entryCache = std::make_shared<EntryCache>(0);

    {
        std::vector<TestPut> tests {
            { 1, "f1", 2, 3600, false },
            { 1, "f2", 3, 3600, false },
            { 1, "f3", 4, 3600, false }
        };
        TEST_PUT(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 0);
    }

    {
        std::vector<TestGet> tests {
            { 1, "f1", false },
            { 1, "f2", false },
            { 1, "f3", false }
        };
        TEST_GET(entryCache, tests);
    }

    {
        std::vector<TestDelete> tests {
            { 1, "f1", false },
            { 1, "f2", false },
            { 1, "f3", false }
        };
        TEST_DELETE(entryCache, tests);
    }
}

TEST_F(EntryCacheTest, LruSize) {
    auto entryCache = std::make_shared<EntryCache>(2);

    {
        std::vector<TestPut> tests {
            { 1, "f1", 2, 3600, true },
            { 1, "f2", 3, 3600, true },
            { 1, "f3", 4, 3600, true },
        };
        TEST_PUT(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 2);
    }

    {
        std::vector<TestGet> tests {
            { 1, "f1", false },
            { 1, "f2", true, 3 },
            { 1, "f3", true, 4 },
        };
        TEST_GET(entryCache, tests);
    }
}

TEST_F(EntryCacheTest, Timeout) {
    auto entryCache = std::make_shared<EntryCache>(100);

    {
        std::vector<TestPut> tests {
            { 1, "f1", 2, 1, true },
            { 1, "f2", 3, 2, true },
            { 1, "f3", 4, 3, true },
        };
        TEST_PUT(entryCache, tests);
        ASSERT_EQ(entryCache->Size(), 3);
    }

    {
        std::vector<TestGet> tests {
            { 1, "f1", true, 2 },
            { 1, "f2", true, 3 },
            { 1, "f3", true, 4 },
        };
        TEST_GET(entryCache, tests);
    }

    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::vector<TestGet> tests {
            { 1, "f1", false },
            { 1, "f2", true, 3 },
            { 1, "f3", true, 4 },
        };
        TEST_GET(entryCache, tests);
    }

    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::vector<TestGet> tests {
            { 1, "f1", false },
            { 1, "f2", false },
            { 1, "f3", true, 4 },
        };
        TEST_GET(entryCache, tests);
    }
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
