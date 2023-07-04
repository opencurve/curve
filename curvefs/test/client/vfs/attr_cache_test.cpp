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
 * Created Date: 2023-09-20
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include <vector>
#include <string>

#include "curvefs/test/client/filesystem/helper/helper.h"
#include "curvefs/src/client/vfs/cache.h"
#include "curvefs/test/client/vfs/helper/helper.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curvefs::client::filesystem::AttrOption;

/*
inline bool operator==(const InodeAttr& lhs, const InodeAttr& rhs) {
    return lhs.inodeid() == rhs.inodeid() &&
            lhs.length() == rhs.length();
}
*/

class AttrCacheTest : public ::testing::Test {
 protected:

    InodeAttr MkAttr(Ino ino, uint64_t length) {
        using ::curvefs::client::filesystem::MkAttr;
        return MkAttr(ino, AttrOption().length(length));
    }

    struct TestGet {
        Ino ino;
        bool yes;
        InodeAttr attr;
    };

    struct TestPut {
        Ino ino;
        InodeAttr attr;
        uint64_t timeoutSec;
        bool yes;
    };

    struct TestDelete {
        Ino ino;
        bool yes;
    };

    void TEST_GET(std::shared_ptr<AttrCache> attrCache,
                  std::vector<TestGet> tests) {
        InodeAttr attr;
        for (const auto& t : tests) {
            bool yes = attrCache->Get(t.ino, &attr);
            ASSERT_EQ(yes, t.yes);
            if (t.yes) {
                ASSERT_EQ(attr.inodeid(), t.attr.inodeid());
                ASSERT_EQ(attr.length(), t.attr.length());
            }
        }
    }

    void TEST_PUT(std::shared_ptr<AttrCache> attrCache,
                  std::vector<TestPut> tests) {
        for (const auto& t : tests) {
            bool yes = attrCache->Put(t.ino, t.attr, t.timeoutSec);
            ASSERT_EQ(yes, t.yes);
        }
    }

    void TEST_DELETE(std::shared_ptr<AttrCache> attrCache,
                     std::vector<TestDelete> tests) {
        for (const auto& t : tests) {
            bool yes = attrCache->Delete(t.ino);
            ASSERT_EQ(yes, t.yes);
        }
    }
};

TEST_F(AttrCacheTest, Get) {
    auto attrCache = std::make_shared<AttrCache>(10);

    {
        std::vector<TestGet> tests {
            { 1, false },
            { 2, false },
            { 3, false }
        };
        TEST_GET(attrCache, tests);
    }

    {
        std::vector<TestPut> tests {
            { 1, MkAttr(1, 4096), 3600, true },
            { 2, MkAttr(2, 4096), 3600, true },
            { 3, MkAttr(3, 4096), 3600, true }
        };
        TEST_PUT(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 3);
    }

    {
        std::vector<TestGet> tests {
            { 1, true, MkAttr(1, 4096) },
            { 2, true, MkAttr(2, 4096) },
            { 3, true, MkAttr(3, 4096) }
        };
        TEST_GET(attrCache, tests);
    }
}

TEST_F(AttrCacheTest, Put) {
    auto attrCache = std::make_shared<AttrCache>(100);

    {
        std::vector<TestPut> tests {
            { 1, MkAttr(1, 4096), 3600, true },
            { 2, MkAttr(2, 4096), 3600, true },
            { 3, MkAttr(3, 4096), 3600, true }
        };
        TEST_PUT(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 3);
    }

    {
        std::vector<TestGet> tests {
            { 1, true, MkAttr(1, 4096) },
            { 2, true, MkAttr(2, 4096) },
            { 3, true, MkAttr(3, 4096) }
        };
        TEST_GET(attrCache, tests);
    }

    {
        std::vector<TestPut> tests {
            { 1, MkAttr(1, 8192), 3600, true },
            { 2, MkAttr(2, 8192), 3600, true },
        };
        TEST_PUT(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 3);
    }

    {
        std::vector<TestGet> tests {
            { 1, true, MkAttr(1, 8192) },
            { 2, true, MkAttr(2, 8192) },
            { 3, true, MkAttr(3, 4096) }
        };
        TEST_GET(attrCache, tests);
    }
}

TEST_F(AttrCacheTest, Delete) {
    auto attrCache = std::make_shared<AttrCache>(100);

    {
        std::vector<TestPut> tests {
            { 1, MkAttr(1, 4096), 3600, true },
            { 2, MkAttr(2, 4096), 3600, true },
            { 3, MkAttr(3, 4096), 3600, true }
        };
        TEST_PUT(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 3);
    }

    {
        std::vector<TestDelete> tests {
            { 1, true },
            { 2, true },
            { 4, true }
        };
        TEST_DELETE(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 1);
    }

    {
        std::vector<TestGet> tests {
            { 1, false },
            { 2, false },
            { 3, true, MkAttr(3, 4096) }
        };
        TEST_GET(attrCache, tests);
    }
}

TEST_F(AttrCacheTest, Size) {
    auto attrCache = std::make_shared<AttrCache>(100);
    ASSERT_EQ(attrCache->Size(), 0);

    {
        std::vector<TestPut> tests {
            { 1, MkAttr(1, 4096), 3600, true },
            { 2, MkAttr(2, 4096), 3600, true },
            { 3, MkAttr(3, 4096), 3600, true }
        };
        TEST_PUT(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 3);
    }

    {
        std::vector<TestDelete> tests {
            { 1, true },
            { 2, true }
        };
        TEST_DELETE(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 1);
    }

    {
        std::vector<TestPut> tests {
            { 1, MkAttr(1, 4096), 3600, true },
            { 2, MkAttr(2, 4096), 3600, true },
            { 4, MkAttr(4, 4096), 3600, true }
        };
        TEST_PUT(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 4);
    }
}

TEST_F(AttrCacheTest, Disable) {
    auto attrCache = std::make_shared<AttrCache>(0);

    {
        std::vector<TestPut> tests {
            { 1, MkAttr(1, 4096), 3600, false },
            { 2, MkAttr(2, 4096), 3600, false },
            { 3, MkAttr(3, 4096), 3600, false }
        };
        TEST_PUT(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 0);
    }

    {
        std::vector<TestGet> tests {
            { 1, false },
            { 2, false },
            { 3, false }
        };
        TEST_GET(attrCache, tests);
    }

    {
        std::vector<TestDelete> tests {
            { 1, false },
            { 2, false },
            { 3, false }
        };
        TEST_DELETE(attrCache, tests);
    }
}

TEST_F(AttrCacheTest, LruSize) {
    auto attrCache = std::make_shared<AttrCache>(2);

    {
        std::vector<TestPut> tests {
            { 1, MkAttr(1, 4096), 3600, true },
            { 2, MkAttr(2, 4096), 3600, true },
            { 3, MkAttr(3, 4096), 3600, true }
        };
        TEST_PUT(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 2);
    }

    {
        std::vector<TestGet> tests {
            { 1, false },
            { 2, true, MkAttr(2, 4096) },
            { 3, true, MkAttr(3, 4096) }
        };
        TEST_GET(attrCache, tests);
    }
}

TEST_F(AttrCacheTest, Timeout) {
    auto attrCache = std::make_shared<AttrCache>(100);

    {
        std::vector<TestPut> tests {
            { 1, MkAttr(1, 1024), 1, true },
            { 2, MkAttr(2, 2048), 2, true },
            { 3, MkAttr(3, 4096), 3, true }
        };
        TEST_PUT(attrCache, tests);
        ASSERT_EQ(attrCache->Size(), 3);
    }

    {
        std::vector<TestGet> tests {
            { 1, true, MkAttr(1, 1024) },
            { 2, true, MkAttr(2, 2048) },
            { 3, true, MkAttr(3, 4096) }
        };
        TEST_GET(attrCache, tests);
    }

    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::vector<TestGet> tests {
            { 1, false },
            { 2, true, MkAttr(2, 2048) },
            { 3, true, MkAttr(3, 4096) }
        };
        TEST_GET(attrCache, tests);
    }

    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::vector<TestGet> tests {
            { 1, false },
            { 2, false },
            { 3, true, MkAttr(3, 4096) }
        };
        TEST_GET(attrCache, tests);
    }
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
