
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
 * Created Date: 2023-03-29
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/filesystem/attr_watcher.h"
#include "curvefs/test/client/filesystem/helper/helper.h"

namespace curvefs {
namespace client {
namespace filesystem {

class AttrWatcherTest : public ::testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(AttrWatcherTest, RememberMtime) {
    auto option = AttrWatcherOption();
    auto attrWatcher = std::make_shared<AttrWatcher>(option, nullptr, nullptr);

    // remeber mtime
    InodeAttr attr = MkAttr(100, AttrOption().mtime(123, 456));
    attrWatcher->RemeberMtime(attr);

    // get mtime
    TimeSpec time;
    bool yes = attrWatcher->GetMtime(100, &time);
    ASSERT_TRUE(yes);
    ASSERT_EQ(time, TimeSpec(123, 456));
}

TEST_F(AttrWatcherTest, EvitAttr) {
    auto option = AttrWatcherOption{lruSize: 1};
    auto attrWatcher = std::make_shared<AttrWatcher>(option, nullptr, nullptr);

    // remeber mtime
    for (const Ino& ino : std::vector<Ino>{100, 200}) {
        InodeAttr attr = MkAttr(ino, AttrOption().mtime(123, 456));
        attrWatcher->RemeberMtime(attr);
    }

    // get mtime
    TimeSpec time;
    bool yes = attrWatcher->GetMtime(100, &time);
    ASSERT_FALSE(yes);

    yes = attrWatcher->GetMtime(200, &time);
    ASSERT_TRUE(yes);
    ASSERT_EQ(time, TimeSpec(123, 456));
}

TEST_F(AttrWatcherTest, UpdateDirEntryAttr) {
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
