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

#include <gtest/gtest.h>

#include "curvefs/src/client/filesystem/utils.h"
#include "curvefs/test/client/filesystem/helper/helper.h"

namespace curvefs {
namespace client {
namespace filesystem {

class DirEntryListTest : public ::testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}
};

class DirCacheTest : public ::testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(DirEntryListTest, Size) {
    DirEntryList entries;
    ASSERT_EQ(entries.Size(), 0);

    entries.Add(MkDirEntry(100, "f1"));
    ASSERT_EQ(entries.Size(), 1);

    entries.Clear();
    ASSERT_EQ(entries.Size(), 0);
}

TEST_F(DirEntryListTest, Iterate) {
    DirEntryList entries;

    std::vector<std::pair<Ino, std::string>> items{
        { 100, "f1" },
        { 200, "f2" },
        { 300, "f3" },
    };

    for (const auto& item : items) {
        entries.Add(MkDirEntry(item.first, item.second));
    }
    ASSERT_EQ(entries.Size(), 3);

    std::vector<std::pair<Ino, std::string>> out;
    entries.Iterate([&](DirEntry* dirEntry){
        out.push_back({dirEntry->ino, dirEntry->name});
    });
    ASSERT_EQ(items, out);
}

TEST_F(DirEntryListTest, Get) {
    DirEntryList entries;
    InodeAttr attr = MkAttr(100, AttrOption().length(1024));
    entries.Add(MkDirEntry(100, "f1", attr));

    // CASE 1: directory entry exit
    {
        DirEntry dirEntry;
        bool yes = entries.Get(100, &dirEntry);
        ASSERT_TRUE(yes);
        ASSERT_EQ(dirEntry.attr.inodeid(), 100);
        ASSERT_EQ(dirEntry.attr.length(), 1024);
    }

    // CASE 2: directory entry not exit
    {
        DirEntry dirEntry;
        bool yes = entries.Get(200, &dirEntry);
        ASSERT_FALSE(yes);
    }
}

TEST_F(DirEntryListTest, UpdateAttr) {
    DirEntryList entries;

    InodeAttr attr = MkAttr(100, AttrOption().length(1024));
    DirEntry dirEntry(100, "f1", attr);
    entries.Add(dirEntry);

    // CASE 1: update attribute success
    {
        InodeAttr attr = MkAttr(100, AttrOption().length(2048));
        bool yes = entries.UpdateAttr(100, attr);
        ASSERT_TRUE(yes);

        yes = entries.Get(100, &dirEntry);
        ASSERT_TRUE(yes);
        ASSERT_EQ(dirEntry.attr.inodeid(), 100);
        ASSERT_EQ(dirEntry.attr.length(), 2048);
    }

    // CASE 2: (update attribute failed) / (ino not found)
    {
        bool yes = entries.UpdateAttr(200, attr);
        ASSERT_FALSE(yes);
    }
}

TEST_F(DirEntryListTest, UpdateLength) {
    DirEntryList entries;

    DirEntry dirEntry = MkDirEntry(100, "f1", MkAttr(100, AttrOption()
        .length(1024)
        .mtime(100, 101)
        .ctime(200, 201)));
    entries.Add(dirEntry);

    // CASE 1: ino not found
    {
        InodeAttr open = MkAttr(200);
        bool yes = entries.UpdateLength(200, open);
        ASSERT_FALSE(yes);
    }

    // CASE 2: update length and mtime
    {
        InodeAttr open = MkAttr(100, AttrOption()
            .length(2048)
            .mtime(100, 101)
            .ctime(200, 202));
        bool yes = entries.UpdateLength(100, open);
        ASSERT_TRUE(yes);

        DirEntry dirEntry;
        yes = entries.Get(100, &dirEntry);
        ASSERT_TRUE(yes);
        ASSERT_EQ(dirEntry.attr.inodeid(), 100);
        ASSERT_EQ(dirEntry.attr.length(), 2048);
        // ASSERT_EQ(AttrMtime(dirEntry.attr), TimeSpec());
        // ASSERT_EQ(AttrCtime(dirEntry.attr), TimeSpec());
    }

    // CASE 3: update length, mtime, ctime
    {
        InodeAttr open = MkAttr(100, AttrOption()
            .length(2048)
            .mtime(100, 101)
            .ctime(200, 202));
        bool yes = entries.UpdateLength(100, open);
        ASSERT_TRUE(yes);

        DirEntry dirEntry;
        yes = entries.Get(100, &dirEntry);
        ASSERT_TRUE(yes);
        ASSERT_EQ(dirEntry.attr.length(), 2048);
        // ASSERT_EQ(AttrMtime(dirEntry.attr), TimeSpec());
        // ASSERT_EQ(AttrCtime(dirEntry.attr), TimeSpec());
    }
}

TEST_F(DirEntryListTest, Clear) {
    DirEntryList entries;
    std::vector<std::pair<Ino, std::string>> items{
        { 100, "f1" },
        { 200, "f2" },
        { 300, "f3" },
    };

    for (const auto& item : items) {
        entries.Add(MkDirEntry(item.first, item.second));
    }
    ASSERT_EQ(entries.Size(), 3);

    DirEntry dirEntry;
    for (const auto& item : items) {
        bool yes = entries.Get(item.first, &dirEntry);
        ASSERT_TRUE(yes);
        ASSERT_EQ(dirEntry.name, item.second);
    }

    entries.Clear();
    ASSERT_EQ(entries.Size(), 0);
    for (const auto& item : items) {
        bool yes = entries.Get(item.first, &dirEntry);
        ASSERT_FALSE(yes);
    }
}

TEST_F(DirEntryListTest, Mtime) {
    DirEntryList entries;
    TimeSpec time = entries.GetMtime();
    ASSERT_EQ(time, TimeSpec(0, 0));

    entries.SetMtime(TimeSpec(123, 456));
    time = entries.GetMtime();
    ASSERT_EQ(time, TimeSpec(123, 456));
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
