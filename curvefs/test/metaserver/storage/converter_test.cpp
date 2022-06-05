/*
 * Copyright (c) 2021 NetEase Inc.
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

/**
 * Project: Curve
 * Created Date: 2022-06-08
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <unordered_map>

#include "curvefs/src/metaserver/storage/converter.h"

namespace curvefs {
namespace metaserver {
namespace storage {

class ConverterTest : public testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}

 protected:
    Converter conv_;
};

TEST_F(ConverterTest, Key4Dentry) {
    std::vector<std::string> paths {
        "/a",
        "/a/b/cdefg",
        "/a:/b:/c",
        "/a:",
        "/a::",
        "/a:::",
        "/a::/b",
        "/a:/b:",
        ":/a",
        "::/a",
        ":",
        "::",
        ":::::::::",
        ":/a:",
        "/::::::",
        "/::::/::/",
        "/012,::2,:2211",
    };

    for (const auto& path : paths) {
        LOG(INFO) << "TEST " << path;
        Key4Dentry key(1, 1, path);
        std::string skey = conv_.SerializeToString(key);
        ASSERT_EQ(skey, "3:1:1:" + path);

        Key4Dentry out;
        ASSERT_TRUE(conv_.ParseFromString(skey, &out));
        ASSERT_EQ(out.fsId, 1);
        ASSERT_EQ(out.parentInodeId, 1);
        ASSERT_EQ(out.name, path);
    }

    for (const auto& path : paths) {
        LOG(INFO) << "TEST " << path;
        Key4Dentry key(100, 1001, path);
        std::string skey = conv_.SerializeToString(key);
        ASSERT_EQ(skey, "3:100:1001:" + path);

        Key4Dentry out;
        ASSERT_TRUE(conv_.ParseFromString(skey, &out));
        ASSERT_EQ(out.fsId, 100);
        ASSERT_EQ(out.parentInodeId, 1001);
        ASSERT_EQ(out.name, path);
    }
}

TEST_F(ConverterTest, Prefix4SameParentDentry) {
    Prefix4SameParentDentry prefix(1, 100);
    std::string sprefix = conv_.SerializeToString(prefix);
    ASSERT_EQ(sprefix, "3:1:100:");

    Prefix4SameParentDentry out;
    ASSERT_TRUE(conv_.ParseFromString(sprefix, &out));
    ASSERT_EQ(out.fsId, 1);
    ASSERT_EQ(out.parentInodeId, 100);
}

TEST_F(ConverterTest, Key4InodeAuxInfo) {
    Key4InodeAuxInfo key(1, 1);
    std::string skey = conv_.SerializeToString(key);
    ASSERT_EQ(skey, "5:1:1");

    Key4InodeAuxInfo out;
    ASSERT_TRUE(conv_.ParseFromString(skey, &out));
    ASSERT_EQ(out.fsId, 1);
    ASSERT_EQ(out.inodeId, 1);
}

TEST_F(ConverterTest, NameGenerator) {
    NameGenerator ng(1);
    ASSERT_EQ(ng.GetFixedLength(), 6);
    ASSERT_EQ(ng.GetInodeTableName().size(), ng.GetFixedLength());
    ASSERT_EQ(ng.GetS3ChunkInfoTableName().size(), ng.GetFixedLength());
    ASSERT_EQ(ng.GetDentryTableName().size(), ng.GetFixedLength());
    ASSERT_EQ(ng.GetVolumnExtentTableName().size(), ng.GetFixedLength());
    ASSERT_EQ(ng.GetInodeAuxInfoTableName().size(), ng.GetFixedLength());
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
