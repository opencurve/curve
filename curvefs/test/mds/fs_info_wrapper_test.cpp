/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Fri Jul 30 18:02:37 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/mds/fs_info_wrapper.h"

#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/proto/mds.pb.h"

namespace curvefs {
namespace mds {

using ::curvefs::common::S3Info;
using ::curvefs::common::Volume;
using ::google::protobuf::util::MessageDifferencer;

TEST(FsInfoWrapperTest, volumeTest) {
    FsInfo fsinfo;

    fsinfo.set_fsid(1);
    fsinfo.set_fsname("hello");
    fsinfo.set_status(mds::FsStatus::INITED);
    fsinfo.set_rootinodeid(1);
    fsinfo.set_capacity(8192);
    fsinfo.set_blocksize(4096);
    fsinfo.set_mountnum(0);
    fsinfo.set_fstype(mds::FSType::TYPE_VOLUME);

    FsInfoWrapper wrapper(fsinfo);

    EXPECT_TRUE(MessageDifferencer::Equals(wrapper.ProtoFsInfo(), fsinfo));
}

TEST(FsInfoWrapperTest, s3Test) {
    FsInfo fsinfo;

    fsinfo.set_fsid(2);
    fsinfo.set_fsname("hello");
    fsinfo.set_status(mds::FsStatus::INITED);
    fsinfo.set_rootinodeid(1);
    fsinfo.set_capacity(8192);
    fsinfo.set_blocksize(4096);
    fsinfo.set_mountnum(0);
    fsinfo.set_fstype(mds::FSType::TYPE_S3);

    FsInfoWrapper wrapper(fsinfo);

    EXPECT_TRUE(MessageDifferencer::Equals(wrapper.ProtoFsInfo(), fsinfo));
}

TEST(FsInfoWrapperTest, hybridTest) {
    FsInfo fsinfo;

    fsinfo.set_fsid(3);
    fsinfo.set_fsname("hello");
    fsinfo.set_status(mds::FsStatus::INITED);
    fsinfo.set_rootinodeid(1);
    fsinfo.set_capacity(8192);
    fsinfo.set_blocksize(4096);
    fsinfo.set_mountnum(0);
    fsinfo.set_fstype(mds::FSType::TYPE_HYBRID);

    FsInfoWrapper wrapper(fsinfo);

    EXPECT_TRUE(MessageDifferencer::Equals(wrapper.ProtoFsInfo(), fsinfo));
}

TEST(FsInfoWrapperTest, mpconflictTest_disablecto) {
    FsInfo fsinfo;

    fsinfo.set_fsid(3);
    fsinfo.set_fsname("hello");
    fsinfo.set_status(mds::FsStatus::INITED);
    fsinfo.set_rootinodeid(1);
    fsinfo.set_capacity(8192);
    fsinfo.set_blocksize(4096);
    fsinfo.set_mountnum(0);
    fsinfo.set_fstype(mds::FSType::TYPE_S3);
    Mountpoint mp;
    mp.set_hostname("0.0.0.0");
    mp.set_port(9000);
    mp.set_path("/data");
    *fsinfo.add_mountpoints() = mp;
    FsInfoWrapper wrapper(fsinfo);

    // mount point exsit
    Mountpoint testmp = mp;
    ASSERT_TRUE(wrapper.IsMountPointConflict(mp));

    // mount point has cto=false, no conflict
    testmp.set_hostname("127.0.0.1");
    testmp.set_cto(false);
    ASSERT_FALSE(wrapper.IsMountPointConflict(testmp));

    // mount point has cto=true, conflict
    testmp.set_hostname("127.0.0.1");
    testmp.set_cto(true);
    ASSERT_TRUE(wrapper.IsMountPointConflict(testmp));
}

TEST(FsInfoWrapperTest, mpconflictTest_enablecto) {
    FsInfo fsinfo;

    fsinfo.set_fsid(3);
    fsinfo.set_fsname("hello");
    fsinfo.set_status(mds::FsStatus::INITED);
    fsinfo.set_rootinodeid(1);
    fsinfo.set_capacity(8192);
    fsinfo.set_blocksize(4096);
    fsinfo.set_mountnum(0);
    fsinfo.set_fstype(mds::FSType::TYPE_S3);
    Mountpoint mp;
    mp.set_hostname("0.0.0.0");
    mp.set_port(9000);
    mp.set_path("/data");
    mp.set_cto(true);
    *fsinfo.add_mountpoints() = mp;
    FsInfoWrapper wrapper(fsinfo);

    // mount point has cto=false, conflict
    Mountpoint testmp = mp;
    testmp.set_hostname("127.0.0.1");
    testmp.set_cto(false);
    ASSERT_TRUE(wrapper.IsMountPointConflict(testmp));

    // mount point has cto=true, no conflict
    testmp.set_hostname("127.0.0.1");
    testmp.set_cto(true);
    ASSERT_FALSE(wrapper.IsMountPointConflict(testmp));
}

}  // namespace mds
}  // namespace curvefs
