
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

TEST(FsInfoWrapperTest, CommonTest) {
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

    ASSERT_TRUE(MessageDifferencer::Equals(wrapper.ProtoFsInfo(), fsinfo));
}

TEST(FsInfoWrapperTest, MergeTest) {
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
    wrapper.AddMountPoint("1.2.3.4:/tmp");
    wrapper.AddMountPoint("1.2.3.4:/data");

    FsInfoWrapper wrapper2(wrapper);
    wrapper2.AddMountPoint("2.3.4.5:/tmp");

    wrapper.Merge(wrapper2);

    ASSERT_EQ(3, wrapper.MountPoints().size());

    std::vector<std::string> expected{"1.2.3.4:/data", "1.2.3.4:/tmp",
                                      "2.3.4.5:/tmp"};
    ASSERT_EQ(wrapper.MountPoints(), expected);
}

TEST(FsInfoWrapperTest, TestGenerateFsInfoWrapper) {
    const uint64_t fsId = 1;
    const std::string fsName = "curvefs";
    const uint64_t rootinodeid = 1;
    const uint64_t blocksize = 4096;

    {
        FsDetail detail;
        Volume volume;
        volume.set_volumesize(8192);
        volume.set_blocksize(blocksize);
        volume.set_volumename("/curvefs");
        volume.set_user("test");

        detail.set_allocated_volume(new Volume(volume));

        auto wrapper = GenerateFsInfoWrapper("curvefs", fsId, blocksize,
                                             rootinodeid, detail);

        auto proto = wrapper.ProtoFsInfo();
        ASSERT_TRUE(proto.IsInitialized());

        ASSERT_EQ(fsId, proto.fsid());
        ASSERT_EQ(fsName, proto.fsname());
        ASSERT_EQ(FsStatus::NEW, proto.status());
        ASSERT_EQ(rootinodeid, proto.rootinodeid());
        ASSERT_EQ(blocksize, proto.blocksize());
        ASSERT_EQ(0, proto.mountnum());
        ASSERT_EQ(FSType::TYPE_VOLUME, proto.fstype());

        ASSERT_TRUE(MessageDifferencer::Equals(detail, proto.detail()));
    }

    {
        FsDetail detail;
        S3Info s3Info;
        s3Info.set_ak("ak");
        s3Info.set_sk("sk");
        s3Info.set_endpoint("endpoint");
        s3Info.set_bucketname("bucketname");
        s3Info.set_blocksize(blocksize);
        s3Info.set_chunksize(4096);

        detail.set_allocated_s3info(new S3Info(s3Info));

        auto wrapper = GenerateFsInfoWrapper("curvefs", fsId, blocksize,
                                             rootinodeid, detail);

        auto proto = wrapper.ProtoFsInfo();
        ASSERT_TRUE(proto.IsInitialized());

        ASSERT_EQ(fsId, proto.fsid());
        ASSERT_EQ(fsName, proto.fsname());
        ASSERT_EQ(FsStatus::NEW, proto.status());
        ASSERT_EQ(rootinodeid, proto.rootinodeid());
        ASSERT_EQ(blocksize, proto.blocksize());
        ASSERT_EQ(0, proto.mountnum());
        ASSERT_EQ(FSType::TYPE_S3, proto.fstype());

        ASSERT_TRUE(MessageDifferencer::Equals(detail, proto.detail()));
    }
}

}  // namespace mds
}  // namespace curvefs
