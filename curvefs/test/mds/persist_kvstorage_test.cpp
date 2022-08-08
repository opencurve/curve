
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
 * Date: Sun Aug  1 15:53:45 CST 2021
 * Author: wuhanqing
 */

#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/src/mds/codec/codec.h"
#include "curvefs/src/mds/fs_storage.h"
#include "curvefs/test/mds/mock/mock_kvstorage_client.h"
#include "curvefs/proto/mds.pb.h"

namespace curvefs {
namespace mds {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Matcher;
using ::testing::Return;
using ::testing::SetArgPointee;

class PersistKVStorageTest : public ::testing::Test {
 protected:
    void SetUp() override {
        storageCli_ = std::make_shared<MockKVStorageClient>();
    }

    void TearDown() override {}

    std::vector<std::pair<std::string, std::string>> PrepareFsInfoSamples() {
        std::vector<std::pair<std::string, std::string>> encoded;

        FsInfo hello;
        hello.set_fsid(1);
        hello.set_fsname("hello");
        hello.set_status(FsStatus::INITED);
        hello.set_rootinodeid(1);
        hello.set_capacity(4096);
        hello.set_blocksize(4096);
        hello.set_mountnum(0);
        hello.set_fstype(FSType::TYPE_VOLUME);
        hello.set_enablesumindir(false);
        hello.set_owner("test");
        hello.set_txsequence(0);
        hello.set_txowner("owner");

        common::Volume volume;
        volume.set_blocksize(4096);
        volume.set_volumesize(4096);
        volume.set_volumename("/hello");
        volume.set_user("test");
        volume.set_blockgroupsize(128ull * 1024 * 1024);
        volume.set_bitmaplocation(common::BitmapLocation::AtStart);
        volume.set_slicesize(1ULL * 1024 * 1024 * 1024);
        volume.set_autoextend(false);

        FsDetail helloDetail;
        helloDetail.set_allocated_volume(new common::Volume(volume));
        hello.set_allocated_detail(new FsDetail(helloDetail));

        FsInfo world;
        world.set_fsid(2);
        world.set_fsname("world");
        world.set_status(FsStatus::INITED);
        world.set_rootinodeid(1);
        world.set_capacity(4096);
        world.set_blocksize(4096);
        world.set_mountnum(0);
        world.set_fstype(FSType::TYPE_S3);
        world.set_enablesumindir(false);
        world.set_owner("test");
        world.set_txsequence(0);
        world.set_txowner("owner");

        common::S3Info s3info;
        s3info.set_ak("ak");
        s3info.set_sk("sk");
        s3info.set_endpoint("endpoint");
        s3info.set_bucketname("bucketname");
        s3info.set_blocksize(4096);
        s3info.set_chunksize(4096);

        FsDetail worldDetail;
        worldDetail.set_allocated_s3info(new common::S3Info(s3info));
        world.set_allocated_detail(new FsDetail(worldDetail));

        std::string encodedFsName = codec::EncodeFsName(hello.fsname());
        std::string encodedFsInfo;

        EXPECT_TRUE(codec::EncodeProtobufMessage(hello, &encodedFsInfo));
        encoded.emplace_back(encodedFsName, encodedFsInfo);

        encodedFsName = codec::EncodeFsName(world.fsname());
        encodedFsInfo.clear();

        EXPECT_TRUE(codec::EncodeProtobufMessage(world, &encodedFsInfo));
        encoded.emplace_back(encodedFsName, encodedFsInfo);

        return encoded;
    }

 protected:
    std::shared_ptr<MockKVStorageClient> storageCli_;
};

TEST_F(PersistKVStorageTest, TestInit) {
    // list from storage failed
    {
        PersisKVStorage storage(storageCli_);

        std::vector<std::pair<std::string, std::string>> encoded{
            {"hello", "world"}};

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(Return(EtcdErrCode::EtcdInternal));

        EXPECT_FALSE(storage.Init());
    }

    // decode data failed
    {
        PersisKVStorage storage(storageCli_);

        std::vector<std::pair<std::string, std::string>> encoded{
            {"hello", "world"}};

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));

        EXPECT_FALSE(storage.Init());
    }

    {
        PersisKVStorage storage(storageCli_);

        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));

        EXPECT_TRUE(storage.Init());

        EXPECT_TRUE(storage.Exist(1));
        EXPECT_TRUE(storage.Exist("hello"));

        EXPECT_TRUE(storage.Exist(2));
        EXPECT_TRUE(storage.Exist("world"));

        EXPECT_FALSE(storage.Exist(3));
        EXPECT_FALSE(storage.Exist("foo"));
    }
}

TEST_F(PersistKVStorageTest, TestGetAndExist) {
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded;

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));

        EXPECT_TRUE(storage.Init());
        EXPECT_FALSE(storage.Exist(1));
        EXPECT_FALSE(storage.Exist("hello"));

        FsInfoWrapper wrapper;
        EXPECT_EQ(FSStatusCode::NOT_FOUND, storage.Get(1, &wrapper));
        EXPECT_EQ(FSStatusCode::NOT_FOUND, storage.Get("hello", &wrapper));
    }

    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));

        EXPECT_TRUE(storage.Init());
        EXPECT_TRUE(storage.Exist(1));
        EXPECT_TRUE(storage.Exist("hello"));
        EXPECT_TRUE(storage.Exist(2));
        EXPECT_TRUE(storage.Exist("world"));

        EXPECT_FALSE(storage.Exist(3));
        EXPECT_FALSE(storage.Exist("foo"));

        FsInfoWrapper wrapper;
        EXPECT_EQ(FSStatusCode::OK, storage.Get("hello", &wrapper));
        EXPECT_EQ(1, wrapper.GetFsId());
        EXPECT_EQ("hello", wrapper.GetFsName());

        FsInfoWrapper wrapperById;
        EXPECT_EQ(FSStatusCode::OK,
                  storage.Get(wrapper.GetFsId(), &wrapperById));
        EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
            wrapper.ProtoFsInfo(), wrapperById.ProtoFsInfo()));

        EXPECT_EQ(FSStatusCode::OK, storage.Get("world", &wrapper));
        EXPECT_EQ(2, wrapper.GetFsId());
        EXPECT_EQ("world", wrapper.GetFsName());

        EXPECT_EQ(FSStatusCode::OK,
                  storage.Get(wrapper.GetFsId(), &wrapperById));
        EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
            wrapper.ProtoFsInfo(), wrapperById.ProtoFsInfo()));
    }
}

TEST_F(PersistKVStorageTest, TestNextID) {
    // etcd get key error
    {
        PersisKVStorage storage(storageCli_);
        EXPECT_CALL(*storageCli_, Get(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdInternal));

        auto id = storage.NextFsId();
        EXPECT_EQ(INVALID_FS_ID, id);
    }

    // etcd cas failed
    {
        PersisKVStorage storage(storageCli_);
        EXPECT_CALL(*storageCli_, Get(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist));
        EXPECT_CALL(*storageCli_, CompareAndSwap(_, _, _))
            .WillOnce(Return(EtcdErrCode::EtcdInternal));

        auto id = storage.NextFsId();
        EXPECT_EQ(INVALID_FS_ID, id);
    }

    {
        PersisKVStorage storage(storageCli_);
        EXPECT_CALL(*storageCli_, Get(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist));
        EXPECT_CALL(*storageCli_, CompareAndSwap(_, _, _))
            .WillOnce(Return(EtcdErrCode::EtcdOK));

        int expectId = 1;
        for (int i = 0; i < 10; ++i) {
            EXPECT_EQ(expectId++, storage.NextFsId());
        }
    }

    {
        PersisKVStorage storage(storageCli_);
        EXPECT_CALL(*storageCli_, Get(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>("100"), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, CompareAndSwap(_, _, _))
            .WillOnce(Return(EtcdErrCode::EtcdOK));

        int expectId = 101;
        for (int i = 0; i < 10; ++i) {
            EXPECT_EQ(expectId++, storage.NextFsId());
        }
    }
}

TEST_F(PersistKVStorageTest, TestInsert) {
    // fs already exists
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, Put(_, _))
            .Times(0);

        EXPECT_TRUE(storage.Init());

        FsInfoWrapper wrapper;
        EXPECT_EQ(FSStatusCode::OK, storage.Get("hello", &wrapper));

        EXPECT_EQ(FSStatusCode::FS_EXIST, storage.Insert(wrapper));
        EXPECT_TRUE(storage.Exist(wrapper.GetFsName()));
    }

    // kvstorage error
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, Put(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdInternal));

        EXPECT_TRUE(storage.Init());

        FsInfoWrapper wrapper;
        EXPECT_EQ(FSStatusCode::OK, storage.Get("hello", &wrapper));

        FsInfo newfs = wrapper.ProtoFsInfo();
        newfs.set_fsid(100);
        newfs.set_fsname("bar");
        FsInfoWrapper newfsWrapper(newfs);

        EXPECT_EQ(FSStatusCode::STORAGE_ERROR, storage.Insert(newfsWrapper));
        EXPECT_FALSE(storage.Exist(newfsWrapper.GetFsName()));
    }

    // kvstorage persist ok
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, Put(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdOK));

        EXPECT_TRUE(storage.Init());

        FsInfoWrapper wrapper;
        EXPECT_EQ(FSStatusCode::OK, storage.Get("hello", &wrapper));

        FsInfo newfs = wrapper.ProtoFsInfo();
        newfs.set_fsid(100);
        newfs.set_fsname("bar");
        FsInfoWrapper newfsWrapper(newfs);

        EXPECT_EQ(FSStatusCode::OK, storage.Insert(newfsWrapper));
        EXPECT_TRUE(storage.Exist(100));
        EXPECT_TRUE(storage.Exist("bar"));

        EXPECT_EQ(FSStatusCode::OK, storage.Get("bar", &newfsWrapper));
        EXPECT_EQ(100, newfsWrapper.GetFsId());
        EXPECT_EQ("bar", newfsWrapper.GetFsName());
    }
}

TEST_F(PersistKVStorageTest, TestUpdate) {
    // fs not found
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, Put(_, _))
            .Times(0);

        EXPECT_TRUE(storage.Init());

        FsInfoWrapper wrapper;
        EXPECT_EQ(FSStatusCode::OK, storage.Get("hello", &wrapper));

        FsInfo newfs = wrapper.ProtoFsInfo();
        newfs.set_fsname("bar");
        FsInfoWrapper newfsWrapper(newfs);

        EXPECT_EQ(FSStatusCode::NOT_FOUND, storage.Update(newfsWrapper));
        EXPECT_FALSE(storage.Exist("bar"));
    }

    // fs id mismatch
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, Put(_, _))
            .Times(0);

        EXPECT_TRUE(storage.Init());

        FsInfoWrapper wrapper;
        EXPECT_EQ(FSStatusCode::OK, storage.Get("hello", &wrapper));

        FsInfo newfs = wrapper.ProtoFsInfo();
        newfs.set_fsid(100);
        FsInfoWrapper newfsWrapper(newfs);

        EXPECT_EQ(FSStatusCode::FS_ID_MISMATCH, storage.Update(newfsWrapper));
        EXPECT_FALSE(storage.Exist(100));
    }

    // storage failed
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, Put(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdInternal));

        EXPECT_TRUE(storage.Init());

        FsInfoWrapper wrapper;
        EXPECT_EQ(FSStatusCode::OK, storage.Get("hello", &wrapper));

        Mountpoint mountpoint;
        mountpoint.set_hostname("1.2.3.4");
        mountpoint.set_path("/tmp");
        mountpoint.set_port(9000);
        wrapper.AddMountPoint(mountpoint);
        EXPECT_EQ(FSStatusCode::STORAGE_ERROR, storage.Update(wrapper));

        FsInfoWrapper wrapper2;
        EXPECT_EQ(FSStatusCode::OK, storage.Get("hello", &wrapper2));
        EXPECT_FALSE(wrapper2.IsMountPointExist(mountpoint));
    }

    // storage ok
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, Put(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdOK));

        EXPECT_TRUE(storage.Init());

        FsInfoWrapper wrapper;
        EXPECT_EQ(FSStatusCode::OK, storage.Get("hello", &wrapper));

        Mountpoint mountpoint;
        mountpoint.set_hostname("1.2.3.4");
        mountpoint.set_path("/tmp");
        mountpoint.set_port(9000);
        wrapper.AddMountPoint(mountpoint);
        EXPECT_EQ(FSStatusCode::OK, storage.Update(wrapper));

        FsInfoWrapper wrapper2;
        EXPECT_EQ(FSStatusCode::OK, storage.Get("hello", &wrapper2));
        EXPECT_TRUE(wrapper2.IsMountPointExist(mountpoint));
    }
}

TEST_F(PersistKVStorageTest, TestDelete) {
    // fs not found
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, Delete(_))
            .Times(0);

        EXPECT_TRUE(storage.Init());

        EXPECT_EQ(FSStatusCode::NOT_FOUND, storage.Delete("bvar"));
    }

    // storage failed
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, Delete(_))
            .WillOnce(Return(EtcdErrCode::EtcdInternal));

        EXPECT_TRUE(storage.Init());

        EXPECT_EQ(FSStatusCode::STORAGE_ERROR, storage.Delete("hello"));
        EXPECT_TRUE(storage.Exist("hello"));
        EXPECT_TRUE(storage.Exist(1));
    }

    // storage ok
    {
        PersisKVStorage storage(storageCli_);
        std::vector<std::pair<std::string, std::string>> encoded =
            PrepareFsInfoSamples();

        EXPECT_CALL(*storageCli_, List(_, _, Matcher<decltype(encoded)*>(_)))
            .WillOnce(
                DoAll(SetArgPointee<2>(encoded), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*storageCli_, Delete(_))
            .WillOnce(Return(EtcdErrCode::EtcdOK));

        EXPECT_TRUE(storage.Init());

        EXPECT_EQ(FSStatusCode::OK, storage.Delete("hello"));
        EXPECT_FALSE(storage.Exist("hello"));
        EXPECT_FALSE(storage.Exist(1));

        EXPECT_TRUE(storage.Exist("world"));
        EXPECT_TRUE(storage.Exist(2));
    }
}

}  // namespace mds
}  // namespace curvefs
