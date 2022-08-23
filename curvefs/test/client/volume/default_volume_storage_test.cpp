/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Thursday Mar 24 16:57:03 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/client/volume/default_volume_storage.h"

#include <string>
#include <vector>

#include "curvefs/src/client/error_code.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/test/volume/mock/mock_block_device_client.h"
#include "curvefs/test/volume/mock/mock_space_manager.h"
#include "gtest/gtest.h"

namespace curvefs {
namespace client {

using ::curvefs::client::rpcclient::MockMetaServerClient;
using ::curvefs::volume::AllocateHint;
using ::curvefs::volume::Extent;
using ::curvefs::volume::MockBlockDeviceClient;
using ::curvefs::volume::MockSpaceManager;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

class DefaultVolumeStorageTest : public ::testing::Test {
 protected:
    DefaultVolumeStorageTest()
        : storage_(&spaceMgr_, &blockDev_, &inodeCacheMgr_),
          metaServerCli_(std::make_shared<MockMetaServerClient>()) {}

    void SetUp() override {
        ON_CALL(*metaServerCli_, UpdateInodeAttrWithOutNlink(_, _, _, _, _, _))
            .WillByDefault(Return(MetaStatusCode::OK));
    }

 protected:
    MockSpaceManager spaceMgr_;
    MockBlockDeviceClient blockDev_;
    MockInodeCacheManager inodeCacheMgr_;
    DefaultVolumeStorage storage_;
    std::shared_ptr<MockMetaServerClient> metaServerCli_;
};

TEST_F(DefaultVolumeStorageTest, WriteAndReadTest_InodeNotFound) {
    EXPECT_CALL(inodeCacheMgr_, GetInode(_, _))
        .Times(2)
        .WillRepeatedly(Return(CURVEFS_ERROR::NOTEXIST));

    uint64_t ino = 1;
    off_t offset = 0;
    size_t len = 4096;
    std::unique_ptr<char[]> data(new char[len]);

    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST,
              storage_.Read(ino, offset, len, data.get()));
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST,
              storage_.Write(ino, offset, len, data.get()));
}

TEST_F(DefaultVolumeStorageTest, ReadTest_BlockDevReadError) {
    Inode inode;
    inode.set_type(FsFileType::TYPE_FILE);

    VolumeExtentList exts;
    auto* slice = exts.add_slices();
    slice->set_offset(0);
    auto* ext = slice->add_extents();
    ext->set_fsoffset(0);
    ext->set_length(4096);
    ext->set_volumeoffset(8192);
    ext->set_isused(true);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaServerCli_);

    EXPECT_CALL(*metaServerCli_, GetVolumeExtent(_, _, _, _))
        .WillOnce(
            Invoke([&](uint32_t, uint64_t, bool, VolumeExtentList* extents) {
                *extents = exts;
                return MetaStatusCode::OK;
            }));

    ASSERT_EQ(CURVEFS_ERROR::OK, inodeWrapper->RefreshVolumeExtent());

    EXPECT_CALL(inodeCacheMgr_, GetInode(_, _))
        .WillOnce(Invoke([&](uint64_t, std::shared_ptr<InodeWrapper>& out) {
            out = inodeWrapper;
            return CURVEFS_ERROR::OK;
        }));

    EXPECT_CALL(blockDev_, Readv(_))
        .WillOnce(Return(-1));

    uint64_t ino = 1;
    off_t offset = 0;
    size_t len = 4096;
    std::unique_ptr<char[]> data(new char[len]);

    ASSERT_EQ(CURVEFS_ERROR::IO_ERROR,
              storage_.Read(ino, offset, len, data.get()));
}

TEST_F(DefaultVolumeStorageTest, ReadTest_BlockDevReadSuccess) {
    Inode inode;
    inode.set_type(FsFileType::TYPE_FILE);

    VolumeExtentList exts;
    auto* slice = exts.add_slices();
    slice->set_offset(0);
    auto* ext = slice->add_extents();
    ext->set_fsoffset(0);
    ext->set_length(4096);
    ext->set_volumeoffset(8192);
    ext->set_isused(true);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaServerCli_);

    EXPECT_CALL(*metaServerCli_, GetVolumeExtent(_, _, _, _))
        .WillOnce(
            Invoke([&](uint32_t, uint64_t, bool, VolumeExtentList* extents) {
                *extents = exts;
                return MetaStatusCode::OK;
            }));

    ASSERT_EQ(CURVEFS_ERROR::OK, inodeWrapper->RefreshVolumeExtent());

    EXPECT_CALL(inodeCacheMgr_, GetInode(_, _))
        .WillOnce(Invoke([&](uint64_t, std::shared_ptr<InodeWrapper>& out) {
            out = inodeWrapper;
            return CURVEFS_ERROR::OK;
        }));

    uint64_t ino = 1;
    off_t offset = 0;
    size_t len = 4096;
    std::unique_ptr<char[]> data(new char[len]);

    EXPECT_CALL(blockDev_, Readv(_))
        .WillOnce(Return(len));

    EXPECT_CALL(inodeCacheMgr_, ShipToFlush(inodeWrapper))
        .Times(1);

    ASSERT_EQ(CURVEFS_ERROR::OK, storage_.Read(ino, offset, len, data.get()));
}

TEST_F(DefaultVolumeStorageTest, ReadTest_BlockDevReadHoleSuccess) {
    Inode inode;
    inode.set_type(FsFileType::TYPE_FILE);

    VolumeExtentList exts;
    auto* slice = exts.add_slices();
    slice->set_offset(0);
    auto* ext = slice->add_extents();
    ext->set_fsoffset(0);
    ext->set_length(4096);
    ext->set_volumeoffset(8192);
    ext->set_isused(false);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaServerCli_);

    EXPECT_CALL(*metaServerCli_, GetVolumeExtent(_, _, _, _))
        .WillOnce(
            Invoke([&](uint32_t, uint64_t, bool, VolumeExtentList* extents) {
                *extents = exts;
                return MetaStatusCode::OK;
            }));

    ASSERT_EQ(CURVEFS_ERROR::OK, inodeWrapper->RefreshVolumeExtent());

    EXPECT_CALL(inodeCacheMgr_, GetInode(_, _))
        .WillOnce(Invoke([&](uint64_t, std::shared_ptr<InodeWrapper>& out) {
            out = inodeWrapper;
            return CURVEFS_ERROR::OK;
        }));

    uint64_t ino = 1;
    off_t offset = 0;
    size_t len = 4096;
    std::unique_ptr<char[]> data(new char[4096]);

    memset(data.get(), 'x', len);

    EXPECT_CALL(blockDev_, Readv(_))
        .Times(0);

    EXPECT_CALL(inodeCacheMgr_, ShipToFlush(inodeWrapper))
        .Times(1);

    ASSERT_EQ(CURVEFS_ERROR::OK, storage_.Read(ino, offset, len, data.get()));

    for (size_t i = 0; i < len; ++i) {
        ASSERT_EQ(data[i], 0);
    }
}

TEST_F(DefaultVolumeStorageTest, WriteTest_PrepareError) {
    Inode inode;
    inode.set_type(FsFileType::TYPE_FILE);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaServerCli_);

    EXPECT_CALL(*metaServerCli_, GetVolumeExtent(_, _, _, _))
        .WillOnce(
            Invoke([&](uint32_t, uint64_t, bool, VolumeExtentList* extents) {
                extents->clear_slices();
                return MetaStatusCode::OK;
            }));

    ASSERT_EQ(CURVEFS_ERROR::OK, inodeWrapper->RefreshVolumeExtent());

    EXPECT_CALL(inodeCacheMgr_, GetInode(_, _))
        .WillOnce(Invoke([&](uint64_t, std::shared_ptr<InodeWrapper>& out) {
            out = inodeWrapper;
            return CURVEFS_ERROR::OK;
        }));

    EXPECT_CALL(spaceMgr_, Alloc(_, _, _))
        .WillOnce(Return(false));

    uint64_t ino = 1;
    off_t offset = 0;
    size_t len = 4096;
    std::unique_ptr<char[]> data(new char[len]);

    ASSERT_EQ(CURVEFS_ERROR::NO_SPACE,
              storage_.Write(ino, offset, len, data.get()));
}

TEST_F(DefaultVolumeStorageTest, WriteTest_BlockDevWriteError) {
    Inode inode;
    inode.set_type(FsFileType::TYPE_FILE);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaServerCli_);

    EXPECT_CALL(*metaServerCli_, GetVolumeExtent(_, _, _, _))
        .WillOnce(
            Invoke([&](uint32_t, uint64_t, bool, VolumeExtentList* extents) {
                extents->clear_slices();
                return MetaStatusCode::OK;
            }));

    ASSERT_EQ(CURVEFS_ERROR::OK, inodeWrapper->RefreshVolumeExtent());

    EXPECT_CALL(inodeCacheMgr_, GetInode(_, _))
        .WillOnce(Invoke([&](uint64_t, std::shared_ptr<InodeWrapper>& out) {
            out = inodeWrapper;
            return CURVEFS_ERROR::OK;
        }));

    std::vector<WritePart> parts;
    parts.push_back({});
    parts.push_back({});

    EXPECT_CALL(spaceMgr_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint32_t size, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(size, size);
                return true;
            }));

    uint64_t ino = 1;
    off_t offset = 0;
    size_t len = 4096;
    std::unique_ptr<char[]> data(new char[len]);

    EXPECT_CALL(blockDev_, Writev(_))
        .WillOnce(Return(-1));

    ASSERT_EQ(CURVEFS_ERROR::IO_ERROR,
              storage_.Write(ino, offset, len, data.get()));
}

TEST_F(DefaultVolumeStorageTest, WriteTest_BlockDevWriteSuccess) {
    Inode inode;
    inode.set_type(FsFileType::TYPE_FILE);

    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, metaServerCli_);

    EXPECT_CALL(*metaServerCli_, GetVolumeExtent(_, _, _, _))
        .WillOnce(
            Invoke([&](uint32_t, uint64_t, bool, VolumeExtentList* extents) {
                extents->clear_slices();
                return MetaStatusCode::OK;
            }));

    ASSERT_EQ(CURVEFS_ERROR::OK, inodeWrapper->RefreshVolumeExtent());


    EXPECT_CALL(inodeCacheMgr_, GetInode(_, _))
        .WillOnce(Invoke([&](uint64_t, std::shared_ptr<InodeWrapper>& out) {
            out = inodeWrapper;
            return CURVEFS_ERROR::OK;
        }));

    std::vector<WritePart> parts;
    parts.push_back({});
    parts.push_back({});

    EXPECT_CALL(spaceMgr_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint32_t size, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(size, size);
                return true;
            }));

    uint64_t ino = 1;
    off_t offset = 0;
    size_t len = 4096;
    std::unique_ptr<char[]> data(new char[len]);

    EXPECT_CALL(blockDev_, Writev(_))
        .WillOnce(Return(len));

    EXPECT_CALL(inodeCacheMgr_, ShipToFlush(inodeWrapper))
        .Times(1);

    ASSERT_EQ(CURVEFS_ERROR::OK, storage_.Write(ino, offset, len, data.get()));

    ASSERT_EQ(offset + len, inodeWrapper->GetInode().length());
}

}  // namespace client
}  // namespace curvefs
