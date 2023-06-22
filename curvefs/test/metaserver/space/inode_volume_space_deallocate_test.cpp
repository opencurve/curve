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
 * Project: curve
 * Date: Tue Apr 18 11:47:54 CST 2023
 * Author: lixiaocui
 */

#include <gtest/gtest.h>

#include "curvefs/src/metaserver/space/inode_volume_space_deallocate.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "curvefs/test/metaserver/storage/utils.h"
#include "curvefs/test/metaserver/space/mock_volume_space_manager.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/test/metaserver/space/utils.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node.h"
#include "curvefs/src/client/rpcclient/task_excutor.h"

namespace curvefs {
namespace metaserver {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using ::curvefs::client::rpcclient::MetaServerClientDone;

namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
}

using curvefs::client::rpcclient::MockMetaServerClient;
using curvefs::common::EmptyMsg;
using curvefs::metaserver::NameGenerator;
using curvefs::metaserver::copyset::MockCopysetNode;
using curvefs::metaserver::storage::Key4VolumeExtentSlice;
using curvefs::metaserver::storage::KVStorage;
using curvefs::metaserver::storage::RandomStoragePath;
using curvefs::metaserver::storage::RocksDBStorage;
using curvefs::metaserver::storage::StorageOptions;


class InodeVolumeSpaceDeallocateTest : public ::testing::Test {
 protected:
    void SetUp() override {
        dataDir_ = RandomStoragePath();
        StorageOptions options;
        options.dataDir = dataDir_;
        options.localFileSystem = localfs.get();
        kvStorage_ = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage_->Open());

        nameGen_ = std::make_shared<NameGenerator>(1);
        inodeStorage_ = std::make_shared<InodeStorage>(kvStorage_, nameGen_, 0);

        volumeSpaceManager_ = std::make_shared<MockVolumeSpaceManager>();
        metaClient_ = std::make_shared<MockMetaServerClient>();

        VolumeDeallocateCalOption calOpt;
        calOpt.kvStorage = kvStorage_;
        calOpt.inodeStorage = inodeStorage_;
        calOpt.nameGen = nameGen_;

        VolumeDeallocateExecuteOption executeOpt;
        executeOpt.volumeSpaceManager = volumeSpaceManager_;
        executeOpt.metaClient = metaClient_;
        executeOpt.batchClean = 10;

        EXPECT_CALL(*volumeSpaceManager_, GetBlockGroupSize(_))
            .WillRepeatedly(Return(blockGroupSize_));
        inodeVolumeSpaceDeallocate_ =
            std::make_shared<InodeVolumeSpaceDeallocate>(fsId_, partitionId_,
                                                         copysetNode_);
        inodeVolumeSpaceDeallocate_->Init(calOpt);
        inodeVolumeSpaceDeallocate_->Init(executeOpt);
        inodeVolumeSpaceDeallocate_->metaCli_ = metaClient_;
    }

    void TearDown() override {
        ASSERT_TRUE(kvStorage_->Close());
        auto output = execShell("rm -rf " + dataDir_);
        ASSERT_EQ(output.size(), 0);
    }

    void PrepareOneDeallocatableInode(Inode *inode, VolumeExtentSlice *slice) {
        // inode
        uint64_t len = 1024 * 1024 * 1024 + 8 * 1024;
        inode->set_inodeid(1);
        inode->set_fsid(fsId_);
        inode->set_length(len);
        inode->set_ctime(1681884587);
        inode->set_ctime_ns(0);
        inode->set_mtime(1681884587);
        inode->set_mtime_ns(0);
        inode->set_atime(1681884587);
        inode->set_atime_ns(0);
        inode->set_uid(100);
        inode->set_gid(100);
        inode->set_mode(777);
        inode->set_nlink(0);
        inode->set_type(FsFileType::TYPE_FILE);

        // extent
        uint64_t validBlockLen = blockGroupSize_ - 4 * 1024;
        slice->set_offset(0);
        uint16_t blockNum = len / validBlockLen;
        for (int i = 0; i <= blockNum; i++) {
            auto extent = slice->add_extents();
            extent->set_fsoffset(validBlockLen * i);
            extent->set_volumeoffset(blockGroupSize_ * i + 4 * 1024);
            if (i == blockNum) {
                extent->set_length(len - validBlockLen * (i - 1));
                extent->set_isused(false);
            } else {
                extent->set_length(validBlockLen);
                extent->set_isused(true);
            }
        }

        // persist extent
        Converter conv;
        auto sliceKey = conv.SerializeToString(
            Key4VolumeExtentSlice(fsId_, 1, slice->offset()));
        auto st = kvStorage_->SSet(nameGen_->GetVolumeExtentTableName(),
                                   sliceKey, *slice);
        ASSERT_TRUE(st.ok());

        // persist inode
        auto inodeKey =
            conv.SerializeToString(Key4Inode(inode->fsid(), inode->inodeid()));
        st = kvStorage_->HSet(nameGen_->GetInodeTableName(), inodeKey, *inode);
        ASSERT_TRUE(st.ok());

        // persist deallocatable inode
        st = kvStorage_->HSet(nameGen_->GetDeallocatableInodeTableName(),
                              inodeKey, EmptyMsg());
        ASSERT_TRUE(st.ok());
    }

    void PrepareDeallocatableGroup(Inode *inode, VolumeExtentSlice *slice,
                                   DeallocatableBlockGroupMap *demap) {
        PrepareOneDeallocatableInode(inode, slice);

        for (auto extent : slice->extents()) {
            DeallocatableBlockGroup tmp;
            uint64_t blockgroupOffset =
                extent.volumeoffset() / blockGroupSize_ * blockGroupSize_;
            tmp.set_blockgroupoffset(blockgroupOffset);
            tmp.set_deallocatablesize(extent.length());
            tmp.add_inodeidlist(inode->inodeid());
            Key4DeallocatableBlockGroup key(fsId_, blockgroupOffset);

            auto st = kvStorage_->HSet(
                nameGen_->GetDeallocatableBlockGroupTableName(),
                key.SerializeToString(), tmp);
            ASSERT_TRUE(st.ok());

            (*demap)[blockgroupOffset] = std::move(tmp);
        }
    }

 protected:
    std::string dataDir_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<MockVolumeSpaceManager> volumeSpaceManager_;
    std::shared_ptr<MockMetaServerClient> metaClient_;
    std::shared_ptr<NameGenerator> nameGen_;
    std::shared_ptr<MockCopysetNode> copysetNode_;

    std::shared_ptr<InodeVolumeSpaceDeallocate> inodeVolumeSpaceDeallocate_;

    uint64_t fsId_ = 1;
    uint32_t partitionId_ = 1;
    uint64_t blockGroupSize_ = 128 * 1024 * 1024;
};

TEST_F(InodeVolumeSpaceDeallocateTest, Test_Status) {
    ASSERT_FALSE(inodeVolumeSpaceDeallocate_->IsCanceled());

    inodeVolumeSpaceDeallocate_->SetCanceled();
    ASSERT_TRUE(inodeVolumeSpaceDeallocate_->IsCanceled());

    inodeVolumeSpaceDeallocate_->SetDeallocateTask(1024);
    ASSERT_EQ(1024, inodeVolumeSpaceDeallocate_->GetDeallocateTask());

    inodeVolumeSpaceDeallocate_->ResetDeallocateTask();
    ASSERT_FALSE(inodeVolumeSpaceDeallocate_->HasDeallocateTask());
}

TEST_F(InodeVolumeSpaceDeallocateTest, Test_CalDeallocatableSpace) {
    // test with empty kvstorage
    inodeVolumeSpaceDeallocate_->CalDeallocatableSpace();

    // test with one inode kvstorage
    Inode inode;
    VolumeExtentSlice slice;
    PrepareOneDeallocatableInode(&inode, &slice);
    EXPECT_CALL(*metaClient_, UpdateDeallocatableBlockGroup(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    inodeVolumeSpaceDeallocate_->CalDeallocatableSpace();
}

TEST_F(InodeVolumeSpaceDeallocateTest, Test_DeallocatableSapceForInode) {
    Inode inode;
    VolumeExtentSlice slice;
    PrepareOneDeallocatableInode(&inode, &slice);
    auto key = Key4Inode(inode.fsid(), inode.inodeid());

    DeallocatableBlockGroupMap increaseMap;
    inodeVolumeSpaceDeallocate_->DeallocatableSapceForInode(key, &increaseMap);

    ASSERT_EQ(increaseMap.size(), slice.extents_size());
    auto iter = slice.extents().begin();
    for (auto &item : increaseMap) {
        auto expectedOffset =
            iter->volumeoffset() / blockGroupSize_ * blockGroupSize_;
        ASSERT_EQ(item.first, expectedOffset);
        ASSERT_EQ(item.second.blockgroupoffset(), expectedOffset);

        auto &increase = item.second.increase();
        ASSERT_EQ(increase.increasedeallocatablesize(), iter->length());
        ASSERT_EQ(increase.inodeidlistadd()[0], inode.inodeid());
        ASSERT_EQ(increase.inodeidlistadd().size(), 1);

        iter++;
    }
}

TEST_F(InodeVolumeSpaceDeallocateTest, Test_DeallocateOneBlockGroup) {
    uint64_t blockGroupOffset = 0;

    // no decallocatable block group
    inodeVolumeSpaceDeallocate_->DeallocateOneBlockGroup(blockGroupOffset);

    // one deallocatable block group
    {
        Inode inode;
        VolumeExtentSlice slice;
        DeallocatableBlockGroupMap demap;
        PrepareDeallocatableGroup(&inode, &slice, &demap);

        EXPECT_CALL(*metaClient_, UpdateDeallocatableBlockGroup(fsId_, _, _))
            .Times(2)
            .WillRepeatedly(Return(MetaStatusCode::OK));
        EXPECT_CALL(*volumeSpaceManager_,
                    DeallocVolumeSpace(fsId_, blockGroupOffset, _))
            .WillOnce(Return(true));
        EXPECT_CALL(*metaClient_, AsyncUpdateVolumeExtent(fsId_, _, _, _))
            .WillOnce(
                Invoke([](uint32_t, uint64_t, const VolumeExtentSliceList &,
                          MetaServerClientDone *done) {
                    done->SetMetaStatusCode(MetaStatusCode::OK);
                    done->Run();
                }));
        inodeVolumeSpaceDeallocate_->DeallocateOneBlockGroup(blockGroupOffset);
    }
}

TEST_F(InodeVolumeSpaceDeallocateTest, Test_ProcessSepcifyInodeList) {
    Inode inode;
    VolumeExtentSlice slice;
    uint64_t blockGroupOffset = 0;
    DeallocatableBlockGroupMap demap;
    PrepareDeallocatableGroup(&inode, &slice, &demap);
    demap[blockGroupOffset].mutable_mark()->add_inodeidunderdeallocate(
        inode.inodeid());

    EXPECT_CALL(*metaClient_, UpdateDeallocatableBlockGroup(fsId_, _, _))
        .Times(2)
        .WillRepeatedly(Return(MetaStatusCode::OK));
    EXPECT_CALL(*volumeSpaceManager_,
                DeallocVolumeSpace(fsId_, blockGroupOffset, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*metaClient_, AsyncUpdateVolumeExtent(fsId_, _, _, _))
        .WillOnce(Invoke([](uint32_t, uint64_t, const VolumeExtentSliceList &,
                            MetaServerClientDone *done) {
            done->SetMetaStatusCode(MetaStatusCode::OK);
            done->Run();
        }));

    inodeVolumeSpaceDeallocate_->ProcessSepcifyInodeList(blockGroupOffset,
                                                         &demap);
    ASSERT_TRUE(
        demap[blockGroupOffset].mark().inodeidunderdeallocate().empty());
}

TEST_F(InodeVolumeSpaceDeallocateTest, Test_DeallocateInode) {
    Inode inode;
    VolumeExtentSlice slice;
    DeallocatableBlockGroupMap demap;
    PrepareDeallocatableGroup(&inode, &slice, &demap);
    uint64_t blockGroupOffset = 0;
    uint64_t decrease = 0;
    Uint64Vec inodelist;
    inodelist.Add(inode.inodeid());

    EXPECT_CALL(*volumeSpaceManager_,
                DeallocVolumeSpace(fsId_, blockGroupOffset, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*metaClient_, AsyncUpdateVolumeExtent(fsId_, _, _, _))
        .WillOnce(Invoke([](uint32_t, uint64_t, const VolumeExtentSliceList &,
                            MetaServerClientDone *done) {
            done->SetMetaStatusCode(MetaStatusCode::OK);
            done->Run();
        }));
    inodeVolumeSpaceDeallocate_->DeallocateInode(blockGroupOffset, inodelist,
                                                 &decrease);
    ASSERT_EQ(decrease, slice.extents(0).length());
}

TEST_F(InodeVolumeSpaceDeallocateTest, Test_UpdateDeallocateInodeExtentSlice) {
    Inode inode;
    VolumeExtentSlice slice;
    PrepareOneDeallocatableInode(&inode, &slice);
    uint64_t blockGroupOffset = 0;
    uint64_t decrease = 0;
    VolumeExtentSliceList slicelist;
    std::vector<Extent> deallocatableVolumeSpace;

    auto st =
        inodeStorage_->GetAllVolumeExtent(fsId_, inode.inodeid(), &slicelist);
    ASSERT_EQ(st, MetaStatusCode::OK);

    // deallocate blockGroupOffset = 0
    {
        inodeVolumeSpaceDeallocate_->UpdateDeallocateInodeExtentSlice(
            blockGroupOffset, inode.inodeid(), &decrease, &slicelist,
            &deallocatableVolumeSpace);

        ASSERT_EQ(slice.extents(0).length(), decrease);
        ASSERT_EQ(deallocatableVolumeSpace.size(), 1);
        ASSERT_EQ(deallocatableVolumeSpace[0].offset,
                  slice.extents(0).volumeoffset());
        ASSERT_EQ(deallocatableVolumeSpace[0].len, slice.extents(0).length());
        ASSERT_EQ(slice.extents_size() - 1, slicelist.slices(0).extents_size());

        deallocatableVolumeSpace.clear();
        decrease = 0;
    }

    // deallocate blockGroupOffset = blockGroupSize
    {
        blockGroupOffset = blockGroupSize_;
        inodeVolumeSpaceDeallocate_->UpdateDeallocateInodeExtentSlice(
            blockGroupOffset, inode.inodeid(), &decrease, &slicelist,
            &deallocatableVolumeSpace);

        ASSERT_EQ(slice.extents(1).length(), decrease);
        ASSERT_EQ(deallocatableVolumeSpace.size(), 1);
        ASSERT_EQ(deallocatableVolumeSpace[0].offset,
                  slice.extents(1).volumeoffset());
        ASSERT_EQ(deallocatableVolumeSpace[0].len, slice.extents(1).length());
        ASSERT_EQ(slice.extents_size() - 2, slicelist.slices(0).extents_size());
    }
}

}  // namespace metaserver
}  // namespace curvefs
