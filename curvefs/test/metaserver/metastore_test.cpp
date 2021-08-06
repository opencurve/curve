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
 * @Project: curve
 * @Date: 2021-09-01 19:38:35
 * @Author: chenwei
 */

#include "curvefs/src/metaserver/metastore.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <condition_variable>  // NOLINT
#include "curvefs/src/common/process.h"

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

namespace curvefs {
namespace metaserver {
class MetastoreTest : public ::testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}

    bool CompareInode(const Inode &first, const Inode &second) {
        return first.fsid() == second.fsid() &&
               first.atime() == second.atime() &&
               first.inodeid() == second.inodeid() &&
               first.length() == second.length() &&
               first.uid() == second.uid() && first.gid() == second.gid() &&
               first.mode() == second.mode() && first.type() == second.type() &&
               first.mtime() == second.mtime() &&
               first.ctime() == second.ctime() &&
               first.symlink() == second.symlink() &&
               first.nlink() == second.nlink();
    }

    void PrintDentry(const Dentry &dentry) {
        LOG(INFO) << "dentry: fsid = " << dentry.fsid()
                  << ", inodeid = " << dentry.inodeid()
                  << ", name = " << dentry.name()
                  << ", parentinodeid = " << dentry.parentinodeid();
    }

    bool CompareDentry(const Dentry &first, const Dentry &second) {
        bool ret = first.fsid() == second.fsid() &&
                   first.inodeid() == second.inodeid() &&
                   first.parentinodeid() == second.parentinodeid() &&
                   first.name() == second.name();
        if (!ret) {
            PrintDentry(first);
            PrintDentry(second);
        }
        return ret;
    }

    bool ComparePartition(const PartitionInfo &first,
                          const PartitionInfo &second) {
        bool ret = first.fsid() == second.fsid() &&
                   first.poolid() == second.poolid() &&
                   first.copysetid() == second.copysetid() &&
                   first.partitionid() == second.partitionid() &&
                   first.start() == second.start() &&
                   first.end() == second.end() &&
                   first.nextid() == second.nextid();
        if (!ret) {
            LOG(INFO) << "first partition :" << first.ShortDebugString()
                      << ", second partiton : " << second.ShortDebugString();
        }
        return ret;
    }

    class OnSnapshotSaveDoneImpl : public OnSnapshotSaveDoneClosure {
     public:
        void SetSuccess() {
            ret_ = true;
            LOG(INFO) << "OnSnapshotSaveDone success";
        }
        void SetError(MetaStatusCode code) {
            ret_ = false;
            LOG(INFO) << "OnSnapshotSaveDone error";
        }
        void Run() {
            LOG(INFO) << "OnSnapshotSaveDone Run";
            std::unique_lock<std::mutex> lk(mtx_);
            condition_.notify_one();
        }
        void Wait() {
            LOG(INFO) << "OnSnapshotSaveDone Wait";
            std::unique_lock<std::mutex> lk(mtx_);
            condition_.wait(lk);
        }
        bool IsSuccess() { return ret_; }

     private:
        bool ret_;
        std::mutex mtx_;
        std::condition_variable condition_;
    };
};

TEST_F(MetastoreTest, partition) {
    MetaStoreImpl metastore;
    CreatePartitionRequest createPartitionRequest;
    CreatePartitionResponse createPartitionResponse;
    PartitionInfo partitionInfo;
    partitionInfo.set_fsid(1);
    partitionInfo.set_poolid(2);
    partitionInfo.set_copysetid(3);
    partitionInfo.set_partitionid(4);
    partitionInfo.set_start(100);
    partitionInfo.set_end(199);
    createPartitionRequest.mutable_partition()->CopyFrom(partitionInfo);
    MetaStatusCode ret = metastore.CreatePartition(&createPartitionRequest,
                                                   &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    ret = metastore.CreatePartition(&createPartitionRequest,
                                    &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::PARTITION_EXIST);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    DeletePartitionRequest deletePartitionRequest;
    DeletePartitionResponse deletePartitionResponse;
    deletePartitionRequest.mutable_partition()->CopyFrom(partitionInfo);
    ret = metastore.DeletePartition(&deletePartitionRequest,
                                    &deletePartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(deletePartitionResponse.statuscode(), ret);

    ret = metastore.DeletePartition(&deletePartitionRequest,
                                    &deletePartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::PARTITION_NOT_FOUND);
    ASSERT_EQ(deletePartitionResponse.statuscode(), ret);

    // after delete, create partiton1 again
    ret = metastore.CreatePartition(&createPartitionRequest,
                                    &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    // create partition2
    PartitionInfo partitionInfo2 = partitionInfo;
    partitionInfo2.set_partitionid(2);
    createPartitionRequest.mutable_partition()->CopyFrom(partitionInfo2);
    ret = metastore.CreatePartition(&createPartitionRequest,
                                    &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    // CreateInode
    CreateInodeRequest createRequest;
    CreateInodeResponse createResponse;

    uint32_t poolId = 2;
    uint32_t copysetId = 3;
    uint32_t partitionId = 4;
    uint32_t fsId = 1;
    uint64_t length = 2;
    uint32_t uid = 100;
    uint32_t gid = 200;
    uint32_t mode = 777;
    FsFileType type = FsFileType::TYPE_DIRECTORY;

    createRequest.set_poolid(poolId);
    createRequest.set_copysetid(copysetId);
    createRequest.set_partitionid(1);
    createRequest.set_fsid(fsId);
    createRequest.set_length(length);
    createRequest.set_uid(uid);
    createRequest.set_gid(gid);
    createRequest.set_mode(mode);
    createRequest.set_type(type);

    ret = metastore.CreateInode(&createRequest, &createResponse);
    ASSERT_EQ(createResponse.statuscode(), ret);
    ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::PARTITION_NOT_FOUND);

    createRequest.set_partitionid(partitionId);
    ret = metastore.CreateInode(&createRequest, &createResponse);
    ASSERT_EQ(createResponse.statuscode(), ret);
    ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::OK);

    // partition has mata, delete fail
    ret = metastore.DeletePartition(&deletePartitionRequest,
                                    &deletePartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::PARTITION_BUSY);
    ASSERT_EQ(deletePartitionResponse.statuscode(), ret);
}

TEST_F(MetastoreTest, test_inode) {
    MetaStoreImpl metastore;

    // create partition1 partition2
    CreatePartitionRequest createPartitionRequest;
    CreatePartitionResponse createPartitionResponse;
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(1);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);
    createPartitionRequest.mutable_partition()->CopyFrom(partitionInfo1);
    MetaStatusCode ret = metastore.CreatePartition(&createPartitionRequest,
                                                   &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    // test CreateInde
    CreateInodeRequest createRequest;
    CreateInodeResponse createResponse;

    uint32_t poolId = 2;
    uint32_t copysetId = 3;
    uint32_t partitionId = 1;
    uint32_t fsId = 1;
    uint64_t length = 2;
    uint32_t uid = 100;
    uint32_t gid = 200;
    uint32_t mode = 777;
    FsFileType type = FsFileType::TYPE_DIRECTORY;

    createRequest.set_poolid(poolId);
    createRequest.set_copysetid(copysetId);
    createRequest.set_partitionid(666);
    createRequest.set_fsid(fsId);
    createRequest.set_length(length);
    createRequest.set_uid(uid);
    createRequest.set_gid(gid);
    createRequest.set_mode(mode);
    createRequest.set_type(type);

    // CreateInde wrong partitionid
    ret = metastore.CreateInode(&createRequest, &createResponse);
    ASSERT_EQ(createResponse.statuscode(), ret);
    ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::PARTITION_NOT_FOUND);

    createRequest.set_partitionid(partitionId);
    ret = metastore.CreateInode(&createRequest, &createResponse);
    ASSERT_EQ(createResponse.statuscode(), ret);
    ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_TRUE(createResponse.has_inode());
    ASSERT_EQ(createResponse.inode().inodeid(), partitionInfo1.start());
    ASSERT_EQ(createResponse.inode().length(), length);
    ASSERT_EQ(createResponse.inode().uid(), uid);
    ASSERT_EQ(createResponse.inode().gid(), gid);
    ASSERT_EQ(createResponse.inode().mode(), mode);
    ASSERT_EQ(createResponse.inode().type(), type);

    createRequest.set_type(FsFileType::TYPE_S3);
    CreateInodeResponse createResponse2;
    ret = metastore.CreateInode(&createRequest, &createResponse2);
    ASSERT_EQ(createResponse2.statuscode(), ret);
    ASSERT_EQ(createResponse2.statuscode(), MetaStatusCode::OK);
    ASSERT_TRUE(createResponse2.has_inode());
    ASSERT_EQ(createResponse2.inode().inodeid(), partitionInfo1.start() + 1);
    ASSERT_EQ(createResponse2.inode().length(), length);
    ASSERT_EQ(createResponse2.inode().uid(), uid);
    ASSERT_EQ(createResponse2.inode().gid(), gid);
    ASSERT_EQ(createResponse2.inode().mode(), mode);
    ASSERT_EQ(createResponse2.inode().type(), FsFileType::TYPE_S3);

    // type symlink
    createRequest.set_type(FsFileType::TYPE_SYM_LINK);
    CreateInodeResponse createResponse3;
    ret = metastore.CreateInode(&createRequest, &createResponse3);
    ASSERT_EQ(createResponse3.statuscode(), MetaStatusCode::SYM_LINK_EMPTY);

    createRequest.set_type(FsFileType::TYPE_SYM_LINK);
    createRequest.set_symlink("");
    ret = metastore.CreateInode(&createRequest, &createResponse3);
    ASSERT_EQ(createResponse3.statuscode(), MetaStatusCode::SYM_LINK_EMPTY);

    createRequest.set_type(FsFileType::TYPE_SYM_LINK);
    createRequest.set_symlink("symlink");
    ret = metastore.CreateInode(&createRequest, &createResponse3);
    ASSERT_EQ(createResponse3.statuscode(), ret);
    ASSERT_EQ(createResponse3.statuscode(), MetaStatusCode::OK);
    ASSERT_TRUE(createResponse3.has_inode());
    ASSERT_EQ(createResponse3.inode().inodeid(), partitionInfo1.start() + 2);
    ASSERT_EQ(createResponse3.inode().length(), length);
    ASSERT_EQ(createResponse3.inode().uid(), uid);
    ASSERT_EQ(createResponse3.inode().gid(), gid);
    ASSERT_EQ(createResponse3.inode().mode(), mode);
    ASSERT_EQ(createResponse3.inode().type(), FsFileType::TYPE_SYM_LINK);
    ASSERT_EQ(createResponse3.inode().symlink(), "symlink");

    // TEST GET INODE
    GetInodeRequest getRequest;
    GetInodeResponse getResponse;
    getRequest.set_poolid(poolId);
    getRequest.set_copysetid(copysetId);
    getRequest.set_partitionid(666);
    getRequest.set_fsid(fsId);
    getRequest.set_inodeid(createResponse.inode().inodeid());

    // GetInode wrong partitionid
    ret = metastore.GetInode(&getRequest, &getResponse);
    ASSERT_EQ(getResponse.statuscode(), MetaStatusCode::PARTITION_NOT_FOUND);
    ASSERT_EQ(getResponse.statuscode(), ret);

    getRequest.set_partitionid(partitionId);
    ret = metastore.GetInode(&getRequest, &getResponse);
    ASSERT_EQ(getResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(getResponse.statuscode(), ret);
    ASSERT_TRUE(getResponse.has_inode());
    ASSERT_EQ(getResponse.inode().fsid(), fsId);
    ASSERT_EQ(getResponse.inode().length(), length);
    ASSERT_EQ(getResponse.inode().uid(), uid);
    ASSERT_EQ(getResponse.inode().gid(), gid);
    ASSERT_EQ(getResponse.inode().mode(), mode);
    ASSERT_EQ(getResponse.inode().type(), type);
    ASSERT_TRUE(CompareInode(createResponse.inode(), getResponse.inode()));

    GetInodeRequest getRequest2;
    GetInodeResponse getResponse2;
    getRequest2.set_poolid(poolId);
    getRequest2.set_copysetid(copysetId);
    getRequest2.set_partitionid(partitionId);
    getRequest2.set_fsid(fsId);
    getRequest2.set_inodeid(createResponse.inode().inodeid() + 100);
    ret = metastore.GetInode(&getRequest2, &getResponse2);
    ASSERT_EQ(getResponse2.statuscode(), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(getResponse2.statuscode(), ret);

    // update inode
    // no param need update
    UpdateInodeRequest updateRequest;
    UpdateInodeResponse updateResponse;
    updateRequest.set_poolid(poolId);
    updateRequest.set_copysetid(copysetId);
    updateRequest.set_partitionid(666);
    updateRequest.set_fsid(fsId);
    updateRequest.set_inodeid(createResponse.inode().inodeid());

    // UpdateInode wrong partitionid
    ret = metastore.UpdateInode(&updateRequest, &updateResponse);
    ASSERT_EQ(updateResponse.statuscode(), MetaStatusCode::PARTITION_NOT_FOUND);
    ASSERT_EQ(updateResponse.statuscode(), ret);

    updateRequest.set_partitionid(partitionId);
    ret = metastore.UpdateInode(&updateRequest, &updateResponse);
    ASSERT_EQ(updateResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(updateResponse.statuscode(), ret);

    GetInodeRequest getRequest3;
    GetInodeResponse getResponse3;
    getRequest3.set_poolid(poolId);
    getRequest3.set_copysetid(copysetId);
    getRequest3.set_partitionid(partitionId);
    getRequest3.set_fsid(fsId);
    getRequest3.set_inodeid(createResponse.inode().inodeid());
    ret = metastore.GetInode(&getRequest3, &getResponse3);
    ASSERT_EQ(getResponse3.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(getResponse3.statuscode(), ret);
    ASSERT_TRUE(CompareInode(createResponse.inode(), getResponse3.inode()));

    UpdateInodeRequest updateRequest2;
    UpdateInodeResponse updateResponse2;
    updateRequest2.set_poolid(poolId);
    updateRequest2.set_copysetid(copysetId);
    updateRequest2.set_partitionid(partitionId);
    updateRequest2.set_fsid(fsId);
    updateRequest2.set_inodeid(createResponse.inode().inodeid());
    updateRequest2.set_length(length + 1);
    ret = metastore.UpdateInode(&updateRequest2, &updateResponse2);
    ASSERT_EQ(updateResponse2.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(updateResponse2.statuscode(), ret);

    GetInodeRequest getRequest4;
    GetInodeResponse getResponse4;
    getRequest4.set_poolid(poolId);
    getRequest4.set_copysetid(copysetId);
    getRequest4.set_partitionid(partitionId);
    getRequest4.set_fsid(fsId);
    getRequest4.set_inodeid(createResponse.inode().inodeid());
    ret = metastore.GetInode(&getRequest4, &getResponse4);
    ASSERT_EQ(getResponse4.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(getResponse4.statuscode(), ret);
    ASSERT_FALSE(CompareInode(createResponse.inode(), getResponse4.inode()));
    ASSERT_EQ(getResponse4.inode().length(), length + 1);

    UpdateInodeRequest updateRequest3;
    UpdateInodeResponse updateResponse3;
    updateRequest3.set_poolid(poolId);
    updateRequest3.set_copysetid(copysetId);
    updateRequest3.set_partitionid(partitionId);
    updateRequest3.set_fsid(fsId);
    updateRequest3.set_inodeid(createResponse.inode().inodeid());
    VolumeExtentList volumeExtentList;
    updateRequest3.mutable_volumeextentlist()->CopyFrom(volumeExtentList);
    S3ChunkInfoList s3ChunkInfoList;
    updateRequest3.mutable_s3chunkinfolist()->CopyFrom(s3ChunkInfoList);
    ret = metastore.UpdateInode(&updateRequest3, &updateResponse3);
    ASSERT_EQ(updateResponse3.statuscode(), MetaStatusCode::PARAM_ERROR);
    ASSERT_EQ(updateResponse3.statuscode(), ret);

    // UPDATE INODE VERSION
    UpdateInodeS3VersionRequest updateVersionRequest;
    UpdateInodeS3VersionResponse updateVersionResponse;
    updateVersionRequest.set_poolid(poolId);
    updateVersionRequest.set_copysetid(copysetId);
    updateVersionRequest.set_partitionid(666);
    updateVersionRequest.set_fsid(fsId);
    updateVersionRequest.set_inodeid(createResponse2.inode().inodeid());

    // updateVersionRequest wrong partitionid
    ret = metastore.UpdateInodeS3Version(&updateVersionRequest,
                                         &updateVersionResponse);
    ASSERT_EQ(updateVersionResponse.statuscode(),
              MetaStatusCode::PARTITION_NOT_FOUND);
    ASSERT_EQ(updateVersionResponse.statuscode(), ret);

    updateVersionRequest.set_partitionid(partitionId);
    ret = metastore.UpdateInodeS3Version(&updateVersionRequest,
                                         &updateVersionResponse);
    ASSERT_EQ(updateVersionResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(updateVersionResponse.statuscode(), ret);
    ASSERT_EQ(updateVersionResponse.version(), 1);

    updateVersionRequest.set_fsid(fsId);
    updateVersionRequest.set_inodeid(createResponse2.inode().inodeid());
    ret = metastore.UpdateInodeS3Version(&updateVersionRequest,
                                         &updateVersionResponse);
    ASSERT_EQ(updateVersionResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(updateVersionResponse.statuscode(), ret);
    ASSERT_EQ(updateVersionResponse.version(), 2);

    updateVersionRequest.set_fsid(fsId);
    updateVersionRequest.set_inodeid(createResponse.inode().inodeid());
    ret = metastore.UpdateInodeS3Version(&updateVersionRequest,
                                         &updateVersionResponse);
    ASSERT_EQ(updateVersionResponse.statuscode(), MetaStatusCode::PARAM_ERROR);
    ASSERT_EQ(updateVersionResponse.statuscode(), ret);

    // DELETE INODE
    DeleteInodeRequest deleteRequest;
    DeleteInodeResponse deleteResponse;
    deleteRequest.set_poolid(poolId);
    deleteRequest.set_copysetid(copysetId);
    deleteRequest.set_partitionid(666);
    deleteRequest.set_fsid(fsId);
    deleteRequest.set_inodeid(createResponse.inode().inodeid());

    // DeleteInode wrong partitionid
    ret = metastore.DeleteInode(&deleteRequest, &deleteResponse);
    ASSERT_EQ(deleteResponse.statuscode(), MetaStatusCode::PARTITION_NOT_FOUND);
    ASSERT_EQ(deleteResponse.statuscode(), ret);

    deleteRequest.set_partitionid(partitionId);
    ret = metastore.DeleteInode(&deleteRequest, &deleteResponse);
    ASSERT_EQ(deleteResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(deleteResponse.statuscode(), ret);

    ret = metastore.DeleteInode(&deleteRequest, &deleteResponse);
    ASSERT_EQ(deleteResponse.statuscode(), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(deleteResponse.statuscode(), ret);
}

TEST_F(MetastoreTest, test_dentry) {
    MetaStoreImpl metastore;

    // create partition1 partition2
    CreatePartitionRequest createPartitionRequest;
    CreatePartitionResponse createPartitionResponse;
    PartitionInfo partitionInfo1;
    partitionInfo1.set_fsid(1);
    partitionInfo1.set_poolid(2);
    partitionInfo1.set_copysetid(3);
    partitionInfo1.set_partitionid(1);
    partitionInfo1.set_start(100);
    partitionInfo1.set_end(199);
    createPartitionRequest.mutable_partition()->CopyFrom(partitionInfo1);
    MetaStatusCode ret = metastore.CreatePartition(&createPartitionRequest,
                                                   &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    PartitionInfo partitionInfo2 = partitionInfo1;
    partitionInfo2.set_partitionid(2);
    createPartitionRequest.mutable_partition()->CopyFrom(partitionInfo2);
    ret = metastore.CreatePartition(&createPartitionRequest,
                                    &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    // test CreateDentry
    CreateDentryRequest createRequest;
    CreateDentryResponse createResponse;

    uint32_t poolId = 2;
    uint32_t copysetId = 3;
    uint32_t partitionId = 1;
    uint32_t fsId = 1;
    uint64_t inodeId = 2;
    uint64_t parentId = 3;

    std::string name = "dentry1";

    Dentry dentry1;
    dentry1.set_fsid(fsId);
    dentry1.set_inodeid(inodeId);
    dentry1.set_parentinodeid(parentId);
    dentry1.set_name(name);

    createRequest.set_poolid(poolId);
    createRequest.set_copysetid(copysetId);
    createRequest.set_partitionid(666);
    createRequest.mutable_dentry()->CopyFrom(dentry1);

    // CreateDentry wrong partitionid
    ret = metastore.CreateDentry(&createRequest, &createResponse);
    ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::PARTITION_NOT_FOUND);
    ASSERT_EQ(createResponse.statuscode(), ret);

    createRequest.set_partitionid(partitionId);
    ret = metastore.CreateDentry(&createRequest, &createResponse);
    ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(createResponse.statuscode(), ret);

    ret = metastore.CreateDentry(&createRequest, &createResponse);
    ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::DENTRY_EXIST);
    ASSERT_EQ(createResponse.statuscode(), ret);

    Dentry dentry2;
    dentry2.set_fsid(fsId);
    dentry2.set_inodeid(inodeId + 1);
    dentry2.set_parentinodeid(parentId);
    dentry2.set_name("dentry2");
    createRequest.mutable_dentry()->CopyFrom(dentry2);

    ret = metastore.CreateDentry(&createRequest, &createResponse);
    ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(createResponse.statuscode(), ret);

    Dentry dentry3;
    dentry3.set_fsid(fsId);
    dentry3.set_inodeid(inodeId + 2);
    dentry3.set_parentinodeid(parentId);
    dentry3.set_name("dentry3");
    createRequest.mutable_dentry()->CopyFrom(dentry3);

    ret = metastore.CreateDentry(&createRequest, &createResponse);
    ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(createResponse.statuscode(), ret);

    // TEST GET DETNRY
    GetDentryRequest getRequest;
    GetDentryResponse getResponse;
    getRequest.set_poolid(poolId);
    getRequest.set_copysetid(copysetId);
    getRequest.set_partitionid(666);
    getRequest.set_fsid(fsId);
    getRequest.set_parentinodeid(parentId);
    getRequest.set_name(name);

    // GetDentry wrong partitionid
    ret = metastore.GetDentry(&getRequest, &getResponse);
    ASSERT_EQ(getResponse.statuscode(), MetaStatusCode::PARTITION_NOT_FOUND);
    ASSERT_EQ(getResponse.statuscode(), ret);

    getRequest.set_partitionid(partitionId);
    ret = metastore.GetDentry(&getRequest, &getResponse);
    ASSERT_EQ(getResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(getResponse.statuscode(), ret);
    ASSERT_TRUE(getResponse.has_dentry());
    ASSERT_TRUE(CompareDentry(dentry1, getResponse.dentry()));

    getRequest.set_fsid(fsId + 1);
    getRequest.set_parentinodeid(parentId);
    getRequest.set_name(name);
    ret = metastore.GetDentry(&getRequest, &getResponse);
    ASSERT_EQ(getResponse.statuscode(), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(getResponse.statuscode(), ret);

    // TEST LIST DENTRY
    ListDentryRequest listRequest;
    ListDentryResponse listResponse;
    listRequest.set_poolid(poolId);
    listRequest.set_copysetid(copysetId);
    listRequest.set_partitionid(666);
    listRequest.set_fsid(fsId);
    listRequest.set_dirinodeid(parentId);

    // ListDentry wrong partitionid
    ret = metastore.ListDentry(&listRequest, &listResponse);
    ASSERT_EQ(listResponse.statuscode(), MetaStatusCode::PARTITION_NOT_FOUND);
    ASSERT_EQ(listResponse.statuscode(), ret);

    listRequest.set_partitionid(partitionId);
    ret = metastore.ListDentry(&listRequest, &listResponse);
    ASSERT_EQ(listResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(listResponse.statuscode(), ret);
    ASSERT_EQ(listResponse.dentrys_size(), 3);
    ASSERT_TRUE(CompareDentry(listResponse.dentrys(0), dentry1));
    ASSERT_TRUE(CompareDentry(listResponse.dentrys(1), dentry2));
    ASSERT_TRUE(CompareDentry(listResponse.dentrys(2), dentry3));

    listRequest.set_fsid(fsId);
    listRequest.set_dirinodeid(parentId);
    listRequest.set_last("dentry1");
    listRequest.set_count(100);

    listResponse.Clear();
    ret = metastore.ListDentry(&listRequest, &listResponse);
    ASSERT_EQ(listResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(listResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(listResponse.dentrys_size(), 2);
    ASSERT_TRUE(CompareDentry(listResponse.dentrys(0), dentry2));
    ASSERT_TRUE(CompareDentry(listResponse.dentrys(1), dentry3));

    listRequest.set_fsid(fsId);
    listRequest.set_dirinodeid(parentId);
    listRequest.clear_last();
    listRequest.set_count(1);

    listResponse.Clear();
    ret = metastore.ListDentry(&listRequest, &listResponse);
    ASSERT_EQ(listResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(listResponse.statuscode(), ret);
    ASSERT_EQ(listResponse.dentrys_size(), 1);
    ASSERT_TRUE(CompareDentry(listResponse.dentrys(0), dentry1));

    // test delete
    DeleteDentryRequest deleteRequest;
    DeleteDentryResponse deleteResponse;
    deleteRequest.set_poolid(poolId);
    deleteRequest.set_copysetid(copysetId);
    deleteRequest.set_partitionid(666);
    deleteRequest.set_fsid(fsId);
    deleteRequest.set_parentinodeid(parentId);
    deleteRequest.set_name("dentry2");

    // DeleteDentry wrong partitionid
    ret = metastore.DeleteDentry(&deleteRequest, &deleteResponse);
    ASSERT_EQ(deleteResponse.statuscode(), MetaStatusCode::PARTITION_NOT_FOUND);
    ASSERT_EQ(deleteResponse.statuscode(), ret);

    deleteRequest.set_partitionid(partitionId);
    ret = metastore.DeleteDentry(&deleteRequest, &deleteResponse);
    ASSERT_EQ(deleteResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(deleteResponse.statuscode(), ret);

    listRequest.set_fsid(fsId);
    listRequest.set_dirinodeid(parentId);
    listRequest.clear_last();
    listRequest.clear_count();
    listResponse.Clear();
    ret = metastore.ListDentry(&listRequest, &listResponse);
    ASSERT_EQ(listResponse.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(listResponse.statuscode(), ret);
    ASSERT_EQ(listResponse.dentrys_size(), 2);
    ASSERT_TRUE(CompareDentry(listResponse.dentrys(0), dentry1));
    ASSERT_TRUE(CompareDentry(listResponse.dentrys(1), dentry3));
    // ASSERT_TRUE(CompareDentry(listResponse.dentrys(2), dentry3));

    ret = metastore.DeleteDentry(&deleteRequest, &deleteResponse);
    ASSERT_EQ(deleteResponse.statuscode(), MetaStatusCode::NOT_FOUND);
}

TEST_F(MetastoreTest, persist_success) {
    MetaStoreImpl metastore;
    uint32_t partitionId = 4;
    uint32_t partitionId2 = 2;
    // create partition1
    CreatePartitionRequest createPartitionRequest;
    CreatePartitionResponse createPartitionResponse;
    PartitionInfo partitionInfo;
    partitionInfo.set_fsid(1);
    partitionInfo.set_poolid(2);
    partitionInfo.set_copysetid(3);
    partitionInfo.set_partitionid(partitionId);
    partitionInfo.set_start(100);
    partitionInfo.set_end(199);
    createPartitionRequest.mutable_partition()->CopyFrom(partitionInfo);
    MetaStatusCode ret = metastore.CreatePartition(&createPartitionRequest,
                                                   &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    ret = metastore.CreatePartition(&createPartitionRequest,
                                    &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::PARTITION_EXIST);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    // create partition2
    PartitionInfo partitionInfo2 = partitionInfo;
    partitionInfo2.set_partitionid(partitionId2);
    createPartitionRequest.mutable_partition()->CopyFrom(partitionInfo2);
    ret = metastore.CreatePartition(&createPartitionRequest,
                                    &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    // add 2 inode to partion1
    CreateInodeRequest createInodeRequest;
    CreateInodeResponse createInodeResponse1;
    CreateInodeResponse createInodeResponse2;

    uint32_t poolId = 2;
    uint32_t copysetId = 3;
    uint32_t fsId = 1;
    uint64_t length = 2;
    uint32_t uid = 100;
    uint32_t gid = 200;
    uint32_t mode = 777;
    FsFileType type = FsFileType::TYPE_DIRECTORY;

    createInodeRequest.set_poolid(poolId);
    createInodeRequest.set_copysetid(copysetId);
    createInodeRequest.set_partitionid(partitionId);
    createInodeRequest.set_fsid(fsId);
    createInodeRequest.set_length(length);
    createInodeRequest.set_uid(uid);
    createInodeRequest.set_gid(gid);
    createInodeRequest.set_mode(mode);
    createInodeRequest.set_type(type);

    ret = metastore.CreateInode(&createInodeRequest, &createInodeResponse1);
    ASSERT_EQ(createInodeResponse1.statuscode(), ret);
    ASSERT_EQ(createInodeResponse1.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(createInodeResponse1.inode().inodeid(), 100);

    createInodeRequest.set_partitionid(partitionId);
    ret = metastore.CreateInode(&createInodeRequest, &createInodeResponse2);
    ASSERT_EQ(createInodeResponse2.statuscode(), ret);
    ASSERT_EQ(createInodeResponse2.statuscode(), MetaStatusCode::OK);

    // add 2 dentry to partiton1
    CreateDentryRequest createDentryRequest;
    CreateDentryResponse createDentryResponse1;
    CreateDentryResponse createDentryResponse2;
    Dentry dentry1;
    dentry1.set_fsid(fsId);
    dentry1.set_inodeid(1);
    dentry1.set_parentinodeid(0);
    dentry1.set_name("dentry1");
    dentry1.set_txid(1);


    createDentryRequest.set_poolid(poolId);
    createDentryRequest.set_copysetid(copysetId);
    createDentryRequest.set_partitionid(partitionId);
    createDentryRequest.mutable_dentry()->CopyFrom(dentry1);

    ret = metastore.CreateDentry(&createDentryRequest, &createDentryResponse1);
    ASSERT_EQ(createDentryResponse1.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(createDentryResponse1.statuscode(), ret);

    Dentry dentry2 = dentry1;
    dentry2.set_inodeid(2);
    dentry2.set_name("dentry2");
    createDentryRequest.mutable_dentry()->CopyFrom(dentry2);
    ret = metastore.CreateDentry(&createDentryRequest, &createDentryResponse2);
    ASSERT_EQ(createDentryResponse2.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(createDentryResponse2.statuscode(), ret);

    // dump MetaStoreImpl to file
    OnSnapshotSaveDoneImpl done;
    LOG(INFO) << "MetastoreTest test Save";
    ASSERT_TRUE(metastore.Save("./metastore_test", &done));

    // wait meta save to file
    done.Wait();
    ASSERT_TRUE(done.IsSuccess());

    // load MetaStoreImpl to new meta
    MetaStoreImpl metastoreNew;
    LOG(INFO) << "MetastoreTest test Load";
    ASSERT_TRUE(metastoreNew.Load("./metastore_test"));

    // compare two meta
    ASSERT_TRUE(ComparePartition(
        metastoreNew.GetPartition(partitionId)->GetPartitionInfo(),
        metastore.GetPartition(partitionId)->GetPartitionInfo()));

    ASSERT_TRUE(ComparePartition(
        metastoreNew.GetPartition(partitionId2)->GetPartitionInfo(),
        metastore.GetPartition(partitionId2)->GetPartitionInfo()));

    Inode tempInode;
    metastoreNew.GetPartition(partitionId)
        ->GetInode(fsId, createInodeResponse1.inode().inodeid(), &tempInode);
    ASSERT_TRUE(CompareInode(tempInode, createInodeResponse1.inode()));

    // clear meta
    LOG(INFO) << "MetastoreTest test Clear";
    ASSERT_TRUE(metastore.Clear());
}

TEST_F(MetastoreTest, persist_partition_fail) {
    MetaStoreImpl metastore;
    uint32_t partitionId = 4;
    // create partition1
    CreatePartitionRequest createPartitionRequest;
    CreatePartitionResponse createPartitionResponse;
    PartitionInfo partitionInfo;
    partitionInfo.set_partitionid(partitionId);
    createPartitionRequest.mutable_partition()->CopyFrom(partitionInfo);
    MetaStatusCode ret = metastore.CreatePartition(&createPartitionRequest,
                                                   &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    // dump MetaStoreImpl to file
    OnSnapshotSaveDoneImpl done;
    LOG(INFO) << "MetastoreTest test Save";
    ASSERT_TRUE(metastore.Save("./metastore_test", &done));

    // wait meta save to file
    done.Wait();
    ASSERT_FALSE(done.IsSuccess());
}

TEST_F(MetastoreTest, persist_dentry_fail) {
    MetaStoreImpl metastore;
    uint32_t partitionId = 4;
    uint32_t partitionId2 = 2;
    // create partition1
    CreatePartitionRequest createPartitionRequest;
    CreatePartitionResponse createPartitionResponse;
    PartitionInfo partitionInfo;
    partitionInfo.set_fsid(1);
    partitionInfo.set_poolid(2);
    partitionInfo.set_copysetid(3);
    partitionInfo.set_partitionid(partitionId);
    partitionInfo.set_start(100);
    partitionInfo.set_end(199);
    createPartitionRequest.mutable_partition()->CopyFrom(partitionInfo);
    MetaStatusCode ret = metastore.CreatePartition(&createPartitionRequest,
                                                   &createPartitionResponse);
    ASSERT_EQ(ret, MetaStatusCode::OK);
    ASSERT_EQ(createPartitionResponse.statuscode(), ret);

    // add dentry to partiton1
    CreateDentryRequest createDentryRequest;
    CreateDentryResponse createDentryResponse1;
    CreateDentryResponse createDentryResponse2;
    Dentry dentry1;
    dentry1.set_inodeid(1);
    dentry1.set_parentinodeid(0);
    dentry1.set_name("dentry1");

    createDentryRequest.set_poolid(2);
    createDentryRequest.set_copysetid(3);
    createDentryRequest.set_partitionid(partitionId);
    createDentryRequest.mutable_dentry()->CopyFrom(dentry1);

    ret = metastore.CreateDentry(&createDentryRequest, &createDentryResponse1);
    ASSERT_EQ(createDentryResponse1.statuscode(), MetaStatusCode::OK);
    ASSERT_EQ(createDentryResponse1.statuscode(), ret);

    // dump MetaStoreImpl to file
    OnSnapshotSaveDoneImpl done;
    LOG(INFO) << "MetastoreTest test Save";
    ASSERT_TRUE(metastore.Save("./metastore_test", &done));

    // wait meta save to file
    done.Wait();
    ASSERT_FALSE(done.IsSuccess());

    // clear meta
    LOG(INFO) << "MetastoreTest test Clear";
    ASSERT_TRUE(metastore.Clear());
}

}  // namespace metaserver
}  // namespace curvefs

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    ::curvefs::common::Process::InitSetProcTitle(argc, argv);
    return RUN_ALL_TESTS();
}
