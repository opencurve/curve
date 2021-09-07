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
 * @Date: 2021-06-10 10:47:07
 * @Author: chenwei
 */

#include "curvefs/src/metaserver/metaserver_service.h"
#include <brpc/channel.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;

namespace curvefs {
namespace metaserver {
class MetaserverServiceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        inodeStorage_ = std::make_shared<MemoryInodeStorage>();
        dentryStorage_ = std::make_shared<MemoryDentryStorage>();
        inodeManager_ = std::make_shared<InodeManager>(inodeStorage_);
        txManager_ = std::make_shared<TxManager>(dentryStorage_);
        dentryManager_ = std::make_shared<DentryManager>(dentryStorage_,
                                                         txManager_);
        addr_ = "127.0.0.1:6702";
    }

    void TearDown() override { return; }

    bool CompareInode(const Inode& first, const Inode& second) {
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

    void PrintDentry(const Dentry& dentry) {
        LOG(INFO) << "dentry: fsid = " << dentry.fsid()
                  << ", inodeid = " << dentry.inodeid()
                  << ", name = " << dentry.name()
                  << ", parentinodeid = " << dentry.parentinodeid();
    }

    bool CompareDentry(const Dentry& first, const Dentry& second) {
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

    std::string addr_;
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<DentryStorage> dentryStorage_;
    std::shared_ptr<InodeManager> inodeManager_;
    std::shared_ptr<TxManager> txManager_;
    std::shared_ptr<DentryManager> dentryManager_;
};

TEST_F(MetaserverServiceTest, inodeTest) {
    brpc::Server server;
    // add metaserver service
    MetaServerServiceImpl metaserverService(inodeManager_, dentryManager_);
    ASSERT_EQ(
        server.AddService(&metaserverService, brpc::SERVER_DOESNT_OWN_SERVICE),
        0);

    // start rpc server
    brpc::ServerOptions option;
    ASSERT_EQ(server.Start(addr_.c_str(), &option), 0);

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    MetaServerService_Stub stub(&channel);

    brpc::Controller cntl;

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
    createRequest.set_partitionid(partitionId);
    createRequest.set_fsid(fsId);
    createRequest.set_length(length);
    createRequest.set_uid(uid);
    createRequest.set_gid(gid);
    createRequest.set_mode(mode);
    createRequest.set_type(type);

    stub.CreateInode(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::OK);
        ASSERT_TRUE(createResponse.has_inode());
        ASSERT_EQ(createResponse.inode().length(), length);
        ASSERT_EQ(createResponse.inode().uid(), uid);
        ASSERT_EQ(createResponse.inode().gid(), gid);
        ASSERT_EQ(createResponse.inode().mode(), mode);
        ASSERT_EQ(createResponse.inode().type(), type);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    createRequest.set_type(FsFileType::TYPE_S3);
    CreateInodeResponse createResponse2;
    stub.CreateInode(&cntl, &createRequest, &createResponse2, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse2.statuscode(), MetaStatusCode::OK);
        ASSERT_TRUE(createResponse2.has_inode());
        ASSERT_EQ(createResponse2.inode().length(), length);
        ASSERT_EQ(createResponse2.inode().uid(), uid);
        ASSERT_EQ(createResponse2.inode().gid(), gid);
        ASSERT_EQ(createResponse2.inode().mode(), mode);
        ASSERT_EQ(createResponse2.inode().type(), FsFileType::TYPE_S3);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // type symlink
    cntl.Reset();
    createRequest.set_type(FsFileType::TYPE_SYM_LINK);
    CreateInodeResponse createResponse3;
    stub.CreateInode(&cntl, &createRequest, &createResponse3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse3.statuscode(), MetaStatusCode::SYM_LINK_EMPTY);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    createRequest.set_type(FsFileType::TYPE_SYM_LINK);
    createRequest.set_symlink("");
    stub.CreateInode(&cntl, &createRequest, &createResponse3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse3.statuscode(), MetaStatusCode::SYM_LINK_EMPTY);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    createRequest.set_type(FsFileType::TYPE_SYM_LINK);
    createRequest.set_symlink("symlink");
    stub.CreateInode(&cntl, &createRequest, &createResponse3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse3.statuscode(), MetaStatusCode::OK);
        ASSERT_TRUE(createResponse3.has_inode());
        ASSERT_EQ(createResponse3.inode().length(), length);
        ASSERT_EQ(createResponse3.inode().uid(), uid);
        ASSERT_EQ(createResponse3.inode().gid(), gid);
        ASSERT_EQ(createResponse3.inode().mode(), mode);
        ASSERT_EQ(createResponse3.inode().type(), FsFileType::TYPE_SYM_LINK);
        ASSERT_EQ(createResponse3.inode().symlink(), "symlink");
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // TEST GET INODE
    cntl.Reset();
    GetInodeRequest getRequest;
    GetInodeResponse getResponse;
    getRequest.set_poolid(poolId);
    getRequest.set_copysetid(copysetId);
    getRequest.set_partitionid(partitionId);
    getRequest.set_fsid(fsId);
    getRequest.set_inodeid(createResponse.inode().inodeid());
    stub.GetInode(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), MetaStatusCode::OK);
        ASSERT_TRUE(getResponse.has_inode());
        ASSERT_EQ(getResponse.inode().fsid(), fsId);
        ASSERT_EQ(getResponse.inode().length(), length);
        ASSERT_EQ(getResponse.inode().uid(), uid);
        ASSERT_EQ(getResponse.inode().gid(), gid);
        ASSERT_EQ(getResponse.inode().mode(), mode);
        ASSERT_EQ(getResponse.inode().type(), type);
        ASSERT_TRUE(CompareInode(createResponse.inode(), getResponse.inode()));
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    GetInodeRequest getRequest2;
    GetInodeResponse getResponse2;
    getRequest2.set_poolid(poolId);
    getRequest2.set_copysetid(copysetId);
    getRequest2.set_partitionid(partitionId);
    getRequest2.set_fsid(fsId);
    getRequest2.set_inodeid(createResponse.inode().inodeid() + 100);
    stub.GetInode(&cntl, &getRequest2, &getResponse2, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse2.statuscode(), MetaStatusCode::NOT_FOUND);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // update inode
    // no param need update
    cntl.Reset();
    UpdateInodeRequest updateRequest;
    UpdateInodeResponse updateResponse;
    updateRequest.set_poolid(poolId);
    updateRequest.set_copysetid(copysetId);
    updateRequest.set_partitionid(partitionId);
    updateRequest.set_fsid(fsId);
    updateRequest.set_inodeid(createResponse.inode().inodeid());
    stub.UpdateInode(&cntl, &updateRequest, &updateResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(updateResponse.statuscode(), MetaStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    GetInodeRequest getRequest3;
    GetInodeResponse getResponse3;
    getRequest3.set_poolid(poolId);
    getRequest3.set_copysetid(copysetId);
    getRequest3.set_partitionid(partitionId);
    getRequest3.set_fsid(fsId);
    getRequest3.set_inodeid(createResponse.inode().inodeid());
    stub.GetInode(&cntl, &getRequest3, &getResponse3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse3.statuscode(), MetaStatusCode::OK);
        ASSERT_TRUE(CompareInode(createResponse.inode(), getResponse3.inode()));
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    UpdateInodeRequest updateRequest2;
    UpdateInodeResponse updateResponse2;
    updateRequest2.set_poolid(poolId);
    updateRequest2.set_copysetid(copysetId);
    updateRequest2.set_partitionid(partitionId);
    updateRequest2.set_fsid(fsId);
    updateRequest2.set_inodeid(createResponse.inode().inodeid());
    updateRequest2.set_length(length + 1);
    stub.UpdateInode(&cntl, &updateRequest2, &updateResponse2, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(updateResponse2.statuscode(), MetaStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    GetInodeRequest getRequest4;
    GetInodeResponse getResponse4;
    getRequest4.set_poolid(poolId);
    getRequest4.set_copysetid(copysetId);
    getRequest4.set_partitionid(partitionId);
    getRequest4.set_fsid(fsId);
    getRequest4.set_inodeid(createResponse.inode().inodeid());
    stub.GetInode(&cntl, &getRequest4, &getResponse4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse4.statuscode(), MetaStatusCode::OK);
        ASSERT_FALSE(
            CompareInode(createResponse.inode(), getResponse4.inode()));
        ASSERT_EQ(getResponse4.inode().length(), length + 1);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
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
    stub.UpdateInode(&cntl, &updateRequest3, &updateResponse3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(updateResponse3.statuscode(), MetaStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // UPDATE INODE VERSION
    cntl.Reset();
    UpdateInodeS3VersionRequest updateVersionRequest;
    UpdateInodeS3VersionResponse updateVersionResponse;
    updateVersionRequest.set_poolid(poolId);
    updateVersionRequest.set_copysetid(copysetId);
    updateVersionRequest.set_partitionid(partitionId);
    updateVersionRequest.set_fsid(fsId);
    updateVersionRequest.set_inodeid(createResponse2.inode().inodeid());
    stub.UpdateInodeS3Version(&cntl, &updateVersionRequest,
                              &updateVersionResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(updateVersionResponse.statuscode(), MetaStatusCode::OK);
        ASSERT_EQ(updateVersionResponse.version(), 1);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    updateVersionRequest.set_fsid(fsId);
    updateVersionRequest.set_inodeid(createResponse2.inode().inodeid());
    stub.UpdateInodeS3Version(&cntl, &updateVersionRequest,
                              &updateVersionResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(updateVersionResponse.statuscode(), MetaStatusCode::OK);
        ASSERT_EQ(updateVersionResponse.version(), 2);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    updateVersionRequest.set_fsid(fsId);
    updateVersionRequest.set_inodeid(createResponse.inode().inodeid());
    stub.UpdateInodeS3Version(&cntl, &updateVersionRequest,
                              &updateVersionResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(updateVersionResponse.statuscode(),
                  MetaStatusCode::PARAM_ERROR);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // DELETE INODE
    cntl.Reset();
    DeleteInodeRequest deleteRequest;
    DeleteInodeResponse deleteResponse;
    deleteRequest.set_poolid(poolId);
    deleteRequest.set_copysetid(copysetId);
    deleteRequest.set_partitionid(partitionId);
    deleteRequest.set_fsid(fsId);
    deleteRequest.set_inodeid(createResponse.inode().inodeid());
    stub.DeleteInode(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), MetaStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    stub.DeleteInode(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), MetaStatusCode::NOT_FOUND);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // stop rpc server
    server.Stop(10);
    server.Join();
}

TEST_F(MetaserverServiceTest, dentryTest) {
    brpc::Server server;
    // add metaserver service
    MetaServerServiceImpl metaserverService(inodeManager_, dentryManager_);
    ASSERT_EQ(
        server.AddService(&metaserverService, brpc::SERVER_DOESNT_OWN_SERVICE),
        0);

    // start rpc server
    brpc::ServerOptions option;
    ASSERT_EQ(server.Start(addr_.c_str(), &option), 0);

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    MetaServerService_Stub stub(&channel);

    brpc::Controller cntl;

    // test CreateInde
    CreateDentryRequest createRequest;
    CreateDentryResponse createResponse;

    uint32_t poolId = 2;
    uint32_t copysetId = 3;
    uint32_t partitionId = 1;
    uint64_t txId = 0;
    uint32_t fsId = 1;
    uint64_t inodeId = 2;
    uint64_t parentId = 3;
    std::string name = "dentry1";

    Dentry dentry1;
    dentry1.set_fsid(fsId);
    dentry1.set_inodeid(inodeId);
    dentry1.set_parentinodeid(parentId);
    dentry1.set_name(name);
    dentry1.set_txid(0);

    createRequest.set_poolid(poolId);
    createRequest.set_copysetid(copysetId);
    createRequest.set_partitionid(partitionId);
    createRequest.mutable_dentry()->CopyFrom(dentry1);

    stub.CreateDentry(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    stub.CreateDentry(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::DENTRY_EXIST);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    Dentry dentry2;
    dentry2.set_fsid(fsId);
    dentry2.set_inodeid(inodeId + 1);
    dentry2.set_parentinodeid(parentId);
    dentry2.set_name("dentry2");
    dentry2.set_txid(0);
    createRequest.mutable_dentry()->CopyFrom(dentry2);

    cntl.Reset();
    stub.CreateDentry(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    Dentry dentry3;
    dentry3.set_fsid(fsId);
    dentry3.set_inodeid(inodeId + 2);
    dentry3.set_parentinodeid(parentId);
    dentry3.set_name("dentry3");
    dentry3.set_txid(0);
    createRequest.mutable_dentry()->CopyFrom(dentry3);

    cntl.Reset();
    stub.CreateDentry(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), MetaStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // TEST GET DETNRY
    cntl.Reset();
    GetDentryRequest getRequest;
    GetDentryResponse getResponse;
    getRequest.set_poolid(poolId);
    getRequest.set_copysetid(copysetId);
    getRequest.set_partitionid(partitionId);
    getRequest.set_fsid(fsId);
    getRequest.set_parentinodeid(parentId);
    getRequest.set_name(name);
    getRequest.set_txid(0);
    stub.GetDentry(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), MetaStatusCode::OK);
        ASSERT_TRUE(getResponse.has_dentry());
        ASSERT_TRUE(CompareDentry(dentry1, getResponse.dentry()));
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    getRequest.set_fsid(fsId + 1);
    getRequest.set_parentinodeid(parentId);
    getRequest.set_name(name);
    getRequest.set_txid(0);
    stub.GetDentry(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getResponse.statuscode(), MetaStatusCode::NOT_FOUND);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // TEST LIST DENTRY
    cntl.Reset();
    ListDentryRequest listRequest;
    ListDentryResponse listResponse;
    listRequest.set_poolid(poolId);
    listRequest.set_copysetid(copysetId);
    listRequest.set_partitionid(partitionId);
    listRequest.set_fsid(fsId);
    listRequest.set_dirinodeid(parentId);
    listRequest.set_txid(0);

    stub.ListDentry(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), MetaStatusCode::OK);
        ASSERT_EQ(listResponse.dentrys_size(), 3);
        ASSERT_TRUE(CompareDentry(listResponse.dentrys(0), dentry1));
        ASSERT_TRUE(CompareDentry(listResponse.dentrys(1), dentry2));
        ASSERT_TRUE(CompareDentry(listResponse.dentrys(2), dentry3));
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    listRequest.set_fsid(fsId);
    listRequest.set_dirinodeid(parentId);
    listRequest.set_txid(0);
    listRequest.set_last("dentry1");
    listRequest.set_count(100);

    stub.ListDentry(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), MetaStatusCode::OK);
        ASSERT_EQ(listResponse.dentrys_size(), 2);
        ASSERT_TRUE(CompareDentry(listResponse.dentrys(0), dentry2));
        ASSERT_TRUE(CompareDentry(listResponse.dentrys(1), dentry3));
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    listRequest.set_fsid(fsId);
    listRequest.set_dirinodeid(parentId);
    listRequest.set_txid(0);
    listRequest.clear_last();
    listRequest.set_count(1);

    stub.ListDentry(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), MetaStatusCode::OK);
        ASSERT_EQ(listResponse.dentrys_size(), 1);
        ASSERT_TRUE(CompareDentry(listResponse.dentrys(0), dentry1));
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // test delete
    cntl.Reset();
    DeleteDentryRequest deleteRequest;
    DeleteDentryResponse deleteResponse;
    deleteRequest.set_poolid(poolId);
    deleteRequest.set_copysetid(copysetId);
    deleteRequest.set_partitionid(partitionId);
    deleteRequest.set_fsid(fsId);
    deleteRequest.set_parentinodeid(parentId);
    deleteRequest.set_name("dentry2");
    deleteRequest.set_txid(0);

    stub.DeleteDentry(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), MetaStatusCode::OK);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    listRequest.set_fsid(fsId);
    listRequest.set_dirinodeid(parentId);
    listRequest.set_txid(0);
    listRequest.clear_last();
    listRequest.clear_count();
    stub.ListDentry(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), MetaStatusCode::OK);
        ASSERT_EQ(listResponse.dentrys_size(), 2);
        ASSERT_TRUE(CompareDentry(listResponse.dentrys(0), dentry1));
        ASSERT_TRUE(CompareDentry(listResponse.dentrys(1), dentry3));
        // ASSERT_TRUE(CompareDentry(listResponse.dentrys(2), dentry3));
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    stub.DeleteDentry(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), MetaStatusCode::NOT_FOUND);
    } else {
        LOG(ERROR) << "error = " << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // stop rpc server
    server.Stop(10);
    server.Join();
}
}  // namespace metaserver
}  // namespace curvefs
