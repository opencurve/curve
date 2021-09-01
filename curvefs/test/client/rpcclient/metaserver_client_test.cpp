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
 * Created Date: Mon Sept 5 2021
 * Author: lixiaocui
 */


#include <brpc/server.h>
#include <gtest/gtest.h>
#include <google/protobuf/util/message_differencer.h>

#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/test/client/rpcclient/mock_metacache.h"
#include "curvefs/test/client/rpcclient/mock_metaserver_service.h"
#include "src/client/mds_client.h"
#include "curvefs/src/client/rpcclient/channel_manager.h"
#include "curvefs/src/client/common/common.h"

namespace curvefs {
namespace client {
namespace rpcclient {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

using ::curvefs::metaserver::Dentry;
using ::curvefs::metaserver::MetaStatusCode;

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void SetRpcService(google::protobuf::RpcController *cntl_base,
                   const RpcRequestType *request, RpcResponseType *response,
                   google::protobuf::Closure *done) {
    if (RpcFailed) {
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    done->Run();
}

class MetaServerClientImplTest : public testing::Test {
 protected:
    void SetUp() override {
        // init metacache
        opt_.maxRPCTimeoutMS = 1000;
        opt_.maxRetrySleepIntervalUS = 500 * 1000;
        opt_.minRetryTimesForceTimeoutBackoff = 2;
        opt_.maxRetryTimesBeforeConsiderSuspend = 5;
        mockMetacache_ = std::make_shared<MockMetaCache>();
        auto channelManager_ = std::make_shared<ChannelManager<MetaserverID>>();
        metaserverCli_.Init(opt_, mockMetacache_, channelManager_);

        // start metaserver service
        ASSERT_EQ(0, server_.AddService(&mockMetaServerService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));

        target_.groupID = std::move(CopysetGroupID(1, 100));
        target_.metaServerID = 1;
        target_.partitionID = 200;
        butil::str2endpoint(addr_.c_str(), &target_.endPoint);
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    MetaServerClientImpl metaserverCli_;

    ExcutorOpt opt_;
    std::shared_ptr<MockMetaCache> mockMetacache_;

    MockMetaServerService mockMetaServerService_;
    std::string addr_ = "127.0.0.1:5200";
    brpc::Server server_;
    CopysetTarget target_;
};

TEST_F(MetaServerClientImplTest, test_GetDentry) {
    // in
    uint32_t fsID = 1;
    uint32_t inodeID = 1;
    std::string name = "/test";

    // out
    Dentry out;
    uint64_t applyIndex = 10;
    uint64_t txID = 1;

    // set response
    curvefs::metaserver::GetDentryResponse response;
    auto *d = new curvefs::metaserver::Dentry();
    d->set_fsid(fsID);
    d->set_inodeid(inodeID);
    d->set_parentinodeid(1);
    d->set_name(name);
    d->set_txid(txID);
    response.set_allocated_dentry(d);
    response.set_appliedindex(100);

    // test0: test rpc error
    EXPECT_CALL(mockMetaServerService_, GetDentry(_, _, _, _))
        .WillRepeatedly(
            Invoke(SetRpcService<GetDentryRequest, GetDentryResponse, true>));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));

    MetaStatusCode status = metaserverCli_.GetDentry(fsID, inodeID, name, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test1: get dentry ok
    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaServerService_, GetDentry(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(SetRpcService<GetDentryRequest, GetDentryResponse>)));

    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(target_), SetArgPointee<3>(applyIndex),
                        Return(true)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));

    status = metaserverCli_.GetDentry(fsID, inodeID, name, &out);
    ASSERT_EQ(MetaStatusCode::OK, status);
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(out, *d))
        << "out:\n"
        << out.ShortDebugString() << "expect:\n"
        << d->ShortDebugString();

    // test2: get dentry get target fail
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(Return(false));
    status = metaserverCli_.GetDentry(fsID, inodeID, name, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test3: get dentry over load and fail retry ok
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));
    curvefs::metaserver::GetDentryResponse responsefail;
    responsefail.set_statuscode(MetaStatusCode::OVERLOAD);
    EXPECT_CALL(mockMetaServerService_, GetDentry(_, _, _, _))
        .Times(2)
        .WillOnce(
            DoAll(SetArgPointee<2>(responsefail),
                  Invoke(SetRpcService<GetDentryRequest, GetDentryResponse>)))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(SetRpcService<GetDentryRequest, GetDentryResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));

    status = metaserverCli_.GetDentry(fsID, inodeID, name, &out);
    ASSERT_EQ(MetaStatusCode::OK, status);

    // test4: test response do not have applyindex
    response.set_statuscode(MetaStatusCode::OK);
    response.clear_appliedindex();
    EXPECT_CALL(mockMetaServerService_, GetDentry(_, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(response),
                  Invoke(SetRpcService<GetDentryRequest, GetDentryResponse>)));

    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));

    status = metaserverCli_.GetDentry(fsID, inodeID, name, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test5: test response do not have dentry
    response.set_appliedindex(100);
    response.clear_dentry();
    status = metaserverCli_.GetDentry(fsID, inodeID, name, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test6: do not have both dentry and appliedindex
    response.clear_dentry();
    response.clear_appliedindex();

    status = metaserverCli_.GetDentry(fsID, inodeID, name, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);
}

TEST_F(MetaServerClientImplTest, test_ListDentry) {
    // in
    uint32_t fsID = 1;
    uint32_t inodeID = 1;
    std::string last = "test1";
    uint32_t count = 10;
    // out
    std::list<Dentry> out;
    uint64_t applyIndex = 10;
    uint64_t txID = 10;

    curvefs::metaserver::ListDentryResponse response;
    auto *d = response.add_dentrys();
    d->set_fsid(fsID);
    d->set_inodeid(inodeID);
    d->set_parentinodeid(1);
    d->set_name("test11");
    d->set_txid(txID);

    // test0: set rpc error
    EXPECT_CALL(mockMetaServerService_, ListDentry(_, _, _, _))
        .WillRepeatedly(
            Invoke(SetRpcService<ListDentryRequest, ListDentryResponse, true>));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));

    MetaStatusCode status =
        metaserverCli_.ListDentry(fsID, inodeID, last, count, &out);

    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test1: list dentry ok
    response.set_statuscode(MetaStatusCode::OK);
    response.set_appliedindex(10);
    EXPECT_CALL(mockMetaServerService_, ListDentry(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<ListDentryRequest, ListDentryResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(target_), SetArgPointee<3>(applyIndex),
                        Return(true)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));

    status = metaserverCli_.ListDentry(fsID, inodeID, last, count, &out);
    ASSERT_EQ(MetaStatusCode::OK, status);
    ASSERT_EQ(1, out.size());
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(*out.begin(), *d))
        << "out:\n"
        << out.begin()->ShortDebugString() << "expect:\n"
        << d->ShortDebugString();

    // test2: list dentry redirect
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));
    curvefs::metaserver::ListDentryResponse responsefail;
    responsefail.set_statuscode(MetaStatusCode::REDIRECTED);
    EXPECT_CALL(mockMetaServerService_, ListDentry(_, _, _, _))
        .Times(2)
        .WillOnce(
            DoAll(SetArgPointee<2>(responsefail),
                  Invoke(SetRpcService<ListDentryRequest, ListDentryResponse>)))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<ListDentryRequest, ListDentryResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));
    status = metaserverCli_.ListDentry(fsID, inodeID, last, count, &out);
    ASSERT_EQ(MetaStatusCode::OK, status);

    // test3: test response do not have applyindex
    response.clear_appliedindex();
    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaServerService_, ListDentry(_, _, _, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<ListDentryRequest, ListDentryResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));

    status = metaserverCli_.ListDentry(fsID, inodeID, last, count, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test4: test response do not have dentrys
    response.set_appliedindex(100);
    response.clear_dentrys();
    status = metaserverCli_.ListDentry(fsID, inodeID, last, count, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test5: do not have both dentrys and appliedindex
    response.clear_dentrys();
    response.clear_appliedindex();
    status = metaserverCli_.ListDentry(fsID, inodeID, last, count, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);
}

TEST_F(MetaServerClientImplTest, test_CreateDentry) {
    // in
    Dentry d;
    d.set_fsid(1);
    d.set_inodeid(2);
    d.set_parentinodeid(1);
    d.set_name("test11");
    d.set_txid(10);

    // out
    MetaserverID metaServerID = 1;
    butil::EndPoint target;
    butil::str2endpoint(addr_.c_str(), &target);
    LogicPoolID poolID = 1;
    CopysetID copysetID = 100;
    uint32_t partitionID = 200;
    uint64_t applyIndex = 10;

    curvefs::metaserver::CreateDentryResponse response;

    // test0: rpc error
    EXPECT_CALL(mockMetaServerService_, CreateDentry(_, _, _, _))
        .WillRepeatedly(Invoke(
            SetRpcService<CreateDentryRequest, CreateDentryResponse, true>));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));

    MetaStatusCode status = metaserverCli_.CreateDentry(d);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test1: create dentry ok
    response.set_statuscode(MetaStatusCode::OK);
    response.set_appliedindex(10);
    EXPECT_CALL(mockMetaServerService_, CreateDentry(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<CreateDentryRequest, CreateDentryResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(target_), SetArgPointee<3>(applyIndex),
                        Return(true)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));

    status = metaserverCli_.CreateDentry(d);
    ASSERT_EQ(MetaStatusCode::OK, status);

    // test2: create dentry copset not exist
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));
    curvefs::metaserver::CreateDentryResponse responsefail;
    responsefail.set_statuscode(MetaStatusCode::COPYSET_NOTEXIST);
    EXPECT_CALL(mockMetaServerService_, CreateDentry(_, _, _, _))
        .Times(2)
        .WillOnce(DoAll(
            SetArgPointee<2>(responsefail),
            Invoke(SetRpcService<CreateDentryRequest, CreateDentryResponse>)))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<CreateDentryRequest, CreateDentryResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));

    status = metaserverCli_.CreateDentry(d);
    ASSERT_EQ(MetaStatusCode::OK, status);


    // test3: test response has applyindex
    response.clear_appliedindex();
    EXPECT_CALL(mockMetaServerService_, CreateDentry(_, _, _, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<CreateDentryRequest, CreateDentryResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));

    status = metaserverCli_.CreateDentry(d);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);
}

TEST_F(MetaServerClientImplTest, test_DeleteDentry) {
    // in
    uint32_t fsid = 1;
    uint64_t inodeid = 2;
    std::string name = "test";

    // out
    MetaserverID metaServerID = 1;
    butil::EndPoint target;
    butil::str2endpoint(addr_.c_str(), &target);
    LogicPoolID poolID = 1;
    CopysetID copysetID = 100;
    uint32_t partitionID = 200;
    uint64_t applyIndex = 10;

    curvefs::metaserver::DeleteDentryResponse response;

    // test1: delete dentry ok
    response.set_statuscode(MetaStatusCode::OK);
    response.set_appliedindex(10);
    EXPECT_CALL(mockMetaServerService_, DeleteDentry(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<DeleteDentryRequest, DeleteDentryResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(target_), SetArgPointee<3>(applyIndex),
                        Return(true)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));

    MetaStatusCode status = metaserverCli_.DeleteDentry(fsid, inodeid, name);
    ASSERT_EQ(MetaStatusCode::OK, status);

    // test2: rpc error
    EXPECT_CALL(mockMetaServerService_, DeleteDentry(_, _, _, _))
        .WillRepeatedly(Invoke(
            SetRpcService<DeleteDentryRequest, DeleteDentryResponse, true>));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));

    status = metaserverCli_.DeleteDentry(fsid, inodeid, name);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test3: delete response with unknown error
    response.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaServerService_, DeleteDentry(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<DeleteDentryRequest, DeleteDentryResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));
    status = metaserverCli_.DeleteDentry(fsid, inodeid, name);
    ASSERT_EQ(MetaStatusCode::UNKNOWN_ERROR, status);

    // test4: test response has applyindex
    response.set_statuscode(MetaStatusCode::OK);
    response.clear_appliedindex();
    EXPECT_CALL(mockMetaServerService_, DeleteDentry(_, _, _, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<DeleteDentryRequest, DeleteDentryResponse>)));
    EXPECT_CALL(mockMetaServerService_, DeleteDentry(_, _, _, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<DeleteDentryRequest, DeleteDentryResponse>)));

    status = metaserverCli_.DeleteDentry(fsid, inodeid, name);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);
}

TEST_F(MetaServerClientImplTest, test_GetInode) {
    // in
    uint32_t fsid = 1;
    uint64_t inodeid = 2;

    // out
    MetaserverID metaServerID = 1;
    butil::EndPoint target;
    butil::str2endpoint(addr_.c_str(), &target);
    LogicPoolID poolID = 1;
    CopysetID copysetID = 100;
    uint32_t partitionID = 200;
    uint64_t applyIndex = 10;
    curvefs::metaserver::Inode out;

    curvefs::metaserver::GetInodeResponse response;

    // test0: rpc error
    EXPECT_CALL(mockMetaServerService_, GetInode(_, _, _, _))
        .WillRepeatedly(
            Invoke(SetRpcService<GetInodeRequest, GetInodeResponse, true>));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));

    MetaStatusCode status = metaserverCli_.GetInode(fsid, inodeid, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test1: get inode ok
    response.set_statuscode(MetaStatusCode::OK);
    response.set_appliedindex(10);
    EXPECT_CALL(mockMetaServerService_, GetInode(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(SetRpcService<GetInodeRequest, GetInodeResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));

    status = metaserverCli_.GetInode(fsid, inodeid, &out);
    ASSERT_EQ(MetaStatusCode::OK, status);

    // test2: get inode with not found error
    response.set_statuscode(MetaStatusCode::NOT_FOUND);
    EXPECT_CALL(mockMetaServerService_, GetInode(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(SetRpcService<GetInodeRequest, GetInodeResponse>)));
    status = metaserverCli_.GetInode(fsid, inodeid, &out);
    ASSERT_EQ(MetaStatusCode::NOT_FOUND, status);

    // test3: test response do not have applyindex
    response.set_statuscode(MetaStatusCode::OK);
    response.clear_appliedindex();
    EXPECT_CALL(mockMetaServerService_, GetInode(_, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(response),
                  Invoke(SetRpcService<GetInodeRequest, GetInodeResponse>)));

    status = metaserverCli_.GetInode(fsid, inodeid, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test4: test response do not have inode
    response.set_appliedindex(10);
    response.clear_inode();
    status = metaserverCli_.GetInode(fsid, inodeid, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test5: do not have both dentrys and appliedindex
    response.clear_inode();
    response.clear_appliedindex();

    status = metaserverCli_.GetInode(fsid, inodeid, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);
}

TEST_F(MetaServerClientImplTest, test_UpdateInode) {
    // in
    curvefs::metaserver::Inode inode;
    inode.set_inodeid(1);
    inode.set_fsid(2);
    inode.set_length(10);
    inode.set_ctime(1623835517);
    inode.set_mtime(1623835517);
    inode.set_atime(1623835517);
    inode.set_uid(1);
    inode.set_gid(1);
    inode.set_mode(1);
    inode.set_nlink(1);
    inode.set_type(curvefs::metaserver::FsFileType::TYPE_FILE);
    inode.set_symlink("test9");

    // out
    MetaserverID metaServerID = 1;
    butil::EndPoint target;
    butil::str2endpoint(addr_.c_str(), &target);
    LogicPoolID poolID = 1;
    CopysetID copysetID = 100;
    uint32_t partitionID = 200;
    uint64_t applyIndex = 10;
    curvefs::metaserver::Inode out;

    curvefs::metaserver::UpdateInodeResponse response;

    // test0: rpc error
    EXPECT_CALL(mockMetaServerService_, UpdateInode(_, _, _, _))
        .WillRepeatedly(Invoke(
            SetRpcService<UpdateInodeRequest, UpdateInodeResponse, true>));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));

    MetaStatusCode status = metaserverCli_.UpdateInode(inode);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test1: update inode ok
    response.set_statuscode(curvefs::metaserver::OK);
    response.set_appliedindex(10);
    EXPECT_CALL(mockMetaServerService_, UpdateInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<UpdateInodeRequest, UpdateInodeResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));
    status = metaserverCli_.UpdateInode(inode);
    ASSERT_EQ(MetaStatusCode::OK, status);

    // test2: update inode with overload
    response.set_statuscode(curvefs::metaserver::OVERLOAD);
    EXPECT_CALL(mockMetaServerService_, UpdateInode(_, _, _, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<UpdateInodeRequest, UpdateInodeResponse>)));
    status = metaserverCli_.UpdateInode(inode);
    ASSERT_EQ(MetaStatusCode::OVERLOAD, status);

    // test3: response has no applyindex
    response.set_statuscode(curvefs::metaserver::OK);
    response.clear_appliedindex();
    EXPECT_CALL(mockMetaServerService_, UpdateInode(_, _, _, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<UpdateInodeRequest, UpdateInodeResponse>)));

    status = metaserverCli_.UpdateInode(inode);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test4: get target always fail
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(Return(false));
    status = metaserverCli_.UpdateInode(inode);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);
}

TEST_F(MetaServerClientImplTest, test_CreateInode) {
    // in
    InodeParam inode;
    inode.fsId = 2;
    inode.length = 10;
    inode.uid = 1;
    inode.gid = 1;
    inode.mode = 1;
    inode.type = curvefs::metaserver::FsFileType::TYPE_FILE;
    inode.symlink = "test9";

    // out
    MetaserverID metaServerID = 1;
    butil::EndPoint target;
    butil::str2endpoint(addr_.c_str(), &target);
    LogicPoolID poolID = 1;
    CopysetID copysetID = 100;
    uint32_t partitionID = 200;
    uint64_t applyIndex = 10;
    curvefs::metaserver::Inode out;

    curvefs::metaserver::CreateInodeResponse response;

    // test0: rpc error
    EXPECT_CALL(mockMetaServerService_, CreateInode(_, _, _, _))
        .WillRepeatedly(Invoke(
            SetRpcService<CreateInodeRequest, CreateInodeResponse, true>));
    EXPECT_CALL(*mockMetacache_.get(), SelectTarget(_, _, _))
        .WillRepeatedly(Return(true));
    MetaStatusCode status = metaserverCli_.CreateInode(inode, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test1: create inode ok
    response.set_statuscode(MetaStatusCode::OK);
    response.set_appliedindex(10);
    EXPECT_CALL(mockMetaServerService_, CreateInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<CreateInodeRequest, CreateInodeResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), SelectTarget(_, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(target_), SetArgPointee<2>(applyIndex),
                        Return(true)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));
    status = metaserverCli_.CreateInode(inode, &out);
    ASSERT_EQ(MetaStatusCode::OK, status);

    // test2: create inode with inode exist
    response.set_statuscode(MetaStatusCode::INODE_EXIST);
    EXPECT_CALL(mockMetaServerService_, CreateInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<CreateInodeRequest, CreateInodeResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), SelectTarget(_, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(target_),
                              SetArgPointee<2>(applyIndex), Return(true)));

    status = metaserverCli_.CreateInode(inode, &out);
    ASSERT_EQ(MetaStatusCode::INODE_EXIST, status);

    // test3: response do not have applyindex
    response.clear_appliedindex();
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockMetaServerService_, CreateInode(_, _, _, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<CreateInodeRequest, CreateInodeResponse>)));

    status = metaserverCli_.CreateInode(inode, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test4: response do not have inode
    response.set_appliedindex(10);
    response.clear_inode();

    status = metaserverCli_.CreateInode(inode, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test5: do not have both inode and appliedindex
    response.clear_inode();
    response.clear_appliedindex();

    status = metaserverCli_.CreateInode(inode, &out);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);
}

TEST_F(MetaServerClientImplTest, test_DeleteInode) {
    // in
    uint32_t fsId = 2;
    uint64_t inodeid = 1;

    // out
    MetaserverID metaServerID = 1;
    butil::EndPoint target;
    butil::str2endpoint(addr_.c_str(), &target);
    LogicPoolID poolID = 1;
    CopysetID copysetID = 100;
    uint32_t partitionID = 200;
    uint64_t applyIndex = 10;

    curvefs::metaserver::DeleteInodeResponse response;

    // test0: rpc error
    EXPECT_CALL(mockMetaServerService_, DeleteInode(_, _, _, _))
        .WillRepeatedly(Invoke(
            SetRpcService<DeleteInodeRequest, DeleteInodeResponse, true>));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));
    MetaStatusCode status = metaserverCli_.DeleteInode(fsId, inodeid);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);

    // test1: delete inode ok
    response.set_statuscode(MetaStatusCode::OK);
    response.set_appliedindex(10);
    EXPECT_CALL(mockMetaServerService_, DeleteInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<DeleteInodeRequest, DeleteInodeResponse>)));
    EXPECT_CALL(*mockMetacache_.get(), GetTarget(_, _, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(target_),
                              SetArgPointee<3>(applyIndex), Return(true)));
    EXPECT_CALL(*mockMetacache_.get(), UpdateApplyIndex(_, _));
    status = metaserverCli_.DeleteInode(fsId, inodeid);
    ASSERT_EQ(MetaStatusCode::OK, status);

    // test2: delete inode with not found error
    response.set_statuscode(MetaStatusCode::NOT_FOUND);
    EXPECT_CALL(mockMetaServerService_, DeleteInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<DeleteInodeRequest, DeleteInodeResponse>)));
    status = metaserverCli_.DeleteInode(fsId, inodeid);
    ASSERT_EQ(MetaStatusCode::NOT_FOUND, status);

    // test3: response do not have apply index
    response.clear_appliedindex();
    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaServerService_, DeleteInode(_, _, _, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<2>(response),
            Invoke(SetRpcService<DeleteInodeRequest, DeleteInodeResponse>)));

    status = metaserverCli_.DeleteInode(fsId, inodeid);
    ASSERT_EQ(MetaStatusCode::RPC_ERROR, status);
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
