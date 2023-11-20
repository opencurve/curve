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
 * Date: Wed 25 Aug 2021 10:25:55 PM CST
 * Author: wuhanqing
 */

#include <brpc/server.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "curvefs/proto/common.pb.h"
#include "curvefs/src/mds/fs_manager.h"
#include "curvefs/src/mds/metaserverclient/metaserver_client.h"
#include "curvefs/test/mds/mock/mock_fs_stroage.h"
#include "curvefs/test/mds/mock/mock_metaserver.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/mock/mock_cli2.h"
#include "test/common/mock_s3_adapter.h"

#include "curvefs/test/mds/mock/mock_space_manager.h"

using ::curvefs::mds::topology::TopologyManager;
using ::curvefs::mds::topology::MockTopologyManager;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockStorage;
using ::curvefs::mds::topology::TopologyIdGenerator;
using ::curvefs::mds::topology::DefaultIdGenerator;
using ::curvefs::mds::topology::TopologyTokenGenerator;
using ::curvefs::mds::topology::DefaultTokenGenerator;
using ::curvefs::mds::topology::MockEtcdClient;
using ::curvefs::mds::topology::MockTopologyManager;
using ::curvefs::mds::topology::TopologyStorageCodec;
using ::curvefs::mds::topology::TopologyStorageEtcd;
using ::curvefs::mds::topology::TopologyImpl;
using ::curvefs::mds::topology::CreatePartitionRequest;
using ::curvefs::mds::topology::CreatePartitionResponse;
using ::curvefs::mds::topology::TopoStatusCode;
using ::curvefs::metaserver::copyset::MockCliService2;
using ::curvefs::metaserver::copyset::GetLeaderResponse2;
using ::curve::common::MockS3Adapter;

namespace curvefs {
namespace mds {

using ::curvefs::mds::space::MockSpaceManager;
using ::curvefs::metaserver::MockMetaserverService;

const char* kFsManagerTest2ServerAddress = "0.0.0.0:22000";

using ::testing::_;
using ::testing::Invoke;
using ::testing::Matcher;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::curvefs::common::S3Info;

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void RpcService(google::protobuf::RpcController *cntl_base,
                const RpcRequestType *request, RpcResponseType *response,
                google::protobuf::Closure *done) {
    if (RpcFailed) {
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    done->Run();
}

class FsManagerTest2 : public testing::Test {
 protected:
    void SetUp() override {
        storage_ = std::make_shared<MockFsStorage>();

        MetaserverOptions metaSvrOpts;
        metaSvrOpts.metaserverAddr = kFsManagerTest2ServerAddress;
        metaSvrOpts.rpcTimeoutMs = 1000;
        metaServerClient_ = std::make_shared<MetaserverClient>(metaSvrOpts);
        // init mock topology manager
        std::shared_ptr<TopologyIdGenerator> idGenerator_ =
            std::make_shared<DefaultIdGenerator>();
        std::shared_ptr<TopologyTokenGenerator> tokenGenerator_ =
            std::make_shared<DefaultTokenGenerator>();

        auto etcdClient_ = std::make_shared<MockEtcdClient>();
        auto codec = std::make_shared<TopologyStorageCodec>();
        auto topoStorage_ =
            std::make_shared<TopologyStorageEtcd>(etcdClient_, codec);
        topoManager_ = std::make_shared<MockTopologyManager>(
                            std::make_shared<TopologyImpl>(idGenerator_,
                            tokenGenerator_, topoStorage_), metaServerClient_);
        s3Adapter_ = std::make_shared<MockS3Adapter>();

        spaceManager_ = std::make_shared<MockSpaceManager>();

        // init fsmanager
        FsManagerOption fsManagerOption;
        fsManagerOption.backEndThreadRunInterSec = 1;
        fsManager_ = std::make_shared<FsManager>(
            storage_, spaceManager_, metaServerClient_, topoManager_,
            s3Adapter_, nullptr, fsManagerOption);

        // spaceService_ = std::make_shared<MockSpaceService>();
        metaserverService_ = std::make_shared<MockMetaserverService>();

        ASSERT_EQ(0, server_.AddService(metaserverService_.get(),
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockCliService2_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(kFsManagerTest2ServerAddress, nullptr));

        EXPECT_CALL(*storage_, Init()).WillOnce(Return(true));

        ASSERT_TRUE(fsManager_->Init());
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();

        EXPECT_CALL(*storage_, Uninit()).Times(1);

        fsManager_->Uninit();
    }

 protected:
    std::shared_ptr<MockFsStorage> storage_;
    std::shared_ptr<MetaserverClient> metaServerClient_;
    std::shared_ptr<MockSpaceManager> spaceManager_;

    std::shared_ptr<MockTopologyManager> topoManager_;
    std::shared_ptr<MockMetaserverService> metaserverService_;

    MockCliService2 mockCliService2_;
    std::shared_ptr<FsManager> fsManager_;
    brpc::Server server_;
    std::shared_ptr<MockS3Adapter> s3Adapter_;
};

TEST_F(FsManagerTest2, CreateFoundConflictFsNameAndNotIdenticalToPreviousOne) {
    std::string fsname = "hello";
    FSType type = FSType::TYPE_S3;
    uint64_t blocksize = 4 * 1024;
    FsDetail detail;
    auto* s3Info = detail.mutable_s3info();
    s3Info->set_ak("hello");
    s3Info->set_sk("world");
    s3Info->set_endpoint("hello.world.com");
    s3Info->set_bucketname("hello");
    s3Info->set_blocksize(4 * 1024);
    s3Info->set_chunksize(16 * 1024 * 1024);

    CreateFsRequest req;
    req.set_fsname(fsname);
    req.set_blocksize(blocksize);
    req.set_fstype(type);
    req.set_allocated_fsdetail(new FsDetail(detail));
    req.set_enablesumindir(false);
    req.set_owner("test");
    req.set_capacity((uint64_t)100 * 1024 * 1024 * 1024);

    // fsstatus is not NEW
    {
        FsInfo fsinfo;
        fsinfo.set_status(FsStatus::INITED);
        fsinfo.set_fsname(fsname);
        fsinfo.set_blocksize(4 * 1024);

        FsInfoWrapper wrapper(fsinfo);

        EXPECT_CALL(*storage_, Exist(Matcher<const std::string&>(_)))
            .WillOnce(Return(true));

        EXPECT_CALL(*storage_, Get(Matcher<const std::string&>(_), _))
            .WillOnce(
                DoAll(SetArgPointee<1>(wrapper), Return(FSStatusCode::OK)));

        EXPECT_EQ(FSStatusCode::FS_EXIST, fsManager_->CreateFs(&req, nullptr));
    }

    // fstype is different
    {
        FsInfo fsinfo;
        fsinfo.set_status(FsStatus::NEW);
        fsinfo.set_fsname(fsname);
        fsinfo.set_blocksize(4 * 1024);
        fsinfo.set_fstype(FSType::TYPE_VOLUME);

        FsInfoWrapper wrapper(fsinfo);

        EXPECT_CALL(*storage_, Exist(Matcher<const std::string&>(_)))
            .WillOnce(Return(true));

        EXPECT_CALL(*storage_, Get(Matcher<const std::string&>(_), _))
            .WillOnce(
                DoAll(SetArgPointee<1>(wrapper), Return(FSStatusCode::OK)));

        EXPECT_EQ(FSStatusCode::FS_EXIST, fsManager_->CreateFs(&req, nullptr));
    }

    // fsdetail is different
    {
        FsInfo fsinfo;
        fsinfo.set_status(FsStatus::NEW);
        fsinfo.set_fsname(fsname);
        fsinfo.set_blocksize(4 * 1024);
        fsinfo.set_fstype(FSType::TYPE_S3);

        auto s3Info2 = *s3Info;
        s3Info2.set_bucketname("different");
        fsinfo.mutable_detail()->set_allocated_s3info(
            new curvefs::common::S3Info(s3Info2));

        FsInfoWrapper wrapper(fsinfo);

        EXPECT_CALL(*storage_, Exist(Matcher<const std::string&>(_)))
            .WillOnce(Return(true));

        EXPECT_CALL(*storage_, Get(Matcher<const std::string&>(_), _))
            .WillOnce(
                DoAll(SetArgPointee<1>(wrapper), Return(FSStatusCode::OK)));

        EXPECT_EQ(FSStatusCode::FS_EXIST, fsManager_->CreateFs(&req, nullptr));
    }

    // prefix is different
    {
        FsInfo fsinfo;
        fsinfo.set_status(FsStatus::NEW);
        fsinfo.set_fsname(fsname);
        fsinfo.set_blocksize(4 * 1024);
        fsinfo.set_fstype(FSType::TYPE_S3);

        auto s3Info2 = *s3Info;
        s3Info2.set_objectprefix(1);
        fsinfo.mutable_detail()->set_allocated_s3info(
            new curvefs::common::S3Info(s3Info2));

        FsInfoWrapper wrapper(fsinfo);

        EXPECT_CALL(*storage_, Exist(Matcher<const std::string&>(_)))
            .WillOnce(Return(true));

        EXPECT_CALL(*storage_, Get(Matcher<const std::string&>(_), _))
            .WillOnce(
                DoAll(SetArgPointee<1>(wrapper), Return(FSStatusCode::OK)));

        EXPECT_EQ(FSStatusCode::FS_EXIST, fsManager_->CreateFs(&req, nullptr));
    }
}

TEST_F(FsManagerTest2, CreateFoundUnCompleteOperation) {
    std::string fsname = "hello";
    FSType type = FSType::TYPE_S3;
    uint64_t blocksize = 4 * 1024;
    FsDetail detail;
    auto* s3Info = detail.mutable_s3info();
    s3Info->set_ak("hello");
    s3Info->set_sk("world");
    s3Info->set_endpoint("hello.world.com");
    s3Info->set_bucketname("hello");
    s3Info->set_blocksize(4 * 1024);
    s3Info->set_chunksize(16 * 1024 * 1024);

    FsInfo fsinfo;
    fsinfo.set_status(FsStatus::NEW);
    fsinfo.set_fsname(fsname);
    fsinfo.set_blocksize(4 * 1024);
    fsinfo.set_fstype(FSType::TYPE_S3);

    auto s3Info2 = *s3Info;
    fsinfo.mutable_detail()->set_allocated_s3info(
        new curvefs::common::S3Info(s3Info2));

    FsInfoWrapper wrapper(fsinfo);

    EXPECT_CALL(*storage_, Exist(Matcher<const std::string&>(_)))
        .WillOnce(Return(true));

    EXPECT_CALL(*storage_, Get(Matcher<const std::string&>(_), _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(wrapper),
                        Return(FSStatusCode::OK)));

    EXPECT_CALL(*storage_, NextFsId())
        .Times(0);

    EXPECT_CALL(*storage_, Insert(_))
        .Times(0);

    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    std::set<std::string> addrs;
    addrs.emplace(kFsManagerTest2ServerAddress);
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(addrs),
            Return(TopoStatusCode::TOPO_OK)));
    GetLeaderResponse2 getLeaderResponse;
    getLeaderResponse.mutable_leader()->set_address("0.0.0.0:22000:0");
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(DoAll(
        SetArgPointee<2>(getLeaderResponse),
        Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));

    EXPECT_CALL(*metaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(Invoke(
            [](::google::protobuf::RpcController* controller,
               const ::curvefs::metaserver::CreateRootInodeRequest* request,
               ::curvefs::metaserver::CreateRootInodeResponse* response,
               ::google::protobuf::Closure* done) {
                response->set_statuscode(metaserver::MetaStatusCode::OK);
                done->Run();
            }));

    EXPECT_CALL(*storage_, Update(_))
        .WillOnce(Return(FSStatusCode::OK));

    CreateFsRequest req;
    req.set_fsname(fsname);
    req.set_blocksize(blocksize);
    req.set_fstype(type);
    req.set_allocated_fsdetail(new FsDetail(detail));
    req.set_enablesumindir(false);
    req.set_owner("test");
    req.set_capacity((uint64_t)100 * 1024 * 1024 * 1024);
    FsInfo resultInfo;
    EXPECT_EQ(FSStatusCode::OK, fsManager_->CreateFs(&req, &resultInfo));

    EXPECT_EQ(FsStatus::INITED, resultInfo.status());
}

// TODO(huyao): create hybrid fs
TEST_F(FsManagerTest2, createHybridFs) {
    std::string fsname = "hello";
    FSType type = FSType::TYPE_HYBRID;
    uint64_t blocksize = 4 * 1024;
    FsDetail detail;
    auto* s3Info = detail.mutable_s3info();
    s3Info->set_ak("hello");
    s3Info->set_sk("world");
    s3Info->set_endpoint("hello.world.com");
    s3Info->set_bucketname("hello");
    s3Info->set_blocksize(4 * 1024);
    s3Info->set_chunksize(16 * 1024 * 1024);
    s3Info->set_objectprefix(0);

    FsInfo fsinfo;
    fsinfo.set_status(FsStatus::NEW);
    fsinfo.set_fsname(fsname);
    fsinfo.set_blocksize(4 * 1024);
    fsinfo.set_fstype(FSType::TYPE_HYBRID);

    auto s3Info2 = *s3Info;
    fsinfo.mutable_detail()->set_allocated_s3info(
        new curvefs::common::S3Info(s3Info2));

    FsInfoWrapper wrapper(fsinfo);

    EXPECT_CALL(*storage_, Exist(Matcher<const std::string&>(_)))
        .WillOnce(Return(true));

    EXPECT_CALL(*storage_, Get(Matcher<const std::string&>(_), _))
        .Times(2)
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(wrapper), Return(FSStatusCode::OK)));

    EXPECT_CALL(*storage_, NextFsId()).Times(0);

    EXPECT_CALL(*storage_, Insert(_)).Times(0);

    EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    std::set<std::string> addrs;
    addrs.emplace(kFsManagerTest2ServerAddress);
    EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    GetLeaderResponse2 getLeaderResponse;
    getLeaderResponse.mutable_leader()->set_address("0.0.0.0:22000:0");
    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(getLeaderResponse),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));

    EXPECT_CALL(*metaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(Invoke(
            [](::google::protobuf::RpcController* controller,
               const ::curvefs::metaserver::CreateRootInodeRequest* request,
               ::curvefs::metaserver::CreateRootInodeResponse* response,
               ::google::protobuf::Closure* done) {
                response->set_statuscode(metaserver::MetaStatusCode::OK);
                done->Run();
            }));

    EXPECT_CALL(*storage_, Update(_)).WillOnce(Return(FSStatusCode::OK));

    CreateFsRequest req;
    req.set_fsname(fsname);
    req.set_blocksize(blocksize);
    req.set_fstype(type);
    req.set_allocated_fsdetail(new FsDetail(detail));
    req.set_enablesumindir(false);
    req.set_owner("test");
    req.set_capacity((uint64_t)100 * 1024 * 1024 * 1024);
    FsInfo resultInfo;
    EXPECT_EQ(FSStatusCode::OK, fsManager_->CreateFs(&req, &resultInfo));

    EXPECT_EQ(FsStatus::INITED, resultInfo.status());
}

TEST_F(FsManagerTest2, checkFsName) {
    EXPECT_FALSE(FsManager::CheckFsName("/hello"));
    EXPECT_TRUE(FsManager::CheckFsName("curve-test-01"));
    EXPECT_FALSE(FsManager::CheckFsName("-"));
    EXPECT_FALSE(FsManager::CheckFsName("--"));
    EXPECT_FALSE(FsManager::CheckFsName("curve-test--01"));
}

TEST_F(FsManagerTest2, test_tso) {
    // Tso success
    TsoRequest request;
    TsoResponse response;
    EXPECT_CALL(*storage_, Tso(_, _)).
        WillOnce(DoAll(SetArgPointee<0>(1),
                       SetArgPointee<1>(100),
                       Return(FSStatusCode::OK)));
    fsManager_->Tso(&request, &response);
    ASSERT_EQ(response.statuscode(), FSStatusCode::OK);
    ASSERT_EQ(response.ts(), 1);
    ASSERT_EQ(response.timestamp(), 100);

    // Tso failed
    EXPECT_CALL(*storage_, Tso(_, _)).
        WillOnce(Return(FSStatusCode::INTERNAL_ERROR));
    fsManager_->Tso(&request, &response);
    ASSERT_EQ(response.statuscode(), FSStatusCode::INTERNAL_ERROR);
}

}  // namespace mds
}  // namespace curvefs
