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
 * Created Date: Thur Jun 17 2021
 * Author: lixiaocui
 */

#include <brpc/server.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/test/client/rpcclient/mock_mds_base_client.h"
#include "curvefs/test/client/rpcclient/mock_mds_service.h"
#include "src/client/mds_client.h"
#include "curvefs/proto/topology.pb.h"

namespace curvefs {
namespace client {
namespace rpcclient {
using curvefs::mds::topology::TopoStatusCode;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

using ::curvefs::mds::topology::TopoStatusCode;

void MountFsRpcFailed(const std::string &fsName, const std::string &mountPt,
                      MountFsResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void UmountFsRpcFailed(const std::string &fsName, const std::string &mountPt,
                       UmountFsResponse *response, brpc::Controller *cntl,
                       brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void GetFsInfoByFsnameRpcFailed(const std::string &fsName,
                                GetFsInfoResponse *response,
                                brpc::Controller *cntl,
                                brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void GetFsInfoByFsIDRpcFailed(uint32_t fsId, GetFsInfoResponse *response,
                              brpc::Controller *cntl, brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void GetMetaServerInfoRpcFailed(uint32_t port, std::string ip,
                                GetMetaServerInfoResponse *response,
                                brpc::Controller *cntl,
                                brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void GetMetaServerListInCopysetsRpcFailed(
    const LogicPoolID &logicalpooid, const std::vector<CopysetID> &copysetidvec,
    GetMetaServerListInCopySetsResponse *response, brpc::Controller *cntl,
    brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void CreatePartitionRpcFailed(uint32_t fsID, uint32_t count,
                              CreatePartitionResponse *response,
                              brpc::Controller *cntl, brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void GetCopysetOfPartitionsRpcFailed(
    const std::vector<uint32_t> &partitionIDList,
    GetCopysetOfPartitionResponse *response, brpc::Controller *cntl,
    brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

void ListPartitionRpcFailed(uint32_t fsID, ListPartitionResponse *response,
                            brpc::Controller *cntl, brpc::Channel *channel) {
    cntl->SetFailed(112, "Not connected to");
}

bool ComparePartition(PartitionInfo first, PartitionInfo second) {
    return first.fsid() == second.fsid() && first.poolid() == second.poolid() &&
           first.copysetid() == second.copysetid() &&
           first.partitionid() == second.partitionid() &&
           first.start() == second.start() && first.end() == second.end();
}

bool CompareCopyset(Copyset first, Copyset second) {
    return first.poolid() == second.poolid() &&
           first.copysetid() == second.copysetid();
}

class MdsClientImplTest : public testing::Test {
 protected:
    void SetUp() override {
        ::curve::client::MetaServerOption mdsopt;
        mdsopt.rpcRetryOpt.addrs = {addr_};
        mdsopt.rpcRetryOpt.rpcTimeoutMs = 500;            // 500ms
        mdsopt.rpcRetryOpt.maxRPCTimeoutMS = 1000;        // 1s
        mdsopt.rpcRetryOpt.rpcRetryIntervalUS = 1000000;  // 100ms
        mdsopt.mdsMaxRetryMS = 2000;                      // 2s
        mdsopt.rpcRetryOpt.maxFailedTimesBeforeChangeAddr = 2;

        ASSERT_EQ(FSStatusCode::OK, mdsclient_.Init(mdsopt, &mockmdsbasecli_));

        ASSERT_EQ(0, server_.AddService(&mockMdsService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    MdsClientImpl mdsclient_;
    MockMDSBaseClient mockmdsbasecli_;

    MockMdsService mockMdsService_;
    std::string addr_ = "127.0.0.1:5602";
    brpc::Server server_;
};

TEST_F(MdsClientImplTest, test_MountFs) {
    std::string fsName = "test1";
    std::string mp = "0.0.0.0:/data";
    FsInfo out;

    curvefs::mds::MountFsResponse response;
    auto fsinfo = new curvefs::mds::FsInfo();
    fsinfo->set_fsid(1);
    fsinfo->set_fsname(fsName);
    fsinfo->set_rootinodeid(1);
    fsinfo->set_capacity(10 * 1024 * 1024L);
    fsinfo->set_blocksize(4 * 1024);
    auto vresp = new curvefs::common::Volume();
    vresp->set_volumesize(10 * 1024 * 1024L);
    vresp->set_blocksize(4 * 1024);
    vresp->set_volumename("test1");
    vresp->set_user("test");
    vresp->set_password("test");
    auto detail = new curvefs::mds::FsDetail();
    detail->set_allocated_volume(vresp);
    fsinfo->set_allocated_detail(detail);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);

    // 1. mount ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, MountFs(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_EQ(FSStatusCode::OK, mdsclient_.MountFs(fsName, mp, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 2. mount point not exist
    out.Clear();
    response.set_statuscode(curvefs::mds::FSStatusCode::MOUNT_POINT_NOT_EXIST);
    EXPECT_CALL(mockmdsbasecli_, MountFs(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_EQ(FSStatusCode::MOUNT_POINT_NOT_EXIST,
              mdsclient_.MountFs(fsName, mp, &out));
    ASSERT_FALSE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 3. umount rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, MountFs(_, _, _, _, _))
        .WillRepeatedly(Invoke(MountFsRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.MountFs(fsName, mp, &out));
}

TEST_F(MdsClientImplTest, test_UmountFs) {
    std::string fsName = "test1";
    std::string mp = "0.0.0.0:/data";
    curvefs::mds::UmountFsResponse response;

    // 1. umount ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, UmountFs(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_EQ(FSStatusCode::OK, mdsclient_.UmountFs(fsName, mp));

    // 2. umount unknown error
    response.set_statuscode(curvefs::mds::FSStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockmdsbasecli_, UmountFs(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_EQ(FSStatusCode::UNKNOWN_ERROR, mdsclient_.UmountFs(fsName, mp));

    // 3. umount rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, UmountFs(_, _, _, _, _))
        .WillRepeatedly(Invoke(UmountFsRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.UmountFs(fsName, mp));
}

TEST_F(MdsClientImplTest, test_GetFsInfo_by_fsname) {
    std::string fsName = "test1";
    FsInfo out;

    curvefs::mds::GetFsInfoResponse response;
    auto fsinfo = new curvefs::mds::FsInfo();
    fsinfo->set_fsid(1);
    fsinfo->set_fsname(fsName);
    fsinfo->set_rootinodeid(1);
    fsinfo->set_capacity(10 * 1024 * 1024L);
    fsinfo->set_blocksize(4 * 1024);
    auto vresp = new curvefs::common::Volume();
    vresp->set_volumesize(10 * 1024 * 1024L);
    vresp->set_blocksize(4 * 1024);
    vresp->set_volumename("test1");
    vresp->set_user("test");
    vresp->set_password("test");
    auto detail = new curvefs::mds::FsDetail();
    detail->set_allocated_volume(vresp);
    fsinfo->set_allocated_detail(detail);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);

    // 1. get fsinfo ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsName, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_EQ(FSStatusCode::OK, mdsclient_.GetFsInfo(fsName, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 2. get fsinfo not found
    out.Clear();
    response.set_statuscode(curvefs::mds::FSStatusCode::NOT_FOUND);
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsName, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_EQ(FSStatusCode::NOT_FOUND, mdsclient_.GetFsInfo(fsName, &out));
    ASSERT_FALSE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 3. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsName, _, _, _))
        .WillRepeatedly(Invoke(GetFsInfoByFsnameRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.GetFsInfo(fsName, &out));
}

TEST_F(MdsClientImplTest, test_GetFsInfo_by_fsid) {
    uint32_t fsid = 1;
    FsInfo out;

    curvefs::mds::GetFsInfoResponse response;
    auto fsinfo = new curvefs::mds::FsInfo();
    fsinfo->set_fsid(1);
    fsinfo->set_fsname("test1");
    fsinfo->set_rootinodeid(1);
    fsinfo->set_capacity(10 * 1024 * 1024L);
    fsinfo->set_blocksize(4 * 1024);
    auto vresp = new curvefs::common::Volume();
    vresp->set_volumesize(10 * 1024 * 1024L);
    vresp->set_blocksize(4 * 1024);
    vresp->set_volumename("test1");
    vresp->set_user("test");
    vresp->set_password("test");
    auto detail = new curvefs::mds::FsDetail();
    detail->set_allocated_volume(vresp);
    fsinfo->set_allocated_detail(detail);
    fsinfo->set_mountnum(1);
    fsinfo->add_mountpoints("0.0.0.0:/data");
    response.set_allocated_fsinfo(fsinfo);

    // 1. get file info ok
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsid, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_EQ(FSStatusCode::OK, mdsclient_.GetFsInfo(fsid, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 2. get fsinfo unknow error
    out.Clear();
    response.set_statuscode(curvefs::mds::FSStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsid, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_EQ(FSStatusCode::UNKNOWN_ERROR, mdsclient_.GetFsInfo(fsid, &out));
    ASSERT_FALSE(
        google::protobuf::util::MessageDifferencer::Equals(*fsinfo, out));

    // 3. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, GetFsInfo(fsid, _, _, _))
        .WillRepeatedly(Invoke(GetFsInfoByFsIDRpcFailed));
    ASSERT_EQ(FSStatusCode::RPC_ERROR, mdsclient_.GetFsInfo(fsid, &out));
}

TEST_F(MdsClientImplTest, CommitTx) {
    curvefs::mds::topology::CommitTxResponse response;

    // CASE 1: CommitTx success
    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockmdsbasecli_, CommitTx(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));

    auto txIds = std::vector<PartitionTxId>();
    auto rc = mdsclient_.CommitTx(txIds);
    ASSERT_EQ(rc, TopoStatusCode::TOPO_OK);

    // CASE 2: CommitTx fail
    response.set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
    EXPECT_CALL(mockmdsbasecli_, CommitTx(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));

    rc = mdsclient_.CommitTx(txIds);
    ASSERT_EQ(rc, TopoStatusCode::TOPO_INTERNAL_ERROR);

    // CASE 3: RPC error, retry until success
    int count = 0;
    EXPECT_CALL(mockmdsbasecli_, CommitTx(_, _, _, _))
        .Times(6)
        .WillRepeatedly(
            Invoke([&](const std::vector<PartitionTxId> &txIds,
                       CommitTxResponse *response, brpc::Controller *cntl,
                       brpc::Channel *channel) {
                if (++count <= 5) {
                    cntl->SetFailed(112, "Not connected to");
                } else {
                    response->set_statuscode(TopoStatusCode::TOPO_OK);
                }
            }));

    rc = mdsclient_.CommitTx(txIds);
    ASSERT_EQ(rc, TopoStatusCode::TOPO_OK);
}

TEST_F(MdsClientImplTest, test_GetMetaServerInfo) {
    // in
    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 5000, &ep);
    curve::client::PeerAddr addr(ep);

    // out
    CopysetPeerInfo<MetaserverID> out;

    curvefs::mds::topology::GetMetaServerInfoResponse response;
    auto metaserverInfo = new curvefs::mds::topology::MetaServerInfo();
    metaserverInfo->set_metaserverid(1);
    metaserverInfo->set_hostname("hangzhou");
    metaserverInfo->set_internalip("127.0.0.1");
    metaserverInfo->set_internalport(5000);
    metaserverInfo->set_externalip("127.0.0.1");
    metaserverInfo->set_externalport(5000);
    metaserverInfo->set_onlinestate(::curvefs::mds::topology::ONLINE);
    response.set_allocated_metaserverinfo(metaserverInfo);

    // 1. get metaserver info ok
    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerInfo(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_TRUE(mdsclient_.GetMetaServerInfo(addr, &out));
    ASSERT_TRUE(out ==
                CopysetPeerInfo<MetaserverID>(1, PeerAddr(ep), PeerAddr(ep)));

    // 2. get metaserver info not found
    response.set_statuscode(TopoStatusCode::TOPO_METASERVER_NOT_FOUND);
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerInfo(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_FALSE(mdsclient_.GetMetaServerInfo(addr, &out));

    // 3. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerInfo(_, _, _, _, _))
        .WillRepeatedly(Invoke(GetMetaServerInfoRpcFailed));
    ASSERT_FALSE(mdsclient_.GetMetaServerInfo(addr, &out));
}

TEST_F(MdsClientImplTest, GetMetaServerListInCopysets) {
    // in
    LogicPoolID poolID = 1;
    std::vector<CopysetID> copysetidvec{1};

    // out
    std::vector<CopysetInfo<MetaserverID>> out;

    curvefs::mds::topology::GetMetaServerListInCopySetsResponse response;
    auto copysetInfo = response.add_csinfo();
    copysetInfo->set_copysetid(1);
    auto l1 = copysetInfo->add_cslocs();
    auto l2 = copysetInfo->add_cslocs();
    auto l3 = copysetInfo->add_cslocs();
    l1->set_metaserverid(1);
    l1->set_internalip("127.0.0.1");
    l1->set_internalport(9000);
    l1->set_externalip("127.0.0.1");
    l2->CopyFrom(*l1);
    l2->set_metaserverid(2);
    l3->CopyFrom(*l1);
    l3->set_metaserverid(3);

    // 1. get metaserver list in copysets ok
    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerListInCopysets(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_TRUE(
        mdsclient_.GetMetaServerListInCopysets(poolID, copysetidvec, &out));
    ASSERT_EQ(1, out.size());
    ASSERT_EQ(3, out[0].csinfos_.size());
    std::list<int> ids{1, 2, 3};
    for (int i = 0; i <= 2; i++) {
        ASSERT_TRUE(std::find(ids.begin(), ids.end(),
                              out[0].csinfos_[i].peerID) != ids.end());
    }

    // 2. get metaserver list in copyset internal error
    response.set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerListInCopysets(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_FALSE(
        mdsclient_.GetMetaServerListInCopysets(poolID, copysetidvec, &out));

    // 3. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, GetMetaServerListInCopysets(_, _, _, _, _))
        .WillRepeatedly(Invoke(GetMetaServerListInCopysetsRpcFailed));
    ASSERT_FALSE(
        mdsclient_.GetMetaServerListInCopysets(poolID, copysetidvec, &out));
}

TEST_F(MdsClientImplTest, CreatePartition) {
    // in
    uint32_t fsID = 1;
    uint32_t count = 2;

    // out
    std::vector<PartitionInfo> out;

    PartitionInfo partitioninfo1;
    PartitionInfo partitioninfo2;
    partitioninfo1.set_fsid(fsID);
    partitioninfo1.set_poolid(1);
    partitioninfo1.set_copysetid(2);
    partitioninfo1.set_partitionid(3);
    partitioninfo1.set_start(4);
    partitioninfo1.set_end(5);

    partitioninfo2.set_fsid(fsID);
    partitioninfo2.set_poolid(2);
    partitioninfo2.set_copysetid(3);
    partitioninfo2.set_partitionid(4);
    partitioninfo2.set_start(5);
    partitioninfo2.set_end(6);

    curvefs::mds::topology::CreatePartitionResponse response;
    // 1. create partition return ok, but no partition info returns
    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockmdsbasecli_, CreatePartition(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_FALSE(mdsclient_.CreatePartition(fsID, count, &out));

    // 2. create partition ok
    response.add_partitioninfolist()->CopyFrom(partitioninfo1);
    response.add_partitioninfolist()->CopyFrom(partitioninfo2);

    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockmdsbasecli_, CreatePartition(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_TRUE(mdsclient_.CreatePartition(fsID, count, &out));
    ASSERT_EQ(2, out.size());
    ASSERT_TRUE(ComparePartition(out[0], partitioninfo1));
    ASSERT_TRUE(ComparePartition(out[1], partitioninfo2));

    // 3. create partition fail
    response.set_statuscode(TopoStatusCode::TOPO_COPYSET_NOT_FOUND);
    EXPECT_CALL(mockmdsbasecli_, CreatePartition(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_FALSE(mdsclient_.CreatePartition(fsID, count, &out));

    // 4. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, CreatePartition(_, _, _, _, _))
        .WillRepeatedly(Invoke(CreatePartitionRpcFailed));
    ASSERT_FALSE(mdsclient_.CreatePartition(fsID, count, &out));
}

TEST_F(MdsClientImplTest, ListPartition) {
    // in
    uint32_t fsID = 1;

    // out
    std::vector<PartitionInfo> out;

    PartitionInfo partitioninfo1;
    PartitionInfo partitioninfo2;
    partitioninfo1.set_fsid(fsID);
    partitioninfo1.set_poolid(1);
    partitioninfo1.set_copysetid(2);
    partitioninfo1.set_partitionid(3);
    partitioninfo1.set_start(4);
    partitioninfo1.set_end(5);

    partitioninfo2.set_fsid(fsID);
    partitioninfo2.set_poolid(2);
    partitioninfo2.set_copysetid(3);
    partitioninfo2.set_partitionid(4);
    partitioninfo2.set_start(5);
    partitioninfo2.set_end(6);

    curvefs::mds::topology::ListPartitionResponse response;
    response.add_partitioninfolist()->CopyFrom(partitioninfo1);
    response.add_partitioninfolist()->CopyFrom(partitioninfo2);

    // 1. get metaserver list in copysets ok
    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockmdsbasecli_, ListPartition(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_TRUE(mdsclient_.ListPartition(fsID, &out));

    ASSERT_EQ(2, out.size());
    ASSERT_TRUE(ComparePartition(out[0], partitioninfo1));
    ASSERT_TRUE(ComparePartition(out[1], partitioninfo2));

    // 2. get metaserver list in copyset unknown error
    response.set_statuscode(TopoStatusCode::TOPO_COPYSET_NOT_FOUND);
    EXPECT_CALL(mockmdsbasecli_, ListPartition(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_FALSE(mdsclient_.ListPartition(fsID, &out));

    // 3. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, ListPartition(_, _, _, _))
        .WillRepeatedly(Invoke(ListPartitionRpcFailed));
    ASSERT_FALSE(mdsclient_.ListPartition(fsID, &out));
}

TEST_F(MdsClientImplTest, GetCopysetOfPartition) {
    // in
    std::vector<uint32_t> partitionIDList{1, 2};

    // out
    std::map<uint32_t, Copyset> out;

    Copyset copyset1;
    Copyset copyset2;
    copyset1.set_poolid(1);
    copyset1.set_copysetid(2);
    Peer peer1;
    peer1.set_id(3);
    peer1.set_address("addr1");
    copyset1.add_peers()->CopyFrom(peer1);

    copyset2.set_poolid(2);
    copyset2.set_copysetid(3);
    Peer peer2;
    peer2.set_id(4);
    peer2.set_address("addr2");
    copyset2.add_peers()->CopyFrom(peer2);

    curvefs::mds::topology::GetCopysetOfPartitionResponse response;

    // 1. get metaserver list return ok, but no copyset returns
    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockmdsbasecli_, GetCopysetOfPartitions(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_FALSE(mdsclient_.GetCopysetOfPartitions(partitionIDList, &out));

    // 2. get metaserver list in copysets ok
    auto copysetMap = response.mutable_copysetmap();
    (*copysetMap)[1] = copyset1;
    (*copysetMap)[2] = copyset2;

    response.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(mockmdsbasecli_, GetCopysetOfPartitions(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_TRUE(mdsclient_.GetCopysetOfPartitions(partitionIDList, &out));
    ASSERT_TRUE(CompareCopyset(out[1], copyset1));
    ASSERT_TRUE(CompareCopyset(out[2], copyset2));

    // 3. get metaserver list in copyset unknown error
    response.set_statuscode(TopoStatusCode::TOPO_COPYSET_NOT_FOUND);
    EXPECT_CALL(mockmdsbasecli_, GetCopysetOfPartitions(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));
    ASSERT_FALSE(mdsclient_.GetCopysetOfPartitions(partitionIDList, &out));

    // 4. get rpc error
    brpc::Controller cntl;
    cntl.SetFailed(ECONNRESET, "error connect reset");
    EXPECT_CALL(mockmdsbasecli_, GetCopysetOfPartitions(_, _, _, _))
        .WillRepeatedly(Invoke(GetCopysetOfPartitionsRpcFailed));
    ASSERT_FALSE(mdsclient_.GetCopysetOfPartitions(partitionIDList, &out));
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
