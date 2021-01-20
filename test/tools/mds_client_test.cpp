/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * File Created: 2019-11-26
 * Author: charisu
 */

#include <gtest/gtest.h>
#include <brpc/server.h>
#include <string>
#include "src/tools/mds_client.h"
#include "test/tools/mock/mock_namespace_service.h"
#include "test/tools/mock/mock_topology_service.h"
#include "test/tools/mock/mock_schedule_service.h"

using curve::mds::topology::DiskState;
using curve::mds::topology::OnlineState;
using curve::mds::topology::AllocateStatus;
using curve::mds::topology::LogicalPoolType;
using curve::mds::topology::ListPhysicalPoolRequest;
using curve::mds::topology::ListPhysicalPoolResponse;
using curve::mds::topology::ListLogicalPoolRequest;
using curve::mds::topology::ListLogicalPoolResponse;
using curve::mds::topology::GetChunkServerListInCopySetsRequest;
using curve::mds::topology::GetChunkServerListInCopySetsResponse;
using curve::mds::topology::GetCopySetsInChunkServerRequest;
using curve::mds::topology::GetCopySetsInChunkServerResponse;
using curve::mds::topology::GetCopySetsInClusterRequest;
using curve::mds::topology::GetCopySetsInClusterResponse;
using curve::mds::schedule::RapidLeaderScheduleResponse;
using curve::mds::schedule::QueryChunkServerRecoverStatusRequest;
using curve::mds::schedule::QueryChunkServerRecoverStatusResponse;
using curve::mds::DefaultSegmentSize;

using ::testing::_;
using ::testing::Return;
using ::testing::Invoke;
using ::testing::DoAll;
using ::testing::SetArgPointee;

DECLARE_string(mdsDummyPort);

namespace curve {
namespace tool {

const char mdsAddr[] = "127.0.0.1:9191,127.0.0.1:9192";

class ToolMDSClientTest : public ::testing::Test {
 protected:
    ToolMDSClientTest() {}
    void SetUp() {
        server = new brpc::Server();
        nameService = new curve::mds::MockNameService();
        topoService = new curve::mds::topology::MockTopologyService();
        scheduleService = new curve::mds::schedule::MockScheduleService();
        ASSERT_EQ(0, server->AddService(nameService,
                                      brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server->AddService(topoService,
                                      brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server->AddService(scheduleService,
                                      brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server->Start("127.0.0.1:9192", nullptr));
        brpc::StartDummyServerAt(9193);

        // 初始化mds client
        curve::mds::topology::ListPhysicalPoolResponse response;
        response.set_statuscode(kTopoErrCodeSuccess);
        EXPECT_CALL(*topoService, ListPhysicalPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const ListPhysicalPoolRequest *request,
                          ListPhysicalPoolResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
        ASSERT_EQ(0, mdsClient.Init(mdsAddr, "9194,9193"));
    }
    void TearDown() {
        server->Stop(0);
        server->Join();
        delete server;
        server = nullptr;
        delete nameService;
        nameService = nullptr;
        delete topoService;
        topoService = nullptr;
        delete scheduleService;
        scheduleService = nullptr;
    }

    void GetFileInfoForTest(uint64_t id, FileInfo* fileInfo) {
        fileInfo->set_id(id);
        fileInfo->set_filename("test");
        fileInfo->set_parentid(0);
        fileInfo->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        fileInfo->set_segmentsize(DefaultSegmentSize);
        fileInfo->set_length(DefaultSegmentSize * 10);
        fileInfo->set_originalfullpathname("/cinder/test");
        fileInfo->set_ctime(1573546993000000);
    }

    void GetCopysetInfoForTest(CopySetServerInfo* info,
                                int num, uint32_t copysetId = 1) {
        info->Clear();
        for (int i = 0; i < num; ++i) {
            curve::mds::topology::ChunkServerLocation *csLoc =
                                                info->add_cslocs();
            csLoc->set_chunkserverid(i);
            csLoc->set_hostip("127.0.0.1");
            csLoc->set_port(9191 + i);
        }
        info->set_copysetid(copysetId);
    }

    void GetSegmentForTest(PageFileSegment* segment) {
        segment->set_logicalpoolid(1);
        segment->set_segmentsize(DefaultSegmentSize);
        segment->set_chunksize(kChunkSize);
        segment->set_startoffset(0);
    }

    void GetPhysicalPoolInfoForTest(PoolIdType id, PhysicalPoolInfo* pool) {
        pool->set_physicalpoolid(id);
        pool->set_physicalpoolname("testPool");
        pool->set_desc("physical pool for test");
    }

    void GetLogicalPoolForTest(PoolIdType id,
                        curve::mds::topology::LogicalPoolInfo *lpInfo) {
        lpInfo->set_logicalpoolid(id);
        lpInfo->set_logicalpoolname("defaultLogicalPool");
        lpInfo->set_physicalpoolid(1);
        lpInfo->set_type(LogicalPoolType::PAGEFILE);
        lpInfo->set_createtime(1574218021);
        lpInfo->set_redundanceandplacementpolicy(
            "{\"zoneNum\": 3, \"copysetNum\": 4000, \"replicaNum\": 3}");
        lpInfo->set_userpolicy("{\"policy\": 1}");
        lpInfo->set_allocatestatus(AllocateStatus::ALLOW);
    }

    void GetZoneInfoForTest(ZoneIdType id, ZoneInfo *zoneInfo) {
        zoneInfo->set_zoneid(1);
        zoneInfo->set_zonename("testZone");
        zoneInfo->set_physicalpoolid(1);
        zoneInfo->set_physicalpoolname("testPool");
    }

    void GetServerInfoForTest(ServerIdType id, ServerInfo *serverInfo) {
        serverInfo->set_serverid(id);
        serverInfo->set_hostname("localhost");
        serverInfo->set_internalip("127.0.0.1");
        serverInfo->set_internalport(8080);
        serverInfo->set_externalip("127.0.0.1");
        serverInfo->set_externalport(8081);
        serverInfo->set_zoneid(1);
        serverInfo->set_zonename("testZone");
        serverInfo->set_physicalpoolid(1);
        serverInfo->set_physicalpoolname("testPool");
        serverInfo->set_desc("123");
    }

    void GetChunkServerInfoForTest(ChunkServerIdType id,
                                   ChunkServerInfo *csInfo,
                                   bool retired = false) {
        csInfo->set_chunkserverid(id);
        csInfo->set_disktype("ssd");
        csInfo->set_hostip("127.0.0.1");
        csInfo->set_port(8200 + id);
        csInfo->set_onlinestate(OnlineState::ONLINE);
        if (retired) {
            csInfo->set_status(ChunkServerStatus::RETIRED);
        } else {
            csInfo->set_status(ChunkServerStatus::READWRITE);
        }
        csInfo->set_diskstatus(DiskState::DISKNORMAL);
        csInfo->set_mountpoint("/test");
        csInfo->set_diskcapacity(1024);
        csInfo->set_diskused(512);
    }
    brpc::Server* server;
    curve::mds::MockNameService* nameService;
    curve::mds::topology::MockTopologyService* topoService;
    curve::mds::schedule::MockScheduleService* scheduleService;
    MDSClient mdsClient;
    const uint64_t kChunkSize = 16777216;
};

TEST(MDSClientInitTest, Init) {
    MDSClient mdsClient;
    ASSERT_EQ(-1, mdsClient.Init(""));
    ASSERT_EQ(-1, mdsClient.Init("127.0.0.1"));
    ASSERT_EQ(-1, mdsClient.Init("127.0.0.1:65536"));
    // dummy server非法
    ASSERT_EQ(-1, mdsClient.Init(mdsAddr, ""));
    // dummy server与mds不匹配
    ASSERT_EQ(-1, mdsClient.Init(mdsAddr, "9091,9092,9093"));
}

TEST_F(ToolMDSClientTest, GetFileInfo) {
    mdsClient.SetUserName("root");
    mdsClient.SetPassword("root_password");
    std::string filename = "/test";
    curve::mds::FileInfo outFileInfo;

    // 发送RPC失败
    EXPECT_CALL(*nameService, GetFileInfo(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                          const curve::mds::GetFileInfoRequest *request,
                          curve::mds::GetFileInfoResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                          brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                          cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.GetFileInfo(filename, &outFileInfo));

    // 返回码不为OK
    curve::mds::GetFileInfoResponse response;
    response.set_statuscode(curve::mds::StatusCode::kParaError);
    EXPECT_CALL(*nameService, GetFileInfo(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const curve::mds::GetFileInfoRequest *request,
                          curve::mds::GetFileInfoResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.GetFileInfo(filename, &outFileInfo));

    // 正常情况
    curve::mds::FileInfo * info = new curve::mds::FileInfo;
    GetFileInfoForTest(1, info);
    response.set_allocated_fileinfo(info);
    response.set_statuscode(curve::mds::StatusCode::kOK);
    EXPECT_CALL(*nameService, GetFileInfo(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const curve::mds::GetFileInfoRequest *request,
                          curve::mds::GetFileInfoResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.GetFileInfo(filename, &outFileInfo));
    ASSERT_EQ(info->DebugString(), outFileInfo.DebugString());
}

TEST_F(ToolMDSClientTest, GetAllocatedSize) {
    uint64_t allocSize;
    std::string filename = "/test";
    // 发送RPC失败
    EXPECT_CALL(*nameService, GetAllocatedSize(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                          const curve::mds::GetAllocatedSizeRequest *request,
                          curve::mds::GetAllocatedSizeResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                          brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                          cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.GetAllocatedSize(filename, &allocSize));

    // 返回码不为OK
    curve::mds::GetAllocatedSizeResponse response;
    response.set_statuscode(curve::mds::StatusCode::kParaError);
    EXPECT_CALL(*nameService, GetAllocatedSize(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const curve::mds::GetAllocatedSizeRequest *request,
                          curve::mds::GetAllocatedSizeResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.GetAllocatedSize(filename, &allocSize));

    // 正常情况
    response.set_allocatedsize(DefaultSegmentSize * 3);
    for (int i = 1; i <= 3; ++i) {
        response.mutable_allocsizemap()->insert({i, DefaultSegmentSize});
    }
    response.set_statuscode(curve::mds::StatusCode::kOK);
    EXPECT_CALL(*nameService, GetAllocatedSize(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const curve::mds::GetAllocatedSizeRequest *request,
                          curve::mds::GetAllocatedSizeResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
    AllocMap allocMap;
    ASSERT_EQ(0, mdsClient.GetAllocatedSize(filename, &allocSize, &allocMap));
    ASSERT_EQ(DefaultSegmentSize * 3, allocSize);
    AllocMap expected = {{1, DefaultSegmentSize}, {2, DefaultSegmentSize},
                         {3, DefaultSegmentSize}};
    ASSERT_EQ(expected, allocMap);
}

TEST_F(ToolMDSClientTest, ListDir) {
    std::string fileName = "/test";
    std::vector<FileInfo> fileInfoVec;

    // 发送RPC失败
    EXPECT_CALL(*nameService, ListDir(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                          const curve::mds::ListDirRequest *request,
                          curve::mds::ListDirResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                          brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                          cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.ListDir(fileName, &fileInfoVec));

    // 返回码不为OK
    curve::mds::ListDirResponse response;
    response.set_statuscode(curve::mds::StatusCode::kParaError);
    EXPECT_CALL(*nameService, ListDir(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const curve::mds::ListDirRequest *request,
                          curve::mds::ListDirResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.ListDir(fileName, &fileInfoVec));
    // 正常情况
    response.set_statuscode(curve::mds::StatusCode::kOK);
    for (int i = 0; i < 5; i++) {
        auto fileInfo = response.add_fileinfo();
        GetFileInfoForTest(i, fileInfo);
    }
    EXPECT_CALL(*nameService, ListDir(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const curve::mds::ListDirRequest *request,
                          curve::mds::ListDirResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.ListDir(fileName, &fileInfoVec));
    for (int i = 0; i < 5; i++) {
        FileInfo expected;
        GetFileInfoForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), fileInfoVec[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, GetSegmentInfo) {
    std::string fileName = "/test";
    curve::mds::PageFileSegment outSegment;
    uint64_t offset = 0;

    // 发送RPC失败
    EXPECT_CALL(*nameService, GetOrAllocateSegment(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const curve::mds::GetOrAllocateSegmentRequest *request,
                        curve::mds::GetOrAllocateSegmentResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(GetSegmentRes::kOtherError,
                    mdsClient.GetSegmentInfo(fileName, offset, &outSegment));

    // segment不存在
    curve::mds::GetOrAllocateSegmentResponse response;
    response.set_statuscode(curve::mds::StatusCode::kSegmentNotAllocated);
    EXPECT_CALL(*nameService, GetOrAllocateSegment(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::GetOrAllocateSegmentRequest *request,
                        curve::mds::GetOrAllocateSegmentResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(GetSegmentRes::kSegmentNotAllocated,
                    mdsClient.GetSegmentInfo(fileName, offset, &outSegment));
    // 文件不存在
    response.set_statuscode(curve::mds::StatusCode::kFileNotExists);
    EXPECT_CALL(*nameService, GetOrAllocateSegment(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::GetOrAllocateSegmentRequest *request,
                        curve::mds::GetOrAllocateSegmentResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(GetSegmentRes::kFileNotExists,
                    mdsClient.GetSegmentInfo(fileName, offset, &outSegment));

    // 其他错误
    response.set_statuscode(curve::mds::StatusCode::kParaError);
    EXPECT_CALL(*nameService, GetOrAllocateSegment(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::GetOrAllocateSegmentRequest *request,
                        curve::mds::GetOrAllocateSegmentResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(GetSegmentRes::kOtherError,
                    mdsClient.GetSegmentInfo(fileName, offset, &outSegment));

    // 正常情况
    PageFileSegment* segment = new PageFileSegment();
    GetSegmentForTest(segment);
    response.set_statuscode(curve::mds::StatusCode::kOK);
    response.set_allocated_pagefilesegment(segment);
    EXPECT_CALL(*nameService, GetOrAllocateSegment(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::GetOrAllocateSegmentRequest *request,
                        curve::mds::GetOrAllocateSegmentResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(GetSegmentRes::kOK,
                    mdsClient.GetSegmentInfo(fileName, offset, &outSegment));
    ASSERT_EQ(segment->DebugString(), outSegment.DebugString());
}

TEST_F(ToolMDSClientTest, DeleteFile) {
    std::string fileName = "/test";

    // 发送RPC失败
    EXPECT_CALL(*nameService, DeleteFile(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const curve::mds::DeleteFileRequest *request,
                        curve::mds::DeleteFileResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.DeleteFile(fileName));

    // 返回码不为OK
    curve::mds::DeleteFileResponse response;
    response.set_statuscode(curve::mds::StatusCode::kParaError);
    EXPECT_CALL(*nameService, DeleteFile(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::DeleteFileRequest *request,
                        curve::mds::DeleteFileResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.DeleteFile(fileName));

    // 正常情况
    response.set_statuscode(curve::mds::StatusCode::kOK);
    EXPECT_CALL(*nameService, DeleteFile(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::DeleteFileRequest *request,
                        curve::mds::DeleteFileResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.DeleteFile(fileName));
}

TEST_F(ToolMDSClientTest, CreateFile) {
    std::string fileName = "/test";
    uint64_t length = 10 * DefaultSegmentSize;

    // 发送RPC失败
    EXPECT_CALL(*nameService, CreateFile(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const curve::mds::CreateFileRequest *request,
                        curve::mds::CreateFileResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.CreateFile(fileName, length));

    // 返回码不为OK
    curve::mds::CreateFileResponse response;
    response.set_statuscode(curve::mds::StatusCode::kParaError);
    EXPECT_CALL(*nameService, CreateFile(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::CreateFileRequest *request,
                        curve::mds::CreateFileResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.CreateFile(fileName, length));

    // 正常情况
    response.set_statuscode(curve::mds::StatusCode::kOK);
    EXPECT_CALL(*nameService, CreateFile(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::CreateFileRequest *request,
                        curve::mds::CreateFileResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.CreateFile(fileName, length));
}

TEST_F(ToolMDSClientTest, GetChunkServerListInCopySets) {
    PoolIdType logicalPoolId = 1;
    CopySetIdType copysetId = 100;
    std::vector<ChunkServerLocation> csLocs;

    // 发送rpc失败
    EXPECT_CALL(*topoService, GetChunkServerListInCopySets(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const GetChunkServerListInCopySetsRequest *request,
                        GetChunkServerListInCopySetsResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.GetChunkServerListInCopySet(
                                logicalPoolId, copysetId, &csLocs));

    // 返回码不为OK
    GetChunkServerListInCopySetsResponse response;
    response.set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    EXPECT_CALL(*topoService, GetChunkServerListInCopySets(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const GetChunkServerListInCopySetsRequest *request,
                        GetChunkServerListInCopySetsResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.GetChunkServerListInCopySet(
                                logicalPoolId, copysetId, &csLocs));

    // 正常情况
    response.set_statuscode(kTopoErrCodeSuccess);
    CopySetServerInfo csInfo;
    GetCopysetInfoForTest(&csInfo, 3, copysetId);
    auto infoPtr = response.add_csinfo();
    infoPtr->CopyFrom(csInfo);
    EXPECT_CALL(*topoService, GetChunkServerListInCopySets(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const GetChunkServerListInCopySetsRequest *request,
                        GetChunkServerListInCopySetsResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.GetChunkServerListInCopySet(
                                logicalPoolId, copysetId, &csLocs));
    ASSERT_EQ(csInfo.cslocs_size(), csLocs.size());
    for (uint32_t i = 0; i < csLocs.size(); ++i) {
        ASSERT_EQ(csInfo.cslocs(i).DebugString(), csLocs[i].DebugString());
    }

    // 测试获取多个copyset
    std::vector<CopySetServerInfo> expected;
    response.Clear();
    response.set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; ++i) {
        CopySetServerInfo csInfo;
        GetCopysetInfoForTest(&csInfo, 3, 100 + i);
        auto infoPtr = response.add_csinfo();
        infoPtr->CopyFrom(csInfo);
        expected.emplace_back(csInfo);
    }
    std::vector<CopySetIdType> copysets = {100, 101, 102};
    std::vector<CopySetServerInfo> csServerInfos;
    EXPECT_CALL(*topoService, GetChunkServerListInCopySets(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const GetChunkServerListInCopySetsRequest *request,
                        GetChunkServerListInCopySetsResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.GetChunkServerListInCopySets(
                                logicalPoolId, copysets, &csServerInfos));
    ASSERT_EQ(expected.size(), csServerInfos.size());
    for (uint32_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(expected[i].DebugString(), csServerInfos[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, ListPhysicalPoolsInCluster) {
    std::vector<PhysicalPoolInfo> pools;

    // 发送rpc失败
    EXPECT_CALL(*topoService, ListPhysicalPool(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const ListPhysicalPoolRequest *request,
                        ListPhysicalPoolResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.ListPhysicalPoolsInCluster(&pools));

    // 返回码不为OK
    ListPhysicalPoolResponse response;
    response.set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    EXPECT_CALL(*topoService, ListPhysicalPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const ListPhysicalPoolRequest *request,
                        ListPhysicalPoolResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.ListPhysicalPoolsInCluster(&pools));

    // 正常情况
    response.set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; i++) {
        auto poolInfo = response.add_physicalpoolinfos();
        GetPhysicalPoolInfoForTest(i, poolInfo);
    }
    EXPECT_CALL(*topoService, ListPhysicalPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const ListPhysicalPoolRequest *request,
                        ListPhysicalPoolResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.ListPhysicalPoolsInCluster(&pools));
    ASSERT_EQ(3, pools.size());
    for (int i = 0; i < 3; ++i) {
        PhysicalPoolInfo expected;
        GetPhysicalPoolInfoForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), pools[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, ListLogicalPoolsInPhysicalPool) {
    PoolIdType poolId = 1;
    std::vector<LogicalPoolInfo> pools;

    // 发送rpc失败
    EXPECT_CALL(*topoService, ListLogicalPool(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const ListLogicalPoolRequest *request,
                        ListLogicalPoolResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.ListLogicalPoolsInPhysicalPool(poolId, &pools));

    // 返回码不为OK
    ListLogicalPoolResponse response;
    response.set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    EXPECT_CALL(*topoService, ListLogicalPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const ListLogicalPoolRequest *request,
                        ListLogicalPoolResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.ListLogicalPoolsInPhysicalPool(poolId, &pools));

    // 正常情况
    response.set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; i++) {
        auto poolInfo = response.add_logicalpoolinfos();
        GetLogicalPoolForTest(i, poolInfo);
    }
    EXPECT_CALL(*topoService, ListLogicalPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const ListLogicalPoolRequest *request,
                        ListLogicalPoolResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.ListLogicalPoolsInPhysicalPool(poolId, &pools));
    ASSERT_EQ(3, pools.size());
    for (int i = 0; i < 3; ++i) {
        LogicalPoolInfo expected;
        GetLogicalPoolForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), pools[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, ListZoneInPhysicalPool) {
    PoolIdType poolId = 1;
    std::vector<ZoneInfo> zones;
    // 发送rpc失败
    EXPECT_CALL(*topoService, ListPoolZone(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                    const curve::mds::topology::ListPoolZoneRequest *request,
                    curve::mds::topology::ListPoolZoneResponse *response,
                    Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.ListZoneInPhysicalPool(poolId, &zones));

    // 返回码不为OK
    curve::mds::topology::ListPoolZoneResponse response;
    response.set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    EXPECT_CALL(*topoService, ListPoolZone(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                    Invoke([](RpcController *controller,
                    const curve::mds::topology::ListPoolZoneRequest *request,
                    curve::mds::topology::ListPoolZoneResponse *response,
                    Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.ListZoneInPhysicalPool(poolId, &zones));
    // 正常情况
    response.set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; i++) {
        auto zoneInfo = response.add_zones();
        GetZoneInfoForTest(i, zoneInfo);
    }
    EXPECT_CALL(*topoService, ListPoolZone(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                    Invoke([](RpcController *controller,
                    const curve::mds::topology::ListPoolZoneRequest *request,
                    curve::mds::topology::ListPoolZoneResponse *response,
                    Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.ListZoneInPhysicalPool(poolId, &zones));
    ASSERT_EQ(3, zones.size());
    for (int i = 0; i < 3; ++i) {
        ZoneInfo expected;
        GetZoneInfoForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), zones[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, ListServersInZone) {
    ZoneIdType zoneId;
    std::vector<ServerInfo> servers;

    // 发送rpc失败
    EXPECT_CALL(*topoService, ListZoneServer(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                    const curve::mds::topology::ListZoneServerRequest *request,
                    curve::mds::topology::ListZoneServerResponse *response,
                    Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.ListServersInZone(zoneId, &servers));

    // 返回码不为OK
    curve::mds::topology::ListZoneServerResponse response;
    response.set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    EXPECT_CALL(*topoService, ListZoneServer(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                    Invoke([](RpcController *controller,
                    const curve::mds::topology::ListZoneServerRequest *request,
                    curve::mds::topology::ListZoneServerResponse *response,
                    Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.ListServersInZone(zoneId, &servers));

    // 正常情况
    response.set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; i++) {
        auto serverInfo = response.add_serverinfo();
        GetServerInfoForTest(i, serverInfo);
    }
    EXPECT_CALL(*topoService, ListZoneServer(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                    Invoke([](RpcController *controller,
                    const curve::mds::topology::ListZoneServerRequest *request,
                    curve::mds::topology::ListZoneServerResponse *response,
                    Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.ListServersInZone(zoneId, &servers));
    ASSERT_EQ(3, servers.size());
    for (int i = 0; i < 3; ++i) {
        ServerInfo expected;
        GetServerInfoForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), servers[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, ListChunkServersOnServer) {
    ServerIdType serverId = 1;
    std::vector<ChunkServerInfo> chunkservers;

    // 发送rpc失败
    EXPECT_CALL(*topoService, ListChunkServer(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                    const curve::mds::topology::ListChunkServerRequest *request,
                    curve::mds::topology::ListChunkServerResponse *response,
                    Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.ListChunkServersOnServer(serverId, &chunkservers));

    // 返回码不为OK
    curve::mds::topology::ListChunkServerResponse response;
    response.set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    EXPECT_CALL(*topoService, ListChunkServer(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                    Invoke([](RpcController *controller,
                    const curve::mds::topology::ListChunkServerRequest *request,
                    curve::mds::topology::ListChunkServerResponse *response,
                    Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.ListChunkServersOnServer(serverId, &chunkservers));

    // 正常情况,两个chunkserver正常，一个chunkserver retired
    response.set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; i++) {
        auto csInfo = response.add_chunkserverinfos();
        GetChunkServerInfoForTest(i, csInfo, i == 2);
    }
    EXPECT_CALL(*topoService, ListChunkServer(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                    Invoke([](RpcController *controller,
                    const curve::mds::topology::ListChunkServerRequest *request,
                    curve::mds::topology::ListChunkServerResponse *response,
                    Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.ListChunkServersOnServer(serverId, &chunkservers));
    ASSERT_EQ(2, chunkservers.size());
    for (int i = 0; i < 2; ++i) {
        ChunkServerInfo expected;
        GetChunkServerInfoForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), chunkservers[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, GetChunkServerInfo) {
    ChunkServerIdType csId = 20;
    std::string csAddr = "127.0.0.1:8200";
    ChunkServerInfo chunkserver;

    // 发送rpc失败
    EXPECT_CALL(*topoService, GetChunkServer(_, _, _, _))
        .Times(12)
        .WillRepeatedly(Invoke([](RpcController *controller,
                const curve::mds::topology::GetChunkServerInfoRequest *request,
                curve::mds::topology::GetChunkServerInfoResponse *response,
                Closure *done){
                brpc::ClosureGuard doneGuard(done);
                brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csId, &chunkserver));
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csAddr, &chunkserver));

    // 返回码不为OK
    curve::mds::topology::GetChunkServerInfoResponse response;
    response.set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    EXPECT_CALL(*topoService, GetChunkServer(_, _, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                const curve::mds::topology::GetChunkServerInfoRequest *request,
                curve::mds::topology::GetChunkServerInfoResponse *response,
                Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csId, &chunkserver));
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csAddr, &chunkserver));

    // 正常情况
    response.set_statuscode(kTopoErrCodeSuccess);
    ChunkServerInfo* csInfo = new ChunkServerInfo();
    GetChunkServerInfoForTest(1, csInfo);
    response.set_allocated_chunkserverinfo(csInfo);
    EXPECT_CALL(*topoService, GetChunkServer(_, _, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                const curve::mds::topology::GetChunkServerInfoRequest *request,
                curve::mds::topology::GetChunkServerInfoResponse *response,
                Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.GetChunkServerInfo(csId, &chunkserver));
    ASSERT_EQ(0, mdsClient.GetChunkServerInfo(csAddr, &chunkserver));
    ChunkServerInfo expected;
    GetChunkServerInfoForTest(1, &expected);
    ASSERT_EQ(expected.DebugString(), chunkserver.DebugString());

    // chunkserver地址不合法的情况
    csAddr = "";
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csAddr, &chunkserver));
    csAddr = "127.0.0.1";
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csAddr, &chunkserver));
    csAddr = "127.0.0.1:%*";
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csAddr, &chunkserver));
}

TEST_F(ToolMDSClientTest, GetCopySetsInChunkServer) {
    ChunkServerIdType csId = 20;
    std::string csAddr = "127.0.0.1:8200";
    std::vector<CopysetInfo> copysets;

    // 发送rpc失败
    EXPECT_CALL(*topoService, GetCopySetsInChunkServer(_, _, _, _))
        .Times(12)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const GetCopySetsInChunkServerRequest *request,
                        GetCopySetsInChunkServerResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csId, &copysets));
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csAddr, &copysets));

    // 返回码不为OK
    GetCopySetsInChunkServerResponse response;
    response.set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    EXPECT_CALL(*topoService, GetCopySetsInChunkServer(_, _, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                const GetCopySetsInChunkServerRequest *request,
                GetCopySetsInChunkServerResponse *response,
                Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csId, &copysets));
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csAddr, &copysets));

    // 正常情况
    response.set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 5; ++i) {
        auto copysetInfo = response.add_copysetinfos();
        copysetInfo->set_logicalpoolid(1);
        copysetInfo->set_copysetid(1000 + i);
    }
    EXPECT_CALL(*topoService, GetCopySetsInChunkServer(_, _, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                const GetCopySetsInChunkServerRequest *request,
                GetCopySetsInChunkServerResponse *response,
                Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.GetCopySetsInChunkServer(csId, &copysets));
    ASSERT_EQ(5, copysets.size());
    copysets.clear();
    ASSERT_EQ(0, mdsClient.GetCopySetsInChunkServer(csAddr, &copysets));
    ASSERT_EQ(5, copysets.size());
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(1, copysets[i].logicalpoolid());
        ASSERT_EQ(1000 + i, copysets[i].copysetid());
    }
    // chunkserver地址不合法的情况
    csAddr = "";
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csAddr, &copysets));
    csAddr = "127.0.0.1";
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csAddr, &copysets));
    csAddr = "127.0.0.1:%*";
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csAddr, &copysets));
}

TEST_F(ToolMDSClientTest, GetCopySetsInCluster) {
    std::vector<CopysetInfo> copysets;

    // 发送rpc失败
    EXPECT_CALL(*topoService, GetCopySetsInCluster(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const GetCopySetsInClusterRequest *request,
                        GetCopySetsInClusterResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.GetCopySetsInCluster(&copysets));

    // 返回码不为OK
    GetCopySetsInClusterResponse response;
    response.set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    EXPECT_CALL(*topoService, GetCopySetsInCluster(_, _, _, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                const GetCopySetsInClusterRequest *request,
                GetCopySetsInClusterResponse *response,
                Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.GetCopySetsInCluster(&copysets));

    // 正常情况
    response.set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 5; ++i) {
        auto copysetInfo = response.add_copysetinfos();
        copysetInfo->set_logicalpoolid(1);
        copysetInfo->set_copysetid(1000 + i);
    }
    EXPECT_CALL(*topoService, GetCopySetsInCluster(_, _, _, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                const GetCopySetsInClusterRequest *request,
                GetCopySetsInClusterResponse *response,
                Closure *done){
                    brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.GetCopySetsInCluster(&copysets));
    ASSERT_EQ(5, copysets.size());
    copysets.clear();
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(1, copysets[i].logicalpoolid());
        ASSERT_EQ(1000 + i, copysets[i].copysetid());
    }
}

TEST_F(ToolMDSClientTest, RapidLeaderSchedule) {
    // 发送rpc失败
    EXPECT_CALL(*scheduleService, RapidLeaderSchedule(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const RapidLeaderScheduleRequst *request,
                        RapidLeaderScheduleResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.RapidLeaderSchedule(1));

    // 返回码不为OK
    RapidLeaderScheduleResponse response;
    response.set_statuscode(
        curve::mds::schedule::kScheduleErrCodeInvalidLogicalPool);
    EXPECT_CALL(*scheduleService, RapidLeaderSchedule(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const RapidLeaderScheduleRequst *request,
                        RapidLeaderScheduleResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.RapidLeaderSchedule(1));

    // 成功
    response.set_statuscode(curve::mds::schedule::kScheduleErrCodeSuccess);
    EXPECT_CALL(*scheduleService, RapidLeaderSchedule(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const RapidLeaderScheduleRequst *request,
                        RapidLeaderScheduleResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.RapidLeaderSchedule(1));
}

TEST_F(ToolMDSClientTest, QueryChunkServerRecoverStatus) {
    std::map<ChunkServerIdType, bool> statusMap;
    // 发送rpc失败
    EXPECT_CALL(*scheduleService, QueryChunkServerRecoverStatus(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const QueryChunkServerRecoverStatusRequest *request,
                        QueryChunkServerRecoverStatusResponse *response,
                        Closure *done) {
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.QueryChunkServerRecoverStatus(
        std::vector<ChunkServerIdType>{}, &statusMap));
    // 1. QueryChunkServerRecoverStatus失败的情况
    QueryChunkServerRecoverStatusResponse response;
    response.set_statuscode(
        curve::mds::schedule::kScheduleErrInvalidQueryChunkserverID);
    EXPECT_CALL(*scheduleService, QueryChunkServerRecoverStatus(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const QueryChunkServerRecoverStatusRequest *request,
                        QueryChunkServerRecoverStatusResponse *response,
                        Closure *done) {
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.QueryChunkServerRecoverStatus(
        std::vector<ChunkServerIdType>{}, &statusMap));

    // 2. QueryChunkServerRecoverStatus成功的情况
    response.set_statuscode(curve::mds::schedule::kScheduleErrCodeSuccess);
    EXPECT_CALL(*scheduleService, QueryChunkServerRecoverStatus(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const QueryChunkServerRecoverStatusRequest *request,
                        QueryChunkServerRecoverStatusResponse *response,
                        Closure *done) {
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.QueryChunkServerRecoverStatus(
        std::vector<ChunkServerIdType>{}, &statusMap));
}

TEST_F(ToolMDSClientTest, GetMetric) {
    std::string metricName = "mds_scheduler_metric_operator_num";
    uint64_t metricValue;
    ASSERT_EQ(-1, mdsClient.GetMetric(metricName, &metricValue));
    std::string metricName2 = "mds_status";
    bvar::Adder<uint32_t> value1;
    value1.expose(metricName);
    bvar::Status<std::string> value2;
    value2.expose(metricName2);
    value1 << 10;
    value2.set_value("leader");
    ASSERT_EQ(0, mdsClient.GetMetric(metricName, &metricValue));
    ASSERT_EQ(10, metricValue);
    std::string metricValue2;
    ASSERT_EQ(0, mdsClient.GetMetric(metricName2, &metricValue2));
    ASSERT_EQ("leader", metricValue2);
}

TEST_F(ToolMDSClientTest, GetCurrentMds) {
    bvar::Status<std::string> value;
    value.expose("mds_status");
    // 有leader
    value.set_value("leader");
    std::vector<std::string> curMds = mdsClient.GetCurrentMds();
    ASSERT_EQ(1, curMds.size());
    ASSERT_EQ("127.0.0.1:9192", curMds[0]);
    // 没有leader
    value.set_value("follower");
    ASSERT_TRUE(mdsClient.GetCurrentMds().empty());
}

TEST_F(ToolMDSClientTest, GetMdsOnlineStatus) {
    bvar::Status<std::string> value;
    value.expose("mds_config_mds_listen_addr");
    std::map<std::string, bool> onlineStatus;
    // 9180在线，9999不在线
    value.set_value("{\"conf_name\":\"mds.listen.addr\","
                        "\"conf_value\":\"127.0.0.1:9192\"}");
    mdsClient.GetMdsOnlineStatus(&onlineStatus);
    std::map<std::string, bool> expected = {{"127.0.0.1:9191", false},
                                            {"127.0.0.1:9192", true}};
    ASSERT_EQ(expected, onlineStatus);
    // 9180的服务端口不一致
    value.set_value("{\"conf_name\":\"mds.listen.addr\","
                        "\"conf_value\":\"127.0.0.1:9188\"}");
    mdsClient.GetMdsOnlineStatus(&onlineStatus);
    expected = {{"127.0.0.1:9191", false}, {"127.0.0.1:9192", false}};
    ASSERT_EQ(expected, onlineStatus);
    // 非json格式
    value.set_value("127.0.0.1::9191");
    mdsClient.GetMdsOnlineStatus(&onlineStatus);
    expected = {{"127.0.0.1:9191", false}, {"127.0.0.1:9192", false}};
    ASSERT_EQ(expected, onlineStatus);
}

TEST_F(ToolMDSClientTest, ListClient) {
    std::vector<std::string> clientAddrs;

    // 发送rpc失败
    EXPECT_CALL(*nameService, ListClient(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const curve::mds::ListClientRequest *request,
                        curve::mds::ListClientResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.ListClient(&clientAddrs));

    // 返回码不为OK
    curve::mds::ListClientResponse response;
    response.set_statuscode(curve::mds::StatusCode::kParaError);
    EXPECT_CALL(*nameService, ListClient(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::ListClientRequest *request,
                        curve::mds::ListClientResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.ListClient(&clientAddrs));

    // 正常情况
    response.set_statuscode(curve::mds::StatusCode::kOK);
    for (int i = 0; i < 5; i++) {
        auto clientInfo = response.add_clientinfos();
        clientInfo->set_ip("127.0.0.1");
        clientInfo->set_port(8888 + i);
    }
    EXPECT_CALL(*nameService, ListClient(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::ListClientRequest *request,
                        curve::mds::ListClientResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.ListClient(&clientAddrs));
    ASSERT_EQ(response.clientinfos_size(), clientAddrs.size());
    for (int i = 0; i < 5; i++) {
        const auto& clientInfo = response.clientinfos(i);
        std::string expected = clientInfo.ip() + ":" +
                               std::to_string(clientInfo.port());
        ASSERT_EQ(expected, clientAddrs[i]);
    }
}

TEST_F(ToolMDSClientTest, ListVolumesOnCopyset) {
    std::vector<common::CopysetInfo> copysets;
    std::vector<std::string> fileNames;

    // send rpc fail
    EXPECT_CALL(*nameService, ListVolumesOnCopysets(_, _, _, _))
        .Times(6)
        .WillRepeatedly(Invoke([](RpcController *controller,
                        const curve::mds::ListVolumesOnCopysetsRequest *request,
                        curve::mds::ListVolumesOnCopysetsResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                        brpc::Controller *cntl =
                            dynamic_cast<brpc::Controller *>(controller);
                        cntl->SetFailed("test");
                    }));
    ASSERT_EQ(-1, mdsClient.ListVolumesOnCopyset(copysets, &fileNames));

    // return code not ok
    curve::mds::ListVolumesOnCopysetsResponse response;
    response.set_statuscode(curve::mds::StatusCode::kParaError);
    EXPECT_CALL(*nameService, ListVolumesOnCopysets(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::ListVolumesOnCopysetsRequest *request,
                        curve::mds::ListVolumesOnCopysetsResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(-1, mdsClient.ListVolumesOnCopyset(copysets, &fileNames));

    // normal
    response.set_statuscode(curve::mds::StatusCode::kOK);
    for (int i = 0; i < 5; i++) {
        auto fileName = response.add_filenames();
        *fileName = "file" + std::to_string(i);
    }
    EXPECT_CALL(*nameService, ListVolumesOnCopysets(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                        Invoke([](RpcController *controller,
                        const curve::mds::ListVolumesOnCopysetsRequest *request,
                        curve::mds::ListVolumesOnCopysetsResponse *response,
                        Closure *done){
                        brpc::ClosureGuard doneGuard(done);
                    })));
    ASSERT_EQ(0, mdsClient.ListVolumesOnCopyset(copysets, &fileNames));
    ASSERT_EQ(response.filenames_size(), fileNames.size());
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(response.filenames(i), fileNames[i]);
    }
}
}  // namespace tool
}  // namespace curve
