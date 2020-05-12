/*
 * Project: curve
 * File Created: 2019-11-26
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <string>
#include "src/tools/mds_client.h"
#include "test/client/fake/mockMDS.h"
#include "test/client/fake/fakeMDS.h"

using curve::mds::topology::LogicalPoolType;
using curve::mds::topology::AllocateStatus;
using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::DiskState;
using curve::mds::topology::OnlineState;
using curve::mds::topology::GetChunkServerListInCopySetsResponse;
using curve::mds::topology::ListPhysicalPoolResponse;
using curve::mds::topology::ListLogicalPoolResponse;
using curve::mds::topology::ListPoolZoneResponse;
using curve::mds::topology::ListChunkServerResponse;
using curve::mds::topology::ListZoneServerResponse;
using curve::mds::topology::ListChunkServerResponse;
using curve::mds::topology::GetChunkServerInfoResponse;
using curve::mds::topology::GetCopySetsInChunkServerResponse;
using curve::mds::GetOrAllocateSegmentResponse;
using curve::tool::GetSegmentRes;
using curve::mds::topology::CopySetServerInfo;

std::string mdsAddr = "127.0.0.1:9999,127.0.0.1:9180";   // NOLINT

DECLARE_uint64(test_disk_size);
extern uint32_t segment_size;
extern uint32_t chunk_size;
extern std::string mdsMetaServerAddr;

namespace brpc {
DECLARE_int32(health_check_interval);
}

class ToolMDSClientTest : public ::testing::Test {
 protected:
    ToolMDSClientTest() : fakemds("/test") {
        FLAGS_test_disk_size = 2 * segment_size;  // NOLINT
        brpc::FLAGS_health_check_interval = -1;
    }
    void SetUp() {
        fakemds.Initialize();
        fakemds.StartService();
    }
    void TearDown() {
        fakemds.UnInitialize();
    }

    void GetFileInfoForTest(uint64_t id, FileInfo* fileInfo) {
        fileInfo->set_id(id);
        fileInfo->set_filename("test");
        fileInfo->set_parentid(0);
        fileInfo->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        fileInfo->set_segmentsize(segment_size);
        fileInfo->set_length(FLAGS_test_disk_size);
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
        segment->set_segmentsize(segment_size);
        segment->set_chunksize(chunk_size);
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

    FakeMDS fakemds;
};

TEST_F(ToolMDSClientTest, Init) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(-1, mdsClient.Init(""));
    ASSERT_EQ(-1, mdsClient.Init("127.0.0.1"));
    ASSERT_EQ(-1, mdsClient.Init("127.0.0.1:65536"));
    // dummy server非法
    ASSERT_EQ(-1, mdsClient.Init(mdsAddr, ""));
    // dummy server与mds不匹配
    ASSERT_EQ(-1, mdsClient.Init(mdsAddr, "9091,9092,9093"));
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));
}

TEST_F(ToolMDSClientTest, GetFileInfo) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));
    mdsClient.SetUserName("root");
    mdsClient.SetPassword("root_password");
    FakeMDSCurveFSService* curvefsservice = fakemds.GetMDSService();
    std::string filename = "/test";
    std::unique_ptr<curve::mds::GetFileInfoResponse> response(
                            new curve::mds::GetFileInfoResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    curvefsservice->SetGetFileInfoFakeReturn(fakeret.get());
    curve::mds::FileInfo outFileInfo;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.GetFileInfo(filename, nullptr));

    // 发送RPC失败
    cntl.SetFailed("fail for test");
    ASSERT_EQ(-1, mdsClient.GetFileInfo(filename, &outFileInfo));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::StatusCode::kParaError);
    ASSERT_EQ(-1, mdsClient.GetFileInfo(filename, &outFileInfo));

    // 正常情况
    curve::mds::FileInfo * info = new curve::mds::FileInfo;
    GetFileInfoForTest(1, info);
    response->set_allocated_fileinfo(info);
    response->set_statuscode(curve::mds::StatusCode::kOK);
    ASSERT_EQ(0, mdsClient.GetFileInfo(filename, &outFileInfo));
    ASSERT_EQ(info->DebugString(), outFileInfo.DebugString());
}

TEST_F(ToolMDSClientTest, GetAllocatedSize) {
    uint64_t allocSize;
    std::string filename = "/test";
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));
    std::unique_ptr<curve::mds::GetAllocatedSizeResponse> response(
                            new curve::mds::GetAllocatedSizeResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    FakeMDSCurveFSService* curvefsservice = fakemds.GetMDSService();
    curvefsservice->SetGetAllocatedSizeReturn(fakeret.get());
    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.GetAllocatedSize(filename, nullptr));

    // 发送RPC失败
    cntl.SetFailed("fail for test");
    ASSERT_EQ(-1, mdsClient.GetAllocatedSize(filename, &allocSize));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::StatusCode::kParaError);
    ASSERT_EQ(-1, mdsClient.GetAllocatedSize(filename, &allocSize));

    // 正常情况
    uint64_t expectedSize1 = 1073741824;
    uint64_t expectedSize2 = allocSize * 3;
    response->set_allocatedsize(expectedSize1);
    response->set_physicalallocatedsize(expectedSize2);

    response->set_statuscode(curve::mds::StatusCode::kOK);
    uint64_t physicAllocSize;
    ASSERT_EQ(0, mdsClient.GetAllocatedSize(filename, &allocSize,
                                            &physicAllocSize));
    ASSERT_EQ(expectedSize1, allocSize);
    ASSERT_EQ(expectedSize2, physicAllocSize);
}

TEST_F(ToolMDSClientTest, ListDir) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));
    FakeMDSCurveFSService* curvefsservice = fakemds.GetMDSService();
    std::string fileName = "/test";
    std::unique_ptr<curve::mds::ListDirResponse> response(
                            new curve::mds::ListDirResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    curvefsservice->SetListDir(fakeret.get());
    std::vector<FileInfo> fileInfoVec;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.ListDir(fileName, nullptr));

    // 发送RPC失败
    cntl.SetFailed("fail for test");
    ASSERT_EQ(-1, mdsClient.ListDir(fileName, &fileInfoVec));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::StatusCode::kParaError);
    ASSERT_EQ(-1, mdsClient.ListDir(fileName, &fileInfoVec));

    // 正常情况
    response->set_statuscode(curve::mds::StatusCode::kOK);
    for (int i = 0; i < 5; i++) {
        auto fileInfo = response->add_fileinfo();
        GetFileInfoForTest(i, fileInfo);
    }
    ASSERT_EQ(0, mdsClient.ListDir(fileName, &fileInfoVec));
    for (int i = 0; i < 5; i++) {
        FileInfo expected;
        GetFileInfoForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), fileInfoVec[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, GetSegmentInfo) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));
    FakeMDSCurveFSService* curvefsservice = fakemds.GetMDSService();
    std::string filename = "/test";
    uint64_t offset = 0;
    std::unique_ptr<curve::mds::GetOrAllocateSegmentResponse> response(
                            new curve::mds::GetOrAllocateSegmentResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    curvefsservice->SetGetOrAllocateSegmentFakeReturn(fakeret.get());
    curve::mds::PageFileSegment outSegment;

    // 参数为空指针
    ASSERT_EQ(GetSegmentRes::kOtherError,
                    mdsClient.GetSegmentInfo(filename, offset, nullptr));

    // 发送RPC失败
    cntl.SetFailed("fail for test");
    ASSERT_EQ(GetSegmentRes::kOtherError,
                    mdsClient.GetSegmentInfo(filename, offset, &outSegment));
    cntl.Reset();

    // segment不存在
    response->set_statuscode(curve::mds::StatusCode::kSegmentNotAllocated);
    ASSERT_EQ(GetSegmentRes::kSegmentNotAllocated,
                    mdsClient.GetSegmentInfo(filename, offset, &outSegment));

    // 文件不存在
    response->set_statuscode(curve::mds::StatusCode::kFileNotExists);
    ASSERT_EQ(GetSegmentRes::kFileNotExists,
                    mdsClient.GetSegmentInfo(filename, offset, &outSegment));

    // 其他错误
    response->set_statuscode(curve::mds::StatusCode::kParaError);
    ASSERT_EQ(GetSegmentRes::kOtherError,
                    mdsClient.GetSegmentInfo(filename, offset, &outSegment));

    // 正常情况
    PageFileSegment* segment = new PageFileSegment();
    GetSegmentForTest(segment);
    response->set_statuscode(curve::mds::StatusCode::kOK);
    response->set_allocated_pagefilesegment(segment);
    ASSERT_EQ(GetSegmentRes::kOK,
                    mdsClient.GetSegmentInfo(filename, offset, &outSegment));
    ASSERT_EQ(segment->DebugString(), outSegment.DebugString());
}

TEST_F(ToolMDSClientTest, DeleteFile) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));
    FakeMDSCurveFSService* curvefsservice = fakemds.GetMDSService();
    std::string fileName = "/test";
    std::unique_ptr<curve::mds::DeleteFileResponse> response(
                            new curve::mds::DeleteFileResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    curvefsservice->SetDeleteFile(fakeret.get());

    // 发送RPC失败
    cntl.SetFailed("fail for test");
    ASSERT_EQ(-1, mdsClient.DeleteFile(fileName));
    cntl.Reset();

    // 返回码不为OK
    fakeret->controller_ = nullptr;
    response->set_statuscode(curve::mds::StatusCode::kParaError);
    ASSERT_EQ(-1, mdsClient.DeleteFile(fileName));

    // 正常情况
    response->set_statuscode(curve::mds::StatusCode::kOK);
    ASSERT_EQ(0, mdsClient.DeleteFile(fileName));
}

TEST_F(ToolMDSClientTest, CreateFile) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));
    FakeMDSCurveFSService* curvefsservice = fakemds.GetMDSService();
    std::string fileName = "/test";
    uint64_t length = FLAGS_test_disk_size;
    std::unique_ptr<curve::mds::CreateFileResponse> response(
                            new curve::mds::CreateFileResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    curvefsservice->SetCreateFileFakeReturn(fakeret.get());

    // 发送RPC失败
    cntl.SetFailed("fail for test");
    ASSERT_EQ(-1, mdsClient.CreateFile(fileName, length));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::StatusCode::kParaError);
    ASSERT_EQ(-1, mdsClient.CreateFile(fileName, length));

    // 正常情况
    response->set_statuscode(curve::mds::StatusCode::kOK);
    ASSERT_EQ(0, mdsClient.CreateFile(fileName, length));
}

TEST_F(ToolMDSClientTest, GetChunkServerListInCopySets) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));

    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    PoolIdType logicalPoolId = 1;
    CopySetIdType copysetId = 100;
    std::unique_ptr<GetChunkServerListInCopySetsResponse> response(
                    new GetChunkServerListInCopySetsResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    topology->SetFakeReturn(fakeret.get());
    std::vector<ChunkServerLocation> csLocs;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.GetChunkServerListInCopySet(
                                logicalPoolId, copysetId, nullptr));

    // 发送rpc失败
    cntl.SetFailed("error for test");
    ASSERT_EQ(-1, mdsClient.GetChunkServerListInCopySet(
                                logicalPoolId, copysetId, &csLocs));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.GetChunkServerListInCopySet(
                                logicalPoolId, copysetId, &csLocs));

    // 正常情况
    response->set_statuscode(kTopoErrCodeSuccess);
    CopySetServerInfo csInfo;
    GetCopysetInfoForTest(&csInfo, 3, copysetId);
    auto infoPtr = response->add_csinfo();
    infoPtr->CopyFrom(csInfo);
    ASSERT_EQ(0, mdsClient.GetChunkServerListInCopySet(
                                logicalPoolId, copysetId, &csLocs));
    ASSERT_EQ(csInfo.cslocs_size(), csLocs.size());
    for (uint32_t i = 0; i < csLocs.size(); ++i) {
        ASSERT_EQ(csInfo.cslocs(i).DebugString(), csLocs[i].DebugString());
    }

    // 测试获取多个copyset
    std::vector<CopySetServerInfo> expected;
    response->Clear();
    response->set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; ++i) {
        CopySetServerInfo csInfo;
        GetCopysetInfoForTest(&csInfo, 3, 100 + i);
        auto infoPtr = response->add_csinfo();
        infoPtr->CopyFrom(csInfo);
        expected.emplace_back(csInfo);
    }
    std::vector<CopySetIdType> copysets = {100, 101, 102};
    std::vector<CopySetServerInfo> csServerInfos;
    ASSERT_EQ(-1, mdsClient.GetChunkServerListInCopySets(
                                logicalPoolId, copysets, nullptr));
    ASSERT_EQ(0, mdsClient.GetChunkServerListInCopySets(
                                logicalPoolId, copysets, &csServerInfos));
    ASSERT_EQ(expected.size(), csServerInfos.size());
    for (uint32_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(expected[i].DebugString(), csServerInfos[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, ListPhysicalPoolsInCluster) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));

    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<ListPhysicalPoolResponse> response(
                    new ListPhysicalPoolResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    topology->fakelistpoolret_ = fakeret.get();
    std::vector<PhysicalPoolInfo> pools;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.ListPhysicalPoolsInCluster(nullptr));

    // 发送rpc失败
    cntl.SetFailed("error for test");
    ASSERT_EQ(-1, mdsClient.ListPhysicalPoolsInCluster(&pools));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.ListPhysicalPoolsInCluster(&pools));

    // 正常情况
    response->set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; i++) {
        auto poolInfo = response->add_physicalpoolinfos();
        GetPhysicalPoolInfoForTest(i, poolInfo);
    }
    ASSERT_EQ(0, mdsClient.ListPhysicalPoolsInCluster(&pools));
    ASSERT_EQ(3, pools.size());
    for (int i = 0; i < 3; ++i) {
        PhysicalPoolInfo expected;
        GetPhysicalPoolInfoForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), pools[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, ListLogicalPoolsInPhysicalPool) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));

    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<ListLogicalPoolResponse> response(
                    new ListLogicalPoolResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    topology->fakelistlogicalpoolret_ = fakeret.get();
    PoolIdType poolId = 1;
    std::vector<LogicalPoolInfo> pools;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.ListLogicalPoolsInPhysicalPool(poolId, nullptr));

    // 发送rpc失败
    cntl.SetFailed("error for test");
    ASSERT_EQ(-1, mdsClient.ListLogicalPoolsInPhysicalPool(poolId, &pools));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.ListLogicalPoolsInPhysicalPool(poolId, &pools));

    // 正常情况
    response->set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; i++) {
        auto poolInfo = response->add_logicalpoolinfos();
        GetLogicalPoolForTest(i, poolInfo);
    }
    ASSERT_EQ(0, mdsClient.ListLogicalPoolsInPhysicalPool(poolId, &pools));
    ASSERT_EQ(3, pools.size());
    for (int i = 0; i < 3; ++i) {
        LogicalPoolInfo expected;
        GetLogicalPoolForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), pools[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, ListZoneInPhysicalPool) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));

    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<ListPoolZoneResponse> response(
                    new ListPoolZoneResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    topology->fakelistzoneret_ = fakeret.get();
    PoolIdType poolId = 1;
    std::vector<ZoneInfo> zones;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.ListZoneInPhysicalPool(poolId, nullptr));

    // 发送rpc失败
    cntl.SetFailed("error for test");
    ASSERT_EQ(-1, mdsClient.ListZoneInPhysicalPool(poolId, &zones));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.ListZoneInPhysicalPool(poolId, &zones));

    // 正常情况
    response->set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; i++) {
        auto zoneInfo = response->add_zones();
        GetZoneInfoForTest(i, zoneInfo);
    }
    ASSERT_EQ(0, mdsClient.ListZoneInPhysicalPool(poolId, &zones));
    ASSERT_EQ(3, zones.size());
    for (int i = 0; i < 3; ++i) {
        ZoneInfo expected;
        GetZoneInfoForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), zones[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, ListServersInZone) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));

    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<ListZoneServerResponse> response(
                    new ListZoneServerResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    topology->fakelistserverret_ = fakeret.get();
    ZoneIdType zoneId;
    std::vector<ServerInfo> servers;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.ListServersInZone(zoneId, nullptr));

    // 发送rpc失败
    cntl.SetFailed("error for test");
    ASSERT_EQ(-1, mdsClient.ListServersInZone(zoneId, &servers));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.ListServersInZone(zoneId, &servers));

    // 正常情况
    response->set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; i++) {
        auto serverInfo = response->add_serverinfo();
        GetServerInfoForTest(i, serverInfo);
    }
    ASSERT_EQ(0, mdsClient.ListServersInZone(zoneId, &servers));
    ASSERT_EQ(3, servers.size());
    for (int i = 0; i < 3; ++i) {
        ServerInfo expected;
        GetServerInfoForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), servers[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, ListChunkServersOnServer) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));

    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<ListChunkServerResponse> response(
                    new ListChunkServerResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    topology->SetFakeReturn(fakeret.get());
    ServerIdType serverId = 1;
    std::vector<ChunkServerInfo> chunkservers;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.ListChunkServersOnServer(serverId, nullptr));

    // 发送rpc失败
    cntl.SetFailed("error for test");
    ASSERT_EQ(-1, mdsClient.ListChunkServersOnServer(serverId, &chunkservers));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.ListChunkServersOnServer(serverId, &chunkservers));

    // 正常情况,两个chunkserver正常，一个chunkserver retired
    response->set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 3; i++) {
        auto csInfo = response->add_chunkserverinfos();
        GetChunkServerInfoForTest(i, csInfo, i == 2);
    }
    ASSERT_EQ(0, mdsClient.ListChunkServersOnServer(serverId, &chunkservers));
    ASSERT_EQ(2, chunkservers.size());
    for (int i = 0; i < 2; ++i) {
        ChunkServerInfo expected;
        GetChunkServerInfoForTest(i, &expected);
        ASSERT_EQ(expected.DebugString(), chunkservers[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, GetChunkServerInfo) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));

    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<GetChunkServerInfoResponse> response(
                    new GetChunkServerInfoResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    topology->SetFakeReturn(fakeret.get());
    ChunkServerIdType csId = 20;
    std::string csAddr = "127.0.0.1:8200";
    ChunkServerInfo chunkserver;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csId, nullptr));
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csAddr, nullptr));

    // 发送rpc失败
    cntl.SetFailed("error for test");
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csId, &chunkserver));
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csAddr, &chunkserver));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csId, &chunkserver));
    ASSERT_EQ(-1, mdsClient.GetChunkServerInfo(csAddr, &chunkserver));

    // 正常情况
    response->set_statuscode(kTopoErrCodeSuccess);
    ChunkServerInfo* csInfo = new ChunkServerInfo();
    GetChunkServerInfoForTest(1, csInfo);
    response->set_allocated_chunkserverinfo(csInfo);
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
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));

    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<GetCopySetsInChunkServerResponse> response(
                    new GetCopySetsInChunkServerResponse());
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    topology->fakegetcopysetincsret_ = fakeret.get();
    ChunkServerIdType csId = 20;
    std::string csAddr = "127.0.0.1:8200";
    std::vector<CopysetInfo> copysets;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csId, nullptr));
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csAddr, nullptr));

    // 发送rpc失败
    cntl.SetFailed("error for test");
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csId, &copysets));
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csAddr, &copysets));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csId, &copysets));
    ASSERT_EQ(-1, mdsClient.GetCopySetsInChunkServer(csAddr, &copysets));

    // 正常情况
    response->set_statuscode(kTopoErrCodeSuccess);
    for (int i = 0; i < 5; ++i) {
        auto copysetInfo = response->add_copysetinfos();
        copysetInfo->set_logicalpoolid(1);
        copysetInfo->set_copysetid(1000 + i);
    }
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

TEST_F(ToolMDSClientTest, GetServerOrChunkserverInCluster) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));
    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::vector<ServerInfo> servers;
    std::vector<ChunkServerInfo> chunkservers;

    // 设置ListPhysicalPool的返回
    std::unique_ptr<ListPhysicalPoolResponse> response1(
                                    new ListPhysicalPoolResponse());
    std::unique_ptr<FakeReturn> fakelistpoolret(
         new FakeReturn(nullptr, static_cast<void*>(response1.get())));
    topology->fakelistpoolret_ = fakelistpoolret.get();
    // 设置ListPoolZone的返回
    std::unique_ptr<ListPoolZoneResponse> response2(
                        new ListPoolZoneResponse());
    std::unique_ptr<FakeReturn>  fakelistzoneret(
         new FakeReturn(nullptr, static_cast<void*>(response2.get())));
    topology->fakelistzoneret_ = fakelistzoneret.get();
    // 设置ListZoneServer的返回
    std::unique_ptr<ListZoneServerResponse> response3(
                    new ListZoneServerResponse());
    std::unique_ptr<FakeReturn> fakelistserverret(
         new FakeReturn(nullptr, static_cast<void*>(response3.get())));
    topology->fakelistserverret_ = fakelistserverret.get();
    // 设置ListChunkserver的返回
    std::unique_ptr<ListChunkServerResponse> response4(
                            new ListChunkServerResponse());
    std::unique_ptr<FakeReturn> fakeRet(new FakeReturn(
                nullptr, static_cast<void*>(response4.get())));
    topology->SetFakeReturn(fakeRet.get());

    // 1、ListPhysicalPool失败的情况
    response1->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.ListServersInCluster(&servers));
    ASSERT_EQ(-1, mdsClient.ListChunkServersInCluster(&chunkservers));

    // 2、ListPoolZone失败的情况
    response1->set_statuscode(curve::mds::topology::kTopoErrCodeSuccess);
    auto pool = response1->add_physicalpoolinfos();
    pool->set_physicalpoolid(1);
    pool->set_physicalpoolname("testPool");
    response2->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.ListServersInCluster(&servers));
    ASSERT_EQ(-1, mdsClient.ListChunkServersInCluster(&chunkservers));

    // 3、ListZoneServer失败的的情况
    response2->set_statuscode(kTopoErrCodeSuccess);
    auto zone = response2->add_zones();
    GetZoneInfoForTest(1, zone);
    response3->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.ListServersInCluster(&servers));
    ASSERT_EQ(-1, mdsClient.ListChunkServersInCluster(&chunkservers));

    // 4、ListChunkserver失败的情况
    response3->set_statuscode(kTopoErrCodeSuccess);
    auto server = response3->add_serverinfo();
    GetServerInfoForTest(1, server);
    response4->set_statuscode(curve::mds::topology::kTopoErrCodeInitFail);
    ASSERT_EQ(-1, mdsClient.ListChunkServersInCluster(&chunkservers));

    // 5、正常情况，有一个chunkserverretired
    response4->set_statuscode(curve::mds::topology::kTopoErrCodeSuccess);
    for (int i = 0; i < 3; ++i) {
        auto chunkserver = response4->add_chunkserverinfos();
        GetChunkServerInfoForTest(i, chunkserver, i == 2);
    }
    ASSERT_EQ(0, mdsClient.ListServersInCluster(&servers));
    ASSERT_EQ(1, servers.size());
    ServerInfo expected;
    GetServerInfoForTest(1, &expected);
    ASSERT_EQ(expected.DebugString(), servers[0].DebugString());
    ASSERT_EQ(0, mdsClient.ListChunkServersInCluster(&chunkservers));
    ASSERT_EQ(2, chunkservers.size());
    for (int i = 0; i < 2; ++i) {
        ChunkServerInfo expected2;
        GetChunkServerInfoForTest(i, &expected2);
        ASSERT_EQ(expected2.DebugString(), chunkservers[i].DebugString());
    }
}

TEST_F(ToolMDSClientTest, RapidLeaderSchedule) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));

    FakeScheduleService* schedule = fakemds.GetScheduleService();

    // 设置QueryChunkServerRecoverStatus的返回
    std::unique_ptr<RapidLeaderScheduleResponse> r(
        new RapidLeaderScheduleResponse);
    std::unique_ptr<FakeReturn> fakeRet(
         new FakeReturn(nullptr, static_cast<void*>(r.get())));

    std::map<ChunkServerIdType, bool> statusMap;
    // 1. QueryChunkServerRecoverStatus失败的情况
    r->set_statuscode(curve::mds::schedule::kScheduleErrCodeInvalidLogicalPool);
    schedule->SetFakeReturn(fakeRet.get());
    ASSERT_EQ(-1, mdsClient.RapidLeaderSchedule(1));

    // 2. QueryChunkServerRecoverStatus成功的情况
    r->set_statuscode(curve::mds::schedule::kScheduleErrCodeSuccess);
    schedule->SetFakeReturn(fakeRet.get());
    ASSERT_EQ(0, mdsClient.RapidLeaderSchedule(1));
}

TEST_F(ToolMDSClientTest, QueryChunkServerRecoverStatus) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));

    FakeScheduleService* schedule = fakemds.GetScheduleService();

    // 设置QueryChunkServerRecoverStatus的返回
    std::unique_ptr<QueryChunkServerRecoverStatusResponse> r(
        new QueryChunkServerRecoverStatusResponse);
    std::unique_ptr<FakeReturn> fakeRet(
         new FakeReturn(nullptr, static_cast<void*>(r.get())));

    std::map<ChunkServerIdType, bool> statusMap;
    // 1. QueryChunkServerRecoverStatus失败的情况
    r->set_statuscode(
        curve::mds::schedule::kScheduleErrInvalidQueryChunkserverID);
    schedule->SetFakeReturn(fakeRet.get());
    ASSERT_EQ(-1, mdsClient.QueryChunkServerRecoverStatus(
        std::vector<ChunkServerIdType>{}, &statusMap));

    // 2. QueryChunkServerRecoverStatus成功的情况
    r->set_statuscode(curve::mds::schedule::kScheduleErrCodeSuccess);
    schedule->SetFakeReturn(fakeRet.get());
    ASSERT_EQ(0, mdsClient.QueryChunkServerRecoverStatus(
        std::vector<ChunkServerIdType>{}, &statusMap));
}

TEST_F(ToolMDSClientTest, GetMetric) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr));
    std::string metricName = "mds_scheduler_metric_operator_num";
    uint64_t metricValue;
    ASSERT_EQ(-1, mdsClient.GetMetric(metricName, &metricValue));
    std::string metricName2 = "mds_status";
    std::unique_ptr<bvar::Adder<uint32_t>> value1(new bvar::Adder<uint32_t>());
    std::unique_ptr<bvar::Status<std::string>>
        value2(new bvar::Status<std::string>());
    (*value1) << 10;
    value2->set_value("leader");
    fakemds.SetMetric(metricName, value1.get());
    fakemds.SetMetric(metricName2, value2.get());
    fakemds.ExposeMetric();
    ASSERT_EQ(0, mdsClient.GetMetric(metricName, &metricValue));
    ASSERT_EQ(10, metricValue);
    std::string metricValue2;
    ASSERT_EQ(0, mdsClient.GetMetric(metricName2, &metricValue2));
    ASSERT_EQ("leader", metricValue2);
}

TEST_F(ToolMDSClientTest, GetCurrentMds) {
    std::unique_ptr<bvar::Status<std::string>>
        value(new bvar::Status<std::string>());
    fakemds.SetMetric("mds_status", value.get());
    fakemds.ExposeMetric();
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr, "9999,9180"));
    // 有leader
    value->set_value("leader");
    std::vector<std::string> curMds = mdsClient.GetCurrentMds();
    ASSERT_EQ(1, curMds.size());
    ASSERT_EQ("127.0.0.1:9180", curMds[0]);
    // 没有leader
    value->set_value("follower");
    ASSERT_TRUE(mdsClient.GetCurrentMds().empty());
}

TEST_F(ToolMDSClientTest, GetMdsOnlineStatus) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr, "9999,9180"));
    std::unique_ptr<bvar::Status<std::string>>
        value(new bvar::Status<std::string>());
    fakemds.SetMetric("mds_config_mds_listen_addr", value.get());
    fakemds.ExposeMetric();
    std::map<std::string, bool> onlineStatus;
    // 9180在线，9999不在线
    value->set_value("{\"conf_name\":\"mds.listen.addr\","
                        "\"conf_value\":\"127.0.0.1:9180\"}");
    mdsClient.GetMdsOnlineStatus(&onlineStatus);
    std::map<std::string, bool> expected = {{"127.0.0.1:9180", true},
                                            {"127.0.0.1:9999", false}};
    ASSERT_EQ(expected, onlineStatus);
    // 9180的服务端口不一致
    value->set_value("{\"conf_name\":\"mds.listen.addr\","
                        "\"conf_value\":\"127.0.0.1:9188\"}");
    mdsClient.GetMdsOnlineStatus(&onlineStatus);
    expected = {{"127.0.0.1:9180", false}, {"127.0.0.1:9999", false}};
    ASSERT_EQ(expected, onlineStatus);
    // 非json格式
    value->set_value("127.0.0.1::9180");
    mdsClient.GetMdsOnlineStatus(&onlineStatus);
    expected = {{"127.0.0.1:9180", false}, {"127.0.0.1:9999", false}};
    ASSERT_EQ(expected, onlineStatus);
}

TEST_F(ToolMDSClientTest, ListClient) {
    curve::tool::MDSClient mdsClient;
    ASSERT_EQ(0, mdsClient.Init(mdsAddr, "9999,9180"));
    FakeMDSCurveFSService* curvefsservice = fakemds.GetMDSService();
    std::string filename = "/test";
    std::unique_ptr<curve::mds::ListClientResponse> response(
                            new curve::mds::ListClientResponse());

    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    curvefsservice->SetListClient(fakeret.get());
    std::vector<std::string> clientAddrs;

    // 参数为空指针
    ASSERT_EQ(-1, mdsClient.ListClient(nullptr));

    // 发送RPC失败
    cntl.SetFailed("fail for test");
    ASSERT_EQ(-1, mdsClient.ListClient(&clientAddrs));
    cntl.Reset();

    // 返回码不为OK
    response->set_statuscode(curve::mds::StatusCode::kParaError);
    ASSERT_EQ(-1, mdsClient.ListClient(&clientAddrs));

    // 正常情况
    response->set_statuscode(curve::mds::StatusCode::kOK);
    for (int i = 0; i < 5; i++) {
        auto clientInfo = response->add_clientinfos();
        clientInfo->set_ip("127.0.0.1");
        clientInfo->set_port(8888 + i);
    }
    ASSERT_EQ(0, mdsClient.ListClient(&clientAddrs));
    ASSERT_EQ(response->clientinfos_size(), clientAddrs.size());
    for (int i = 0; i < 5; i++) {
        const auto& clientInfo = response->clientinfos(i);
        std::string expected = clientInfo.ip() + ":" +
                               std::to_string(clientInfo.port());
        ASSERT_EQ(expected, clientAddrs[i]);
    }
}
