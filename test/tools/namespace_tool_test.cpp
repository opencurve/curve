/*
 * Project: curve
 * File Created: 2019-09-29
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include "src/tools/namespace_tool.h"
#include "test/client/fake/mockMDS.h"
#include "test/client/fake/fakeMDS.h"

using curve::mds::topology::GetChunkServerListInCopySetsResponse;
using curve::mds::GetOrAllocateSegmentResponse;

uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 16 * 1024 * 1024;   // NOLINT
std::string metaserver_addr = "127.0.0.1:9180";   // NOLINT
std::string mdsAddr = "127.0.0.1:9999,127.0.0.1:9180";   // NOLINT

DECLARE_string(mds_addr);
DECLARE_uint64(test_disk_size);
DECLARE_bool(isTest);
DECLARE_string(fileName);
DECLARE_uint64(offset);

class NameSpaceToolTest : public ::testing::Test {
 protected:
    NameSpaceToolTest() : fakemds("/test") {}
    void SetUp() {
        FLAGS_test_disk_size = 2 * segment_size;
        FLAGS_isTest = true;
        fakemds.Initialize();
        fakemds.StartService();
    }
    void TearDown() {
        fakemds.UnInitialize();
    }

    void GetFileInfoForTest(FileInfo* fileInfo) {
        fileInfo->set_id(1);
        fileInfo->set_filename("test");
        fileInfo->set_parentid(0);
        fileInfo->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        fileInfo->set_segmentsize(segment_size);
        fileInfo->set_length(FLAGS_test_disk_size);
        fileInfo->set_originalfullpathname("/cinder/test");
        fileInfo->set_ctime(1573546993000000);
    }

    void GetCopysetInfoForTest(curve::mds::topology::CopySetServerInfo* info,
                                int num) {
        for (int i = 0; i < num; ++i) {
            curve::mds::topology::ChunkServerLocation *csLoc =
                                                info->add_cslocs();
            csLoc->set_chunkserverid(i);
            csLoc->set_hostip("127.0.0.1");
            csLoc->set_port(9191 + i);
        }
        info->set_copysetid(1);
    }

    void GetSegmentForTest(PageFileSegment* segment) {
        segment->set_logicalpoolid(1);
        segment->set_segmentsize(segment_size);
        segment->set_chunksize(chunk_size);
        segment->set_startoffset(0);
    }
    FakeMDS fakemds;
};

TEST_F(NameSpaceToolTest, common) {
    curve::tool::NameSpaceTool namespaceTool;
    ASSERT_EQ(-1, namespaceTool.Init(""));
    ASSERT_EQ(-1, namespaceTool.Init("127.0.0.1:9999"));
    ASSERT_EQ(0, namespaceTool.Init(mdsAddr));
    namespaceTool.PrintHelp("abc");
    ASSERT_EQ(-1, namespaceTool.RunCommand("abc"));

    // test list dir
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();

    // 设置ListDir的返回
    std::unique_ptr<curve::mds::ListDirResponse> listdirresponse(
                                    new curve::mds::ListDirResponse());
    listdirresponse->set_statuscode(curve::mds::StatusCode::kOK);
    FileInfo* fileinfoptr = listdirresponse->add_fileinfo();
    fileinfoptr->CopyFrom(fileInfo);
    FileInfo* fileinfoptr2 = listdirresponse->add_fileinfo();
    fileinfoptr2->CopyFrom(fileInfo);
    std::unique_ptr<FakeReturn> fakeListDirRet(
         new FakeReturn(nullptr, static_cast<void*>(listdirresponse.get())));
    fakecurvefsservice->SetListDir(fakeListDirRet.get());

    // test list dir
    namespaceTool.PrintHelp("list");
    FLAGS_fileName = "/";
    ASSERT_EQ(0, namespaceTool.RunCommand("list"));
    FLAGS_fileName = "/test/";
    ASSERT_EQ(0, namespaceTool.RunCommand("list"));

    // test get file
    namespaceTool.PrintHelp("get");
    ASSERT_EQ(0, namespaceTool.RunCommand("get"));

    // test seginfo
    namespaceTool.PrintHelp("seginfo");
    FLAGS_fileName = "/test";
    ASSERT_EQ(0, namespaceTool.RunCommand("seginfo"));

    // test chunk-location
    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<GetChunkServerListInCopySetsResponse> response(
                            new GetChunkServerListInCopySetsResponse());
    std::unique_ptr<FakeReturn> fakeRet(
            new FakeReturn(nullptr, static_cast<void*>(response.get())));
    topology->SetFakeReturn(fakeRet.get());
    curve::mds::topology::CopySetServerInfo csInfo;
    GetCopysetInfoForTest(&csInfo, 3);
    response->set_statuscode(kTopoErrCodeSuccess);
    auto infoPtr = response->add_csinfo();
    infoPtr->CopyFrom(csInfo);
    ASSERT_EQ(0, namespaceTool.RunCommand("chunk-location"));

    // test get dir
    // 设置让GetFileInfo返回目录
    std::unique_ptr<curve::mds::GetFileInfoResponse> getfileinforesponse(
                            new curve::mds::GetFileInfoResponse());
    FileInfo* fileinfoptr3 = new FileInfo();
    fileinfoptr3->set_filetype(curve::mds::FileType::INODE_DIRECTORY);
    fileinfoptr3->set_filename("/");
    getfileinforesponse->set_allocated_fileinfo(fileinfoptr3);
    getfileinforesponse->set_statuscode(::curve::mds::StatusCode::kOK);
    std::unique_ptr<FakeReturn> fakeGetFileInforet(
        new FakeReturn(nullptr,
                    static_cast<void*>(getfileinforesponse.get())));
    fakecurvefsservice->SetGetFileInfoFakeReturn(fakeGetFileInforet.get());
    ASSERT_EQ(0, namespaceTool.RunCommand("get"));

    // test removefile and clean recycle
    std::unique_ptr<curve::mds::DeleteFileResponse> deletefileresponse(
                            new curve::mds::DeleteFileResponse());
    deletefileresponse->set_statuscode(curve::mds::StatusCode::kOK);
    std::unique_ptr<FakeReturn> fakeDeleteFileret(
        new FakeReturn(nullptr, static_cast<void*>(deletefileresponse.get())));
    fakecurvefsservice->SetDeleteFile(fakeDeleteFileret.get());
    namespaceTool.PrintHelp("delete");
    ASSERT_EQ(0, namespaceTool.RunCommand("delete"));
    namespaceTool.PrintHelp("clean-recycle");
    ASSERT_EQ(0, namespaceTool.RunCommand("clean-recycle"));
    namespaceTool.PrintHelp("create");
    ASSERT_EQ(0, namespaceTool.RunCommand("create"));

    // testclean recycle with -fileName
    FLAGS_fileName = "/cinder";
    ASSERT_EQ(0, namespaceTool.RunCommand("clean-recycle"));
}

TEST_F(NameSpaceToolTest, listError) {
    curve::tool::NameSpaceTool namespaceTool;
    ASSERT_EQ(0, namespaceTool.Init(mdsAddr));

    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    brpc::Controller cntl;
    cntl.SetFailed("error for test");

    // statusCode不等于kOK的情况
    std::unique_ptr<curve::mds::ListDirResponse> listdirresponse(
                            new curve::mds::ListDirResponse());
    listdirresponse->set_statuscode(curve::mds::StatusCode::kFileNotExists);
    std::unique_ptr<FakeReturn> fakeListDirRet(
         new FakeReturn(nullptr, static_cast<void*>(listdirresponse.get())));
    fakecurvefsservice->SetListDir(fakeListDirRet.get());
    ASSERT_EQ(-1, namespaceTool.RunCommand("list"));

    // controller failed的情况
    fakeListDirRet->controller_ = &cntl;
    ASSERT_EQ(-1, namespaceTool.RunCommand("list"));

    // list成功，计算文件segment出错的情况
    fakeListDirRet->controller_ = nullptr;
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    listdirresponse->set_statuscode(curve::mds::StatusCode::kOK);
    FileInfo* fileinfoptr = listdirresponse->add_fileinfo();
    fileinfoptr->CopyFrom(fileInfo);
    std::unique_ptr<curve::mds::GetOrAllocateSegmentResponse> segmentresponse(
                new curve::mds::GetOrAllocateSegmentResponse());
    std::unique_ptr<FakeReturn> segmentRet(new FakeReturn(&cntl,
                        static_cast<void*>(segmentresponse.get())));
    fakecurvefsservice->SetGetOrAllocateSegmentFakeReturn(segmentRet.get());
    ASSERT_EQ(-1, namespaceTool.RunCommand("list"));
}

TEST_F(NameSpaceToolTest, seginfoError) {
    curve::tool::NameSpaceTool namespaceTool;
    ASSERT_EQ(0, namespaceTool.Init(mdsAddr));
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    brpc::Controller cntl;
    cntl.SetFailed("error for test");

    // segment不存在的情况
    std::unique_ptr<curve::mds::GetOrAllocateSegmentResponse> segmentresponse(
                new curve::mds::GetOrAllocateSegmentResponse());
    segmentresponse->set_statuscode(curve::mds::StatusCode::kSegmentNotAllocated);   // NOLINT
    std::unique_ptr<FakeReturn> segmentRet(new FakeReturn(nullptr,
                            static_cast<void*>(segmentresponse.get())));
    fakecurvefsservice->SetGetOrAllocateSegmentFakeReturn(segmentRet.get());
    ASSERT_EQ(0, namespaceTool.RunCommand("seginfo"));
    ASSERT_EQ(-1, namespaceTool.RunCommand("chunk-location"));

    // 其他情况
    segmentresponse->set_statuscode(curve::mds::StatusCode::kParaError);
    segmentRet->controller_ = nullptr;
    ASSERT_EQ(-1, namespaceTool.RunCommand("seginfo"));
    ASSERT_EQ(-1, namespaceTool.RunCommand("chunk-location"));

    // controller failed的情况
    segmentRet->controller_ = &cntl;
    ASSERT_EQ(-1, namespaceTool.RunCommand("seginfo"));
    ASSERT_EQ(-1, namespaceTool.RunCommand("chunk-location"));

    // GetFileInfo失败的情况
    std::unique_ptr<curve::mds::GetFileInfoResponse> getfileinforesponse(
                            new curve::mds::GetFileInfoResponse());
    std::unique_ptr<FakeReturn> fakeGetFileInforet(new FakeReturn(&cntl,
                        static_cast<void*>(getfileinforesponse.get())));
    fakecurvefsservice->SetGetFileInfoFakeReturn(fakeGetFileInforet.get());
    ASSERT_EQ(-1, namespaceTool.RunCommand("seginfo"));
    ASSERT_EQ(-1, namespaceTool.RunCommand("chunk-location"));

    // 查询的文件是目录的情况
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    getfileinforesponse->set_statuscode(curve::mds::StatusCode::kOK);
    fakeGetFileInforet->controller_ = nullptr;
    FileInfo* fileinfoptr = new FileInfo();
    GetFileInfoForTest(fileinfoptr);
    fileinfoptr->set_filetype(curve::mds::FileType::INODE_DIRECTORY);
    getfileinforesponse->set_allocated_fileinfo(fileinfoptr);
    ASSERT_EQ(-1, namespaceTool.RunCommand("seginfo"));
    ASSERT_EQ(-1, namespaceTool.RunCommand("chunk-location"));
}

TEST_F(NameSpaceToolTest, getError) {
    curve::tool::NameSpaceTool namespaceTool;
    ASSERT_EQ(0, namespaceTool.Init(mdsAddr));
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    brpc::Controller cntl;
    cntl.SetFailed("error for test");

    // statusCode不等于kOK的情况
    std::unique_ptr<curve::mds::GetFileInfoResponse> getfileinforesponse(
                            new curve::mds::GetFileInfoResponse());
    getfileinforesponse->set_statuscode(curve::mds::StatusCode::kFileNotExists);   // NOLINT
    std::unique_ptr<FakeReturn> fakeGetFileInforet (new FakeReturn(nullptr,
                            static_cast<void*>(getfileinforesponse.get())));
    fakecurvefsservice->SetGetFileInfoFakeReturn(fakeGetFileInforet.get());
    ASSERT_EQ(-1, namespaceTool.RunCommand("get"));

    // controller failed的情况
    fakeGetFileInforet->controller_ = &cntl;
    ASSERT_EQ(-1, namespaceTool.RunCommand("get"));

    // GetFile成功，获取segment出错的情况
    FileInfo* fileinfoptr = new FileInfo();
    GetFileInfoForTest(fileinfoptr);
    getfileinforesponse->set_allocated_fileinfo(fileinfoptr);
    getfileinforesponse->set_statuscode(curve::mds::StatusCode::kOK);
    fakeGetFileInforet->controller_ = nullptr;
    std::unique_ptr<curve::mds::GetOrAllocateSegmentResponse> segmentresponse(
                new curve::mds::GetOrAllocateSegmentResponse());
    std::unique_ptr<FakeReturn> segmentRet(new FakeReturn(&cntl,
                static_cast<void*>(segmentresponse.get())));
    fakecurvefsservice->SetGetOrAllocateSegmentFakeReturn(segmentRet.get());
    ASSERT_EQ(-1, namespaceTool.RunCommand("get"));

    // get目录的时候，listDir错误
    fileinfoptr->set_filetype(curve::mds::FileType::INODE_DIRECTORY);
    std::unique_ptr<curve::mds::ListDirResponse> listdirresponse(
                            new curve::mds::ListDirResponse());
    std::unique_ptr<FakeReturn> fakeListDirRet(
         new FakeReturn(&cntl, static_cast<void*>(listdirresponse.get())));
    fakecurvefsservice->SetListDir(fakeListDirRet.get());
    ASSERT_EQ(-1, namespaceTool.RunCommand("get"));

    // get目录的时候，listDir成功，获取文件segment出错
    listdirresponse->set_statuscode(curve::mds::StatusCode::kOK);
    FileInfo* fileinfoptr2 = listdirresponse->add_fileinfo();
    fileinfoptr2->CopyFrom(fileInfo);
    fakeListDirRet->controller_ = nullptr;
    ASSERT_EQ(-1, namespaceTool.RunCommand("get"));
}

TEST_F(NameSpaceToolTest, deleteError) {
    curve::tool::NameSpaceTool namespaceTool;
    ASSERT_EQ(0, namespaceTool.Init(mdsAddr));
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    brpc::Controller cntl;
    cntl.SetFailed("error for test");

    // statusCode不等于kOK的情况
    std::unique_ptr<curve::mds::DeleteFileResponse> deletefileresponse(
                            new curve::mds::DeleteFileResponse());
    deletefileresponse->set_statuscode(curve::mds::StatusCode::kFileNotExists);
    std::unique_ptr<FakeReturn> fakeDeleteFileret(
        new FakeReturn(nullptr, static_cast<void*>(deletefileresponse.get())));
    fakecurvefsservice->SetDeleteFile(fakeDeleteFileret.get());
    ASSERT_EQ(-1, namespaceTool.RunCommand("delete"));

    // controller failed的情况
    fakeDeleteFileret->controller_ = &cntl;
    ASSERT_EQ(-1, namespaceTool.RunCommand("delete"));

    // 清空回收站的时候，ListFile失败
    std::unique_ptr<curve::mds::ListDirResponse> listdirresponse(
                            new curve::mds::ListDirResponse());
    std::unique_ptr<FakeReturn> fakeListDirRet(
         new FakeReturn(&cntl, static_cast<void*>(listdirresponse.get())));
    fakecurvefsservice->SetListDir(fakeListDirRet.get());
    ASSERT_EQ(-1, namespaceTool.RunCommand("clean-recycle"));
}

TEST_F(NameSpaceToolTest, createError) {
    curve::tool::NameSpaceTool namespaceTool;
    ASSERT_EQ(0, namespaceTool.Init(mdsAddr));
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    brpc::Controller cntl;
    cntl.SetFailed("error for test");

    // statusCode不等于kOK的情况
    std::unique_ptr<curve::mds::CreateFileResponse> createfileresponse(
                            new curve::mds::CreateFileResponse());
    createfileresponse->set_statuscode(curve::mds::StatusCode::kFileNotExists);
    std::unique_ptr<FakeReturn> fakeCreateFileret(
        new FakeReturn(nullptr, static_cast<void*>(createfileresponse.get())));
    fakecurvefsservice->SetCreateFileFakeReturn(fakeCreateFileret.get());
    ASSERT_EQ(-1, namespaceTool.RunCommand("create"));

    // controller failed的情况
    fakeCreateFileret->controller_ = &cntl;
    ASSERT_EQ(-1, namespaceTool.RunCommand("create"));
}

TEST_F(NameSpaceToolTest, testGetChunkLocationErr) {
    curve::tool::NameSpaceTool namespaceTool;
    ASSERT_EQ(0, namespaceTool.Init(mdsAddr));
    // 获取copyset成员失败的情况
    brpc::Controller cntl;
    FakeMDSTopologyService* topology = fakemds.GetTopologyService();
    std::unique_ptr<GetChunkServerListInCopySetsResponse> getCsListResp(
                            new GetChunkServerListInCopySetsResponse());
    std::unique_ptr<FakeReturn> fakeRet(
            new FakeReturn(&cntl, static_cast<void*>(getCsListResp.get())));
    topology->SetFakeReturn(fakeRet.get());

    // 返回值不OK的情况
    getCsListResp->set_statuscode(-1);
    ASSERT_EQ(-1, namespaceTool.RunCommand("chunk-location"));

    // controller failed的情况
    cntl.SetFailed("error for test");
    ASSERT_EQ(-1, namespaceTool.RunCommand("chunk-location"));

    std::unique_ptr<GetOrAllocateSegmentResponse> response(
                            new GetOrAllocateSegmentResponse());
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(nullptr, static_cast<void*>(response.get())));
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    fakecurvefsservice->SetGetOrAllocateSegmentFakeReturn(fakeret.get());
    PageFileSegment* segment = new PageFileSegment();
    GetSegmentForTest(segment);
    response->set_statuscode(::curve::mds::StatusCode::kOK);
    response->set_allocated_pagefilesegment(segment);
    // chunks_size为0的情况
    ASSERT_EQ(-1, namespaceTool.RunCommand("chunk-location"));
}
