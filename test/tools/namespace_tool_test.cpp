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

uint32_t segment_size = 1 << 30;   // NOLINT
uint32_t chunk_size = 1 << 29;   // NOLINT
std::string metaserver_addr = "127.0.0.1:9180";   // NOLINT

DECLARE_string(mds_addr);
DECLARE_uint64(test_disk_size);
DECLARE_bool(isTest);

class NameSpaceToolTest : public ::testing::Test {
 protected:
    NameSpaceToolTest() : fakemds("/test") {}
    void SetUp() {
        FLAGS_mds_addr = "127.0.0.1:9999,127.0.0.1:9180";
        FLAGS_test_disk_size = 1ull << 32;
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
        fileInfo->set_segmentsize(1 << 30);
        fileInfo->set_length(1ull << 32);
    }
    FakeMDS fakemds;
};

TEST_F(NameSpaceToolTest, common) {
    curve::tool::NameSpaceTool namespaceTool;
    namespaceTool.Init();

    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();

    // 设置ListDir的返回
    curve::mds::ListDirResponse* listdirresponse =
                            new curve::mds::ListDirResponse();
    listdirresponse->set_statuscode(curve::mds::StatusCode::kOK);
    FileInfo *fileinfoptr = listdirresponse->add_fileinfo();
    fileinfoptr->CopyFrom(fileInfo);
    FakeReturn* fakeListDirRet =
         new FakeReturn(nullptr, static_cast<void*>(listdirresponse));
    fakecurvefsservice->SetListDir(fakeListDirRet);

    // test list dir
    ASSERT_EQ(namespaceTool.RunCommand("list"), 0);

    // test seginfo
    ASSERT_EQ(namespaceTool.RunCommand("seginfo"), 0);

    // test get file
    ASSERT_EQ(namespaceTool.RunCommand("get"), 0);

    // test get dir
    // 设置让GetFileInfo返回目录
    curve::mds::GetFileInfoResponse* getfileinforesponse =
                            new curve::mds::GetFileInfoResponse();
    fileInfo.set_filetype(curve::mds::FileType::INODE_DIRECTORY);
    getfileinforesponse->set_allocated_fileinfo(&fileInfo);
    getfileinforesponse->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeGetFileInforet =
            new FakeReturn(nullptr, static_cast<void*>(getfileinforesponse));
    fakecurvefsservice->SetGetFileInfoFakeReturn(fakeGetFileInforet);
    ASSERT_EQ(namespaceTool.RunCommand("get"), 0);

    // test removefile and clean recycle
    curve::mds::DeleteFileResponse* deletefileresponse =
                            new curve::mds::DeleteFileResponse();
    deletefileresponse->set_statuscode(curve::mds::StatusCode::kOK);
    FakeReturn* fakeDeleteFileret =
            new FakeReturn(nullptr, static_cast<void*>(deletefileresponse));
    fakecurvefsservice->SetDeleteFile(fakeDeleteFileret);
    ASSERT_EQ(namespaceTool.RunCommand("delete"), 0);
    ASSERT_EQ(namespaceTool.RunCommand("clean-recycle"), 0);
    ASSERT_EQ(namespaceTool.RunCommand("create"), 0);
}

TEST_F(NameSpaceToolTest, listError) {
    curve::tool::NameSpaceTool namespaceTool;
    namespaceTool.Init();

    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    brpc::Controller cntl;
    cntl.SetFailed("error for test");

    // statusCode不等于kOK的情况
    curve::mds::ListDirResponse* listdirresponse =
                            new curve::mds::ListDirResponse();
    listdirresponse->set_statuscode(curve::mds::StatusCode::kFileNotExists);
    FakeReturn* fakeListDirRet =
         new FakeReturn(&cntl, listdirresponse);
    fakecurvefsservice->SetListDir(fakeListDirRet);
    ASSERT_EQ(namespaceTool.RunCommand("list"), -1);

    // controller failed的情况
    fakeListDirRet->controller_ = &cntl;
    fakeListDirRet->response_ = listdirresponse;
    fakecurvefsservice->SetListDir(fakeListDirRet);
    ASSERT_EQ(namespaceTool.RunCommand("list"), -1);
}

TEST_F(NameSpaceToolTest, seginfoError) {
    curve::tool::NameSpaceTool namespaceTool;
    namespaceTool.Init();
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    brpc::Controller cntl;
    cntl.SetFailed("error for test");

    // segment不存在的情况
    curve::mds::GetOrAllocateSegmentResponse* segmentresponse =
                new curve::mds::GetOrAllocateSegmentResponse();
    segmentresponse->set_statuscode(curve::mds::StatusCode::kSegmentNotAllocated);   // NOLINT
    FakeReturn* segmentRet = new FakeReturn(nullptr, segmentresponse);
    fakecurvefsservice->SetGetOrAllocateSegmentFakeReturn(segmentRet);
    ASSERT_EQ(namespaceTool.RunCommand("seginfo"), 0);

    // 其他情况
    segmentresponse->set_statuscode(curve::mds::StatusCode::kParaError);
    segmentRet->controller_ = nullptr;
    segmentRet->response_ = segmentresponse;
    fakecurvefsservice->SetGetOrAllocateSegmentFakeReturn(segmentRet);
    ASSERT_EQ(namespaceTool.RunCommand("seginfo"), -1);

    // controller failed的情况
    segmentRet->controller_ = &cntl;
    segmentRet->response_ = segmentresponse;
    fakecurvefsservice->SetGetOrAllocateSegmentFakeReturn(segmentRet);
    ASSERT_EQ(namespaceTool.RunCommand("seginfo"), -1);
}

TEST_F(NameSpaceToolTest, getError) {
    curve::tool::NameSpaceTool namespaceTool;
    namespaceTool.Init();
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    brpc::Controller cntl;
    cntl.SetFailed("error for test");

    // statusCode不等于kOK的情况
    curve::mds::GetFileInfoResponse* getfileinforesponse =
                            new curve::mds::GetFileInfoResponse();
    getfileinforesponse->set_statuscode(curve::mds::StatusCode::kFileNotExists);   // NOLINT
    FakeReturn* fakeGetFileInforet = new FakeReturn(&cntl, getfileinforesponse);
    fakecurvefsservice->SetGetFileInfoFakeReturn(fakeGetFileInforet);
    ASSERT_EQ(namespaceTool.RunCommand("get"), -1);

    // GetFile成功，获取segment出错的情况
    getfileinforesponse->set_allocated_fileinfo(&fileInfo);
    getfileinforesponse->set_statuscode(curve::mds::StatusCode::kOK);
    fakeGetFileInforet->controller_ = nullptr;
    fakeGetFileInforet->response_ = getfileinforesponse;
    fakecurvefsservice->SetGetFileInfoFakeReturn(fakeGetFileInforet);
    curve::mds::GetOrAllocateSegmentResponse* segmentresponse =
                new curve::mds::GetOrAllocateSegmentResponse();
    FakeReturn* segmentRet = new FakeReturn(&cntl, segmentresponse);
    fakecurvefsservice->SetGetOrAllocateSegmentFakeReturn(segmentRet);
    ASSERT_EQ(namespaceTool.RunCommand("get"), -1);

    // controller failed的情况
    fakeGetFileInforet->controller_ = &cntl;
    fakeGetFileInforet->response_ = getfileinforesponse;
    fakecurvefsservice->SetGetFileInfoFakeReturn(fakeGetFileInforet);
    ASSERT_EQ(namespaceTool.RunCommand("get"), -1);

    // get目录的时候，listDir错误
    curve::mds::ListDirResponse* listdirresponse =
                            new curve::mds::ListDirResponse();
    FakeReturn* fakeListDirRet =
         new FakeReturn(&cntl, listdirresponse);
    fakecurvefsservice->SetListDir(fakeListDirRet);
    ASSERT_EQ(namespaceTool.RunCommand("get"), -1);
}

TEST_F(NameSpaceToolTest, deleteError) {
    curve::tool::NameSpaceTool namespaceTool;
    namespaceTool.Init();
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    brpc::Controller cntl;
    cntl.SetFailed("error for test");

    // statusCode不等于kOK的情况
    curve::mds::DeleteFileResponse* deletefileresponse =
                            new curve::mds::DeleteFileResponse();
    deletefileresponse->set_statuscode(curve::mds::StatusCode::kFileNotExists);
    FakeReturn* fakeDeleteFileret =
            new FakeReturn(nullptr, static_cast<void*>(deletefileresponse));
    fakecurvefsservice->SetDeleteFile(fakeDeleteFileret);
    ASSERT_EQ(namespaceTool.RunCommand("delete"), -1);

    // controller failed的情况
    fakeDeleteFileret->controller_ = &cntl;
    fakeDeleteFileret->response_ = deletefileresponse;
    fakecurvefsservice->SetDeleteFile(fakeDeleteFileret);
    ASSERT_EQ(namespaceTool.RunCommand("delete"), -1);

    // 清空回收站的时候，ListFile失败
    curve::mds::ListDirResponse* listdirresponse =
                            new curve::mds::ListDirResponse();
    FakeReturn* fakeListDirRet =
         new FakeReturn(&cntl, listdirresponse);
    fakecurvefsservice->SetListDir(fakeListDirRet);
    ASSERT_EQ(namespaceTool.RunCommand("clean-recycle"), -1);
}

TEST_F(NameSpaceToolTest, createError) {
    curve::tool::NameSpaceTool namespaceTool;
    namespaceTool.Init();
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    FakeMDSCurveFSService* fakecurvefsservice = fakemds.GetMDSService();
    brpc::Controller cntl;
    cntl.SetFailed("error for test");

    // statusCode不等于kOK的情况
    curve::mds::CreateFileResponse* createfileresponse =
                            new curve::mds::CreateFileResponse();
    createfileresponse->set_statuscode(curve::mds::StatusCode::kFileNotExists);
    FakeReturn* fakeCreateFileret =
            new FakeReturn(nullptr, static_cast<void*>(createfileresponse));
    fakecurvefsservice->SetCreateFileFakeReturn(fakeCreateFileret);
    ASSERT_EQ(namespaceTool.RunCommand("create"), -1);

    // controller failed的情况
    fakeCreateFileret->controller_ = &cntl;
    fakeCreateFileret->response_ = createfileresponse;
    fakecurvefsservice->SetCreateFileFakeReturn(fakeCreateFileret);
    ASSERT_EQ(namespaceTool.RunCommand("create"), -1);
}
