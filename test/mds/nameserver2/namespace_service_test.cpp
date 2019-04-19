/*
 * Project: curve
 * Created Date: Wednesday September 26th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include <unistd.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include "src/mds/nameserver2/namespace_service.h"
#include "src/mds/nameserver2/curvefs.h"
#include "src/mds/nameserver2/chunk_allocator.h"
#include "src/common/timeutility.h"
#include "src/common/configuration.h"
#include "test/mds/nameserver2/fakes.h"
#include "test/mds/nameserver2/mock_clean_manager.h"
#include "src/mds/nameserver2/clean_manager.h"
#include "src/mds/nameserver2/clean_core.h"
#include "src/mds/nameserver2/clean_task_manager.h"
#include "src/common/authenticator.h"

using curve::common::TimeUtility;
using curve::common::Authenticator;

namespace curve {
namespace mds {

class NameSpaceServiceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        // init the kcurvefs, use the fake element
        storage_ =  new FakeNameServerStorage();
        inodeGenerator_ = new FakeInodeIDGenerator(0);

        cleanCore_ = std::make_shared<CleanCore>(storage_);
        // new taskmanger for 2 worker thread, and check thread period 2 second
        cleanTaskManager_ = std::make_shared<CleanTaskManager>(2, 2000);

        cleanManager_ = std::make_shared<CleanManager>(cleanCore_,
                cleanTaskManager_, storage_);

        ASSERT_EQ(cleanManager_->Start(), true);

        std::shared_ptr<FackTopologyAdmin> topologyAdmin =
                                std::make_shared<FackTopologyAdmin>();
        std::shared_ptr<FackChunkIDGenerator> chunkIdGenerator =
                            std::make_shared<FackChunkIDGenerator>();
        chunkSegmentAllocate_ =
                new ChunkSegmentAllocatorImpl(topologyAdmin, chunkIdGenerator);

        sessionManager_ =
               new SessionManager(std::make_shared<FakeRepoInterface>());

        // session repo已经fake，数据库相关参数不需要
        sessionOptions.sessionDbName = "";
        sessionOptions.sessionUser = "";
        sessionOptions.sessionUrl = "";
        sessionOptions.sessionPassword = "";
        sessionOptions.leaseTime = 5000000;
        sessionOptions.toleranceTime = 500000;
        sessionOptions.intevalTime = 100000;

        authOptions.rootOwner = "root";
        authOptions.rootPassword = "root_password";
        kCurveFS.Init(storage_, inodeGenerator_, chunkSegmentAllocate_,
                        cleanManager_,
                        sessionManager_, sessionOptions, authOptions);
    }

    void TearDown() override {
        kCurveFS.Uninit();

        if (cleanManager_ != nullptr) {
            ASSERT_EQ(cleanManager_->Stop(), true);
        }

        if (storage_ != nullptr) {
            delete storage_;
            storage_ = nullptr;
        }
        if (inodeGenerator_ != nullptr) {
            delete inodeGenerator_;
            inodeGenerator_ = nullptr;
        }
        if (chunkSegmentAllocate_ != nullptr) {
            delete chunkSegmentAllocate_;
            chunkSegmentAllocate_ = nullptr;
        }
        if (sessionManager_ != nullptr) {
            delete sessionManager_;
            sessionManager_ = nullptr;
        }
    }

 public:
    NameServerStorage *storage_;
    InodeIDGenerator *inodeGenerator_;
    ChunkSegmentAllocator *chunkSegmentAllocate_;

    std::shared_ptr<CleanCore> cleanCore_;
    std::shared_ptr<CleanTaskManager> cleanTaskManager_;
    std::shared_ptr<CleanManager> cleanManager_;

    SessionManager *sessionManager_;
    struct SessionOptions sessionOptions;
    struct RootAuthOption authOptions;
};

TEST_F(NameSpaceServiceTest, test1) {
    brpc::Server server;
    std::string listenAddr = "0.0.0.0:9761";

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start(listenAddr.c_str(), &option), 0);

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(listenAddr.c_str(), nullptr), 0);

    CurveFSService_Stub stub(&channel);


    // test CreateFile
    // create /file1 , /file2, /dir/file3
    CreateFileRequest request;
    CreateFileResponse response;
    brpc::Controller cntl;
    uint64_t fileLength = kMiniFileLength;

    request.set_filename("/file1");
    request.set_owner("owner1");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(1);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request.set_filename("/file2");
    request.set_owner("owner2");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(2);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    cntl.Reset();
    request.set_filename("/dir");
    request.set_owner("owner3");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_DIRECTORY);
    request.set_filelength(0);

    cntl.set_log_id(3);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    cntl.Reset();
    request.set_filename("/dir/file3");
    request.set_owner("owner3");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(3);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    // 在一个不存在的目录下创建文件，会失败 kFileNotExists
    cntl.Reset();
    request.set_filename("/dir4/file4");
    request.set_owner("owner4");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(3);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kFileNotExists);
    } else {
        FAIL();
    }

    // 在一个文件下创建文件，会失败 kNotDirectory
    cntl.Reset();
    request.set_filename("/file2/file4");
    request.set_owner("owner2");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(3);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kNotDirectory);
    } else {
        FAIL();
    }

    // 如果创建一个已经存在的文件，会创建失败kFileExists
    cntl.Reset();
    request.set_filename("/file2");
    request.set_owner("owner2");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(2);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kFileExists);
    } else {
        FAIL();
    }

    // test GetFileInfo
    cntl.Reset();
    GetFileInfoRequest request1;
    GetFileInfoResponse response1;
    request1.set_filename("/file1");
    request1.set_owner("owner1");
    request1.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response1.statuscode(), StatusCode::kOK);
        ASSERT_EQ(response1.fileinfo().id(), 1);
        ASSERT_EQ(response1.fileinfo().filename(), "file1");
        ASSERT_EQ(response1.fileinfo().owner(), "owner1");
        ASSERT_EQ(response1.fileinfo().parentid(), 0);
        ASSERT_EQ(response1.fileinfo().filetype(), INODE_PAGEFILE);
        ASSERT_EQ(response1.fileinfo().chunksize(), DefaultChunkSize);
        ASSERT_EQ(response1.fileinfo().segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(response1.fileinfo().length(), fileLength);
    } else {
        ASSERT_TRUE(false);
    }

    // test GetOrAllocateSegment
    // 为file1分配空间
    cntl.Reset();
    GetOrAllocateSegmentRequest request2;
    GetOrAllocateSegmentResponse response2;
    request2.set_filename("/file1");
    request2.set_owner("owner1");
    request2.set_date(TimeUtility::GetTimeofDayUs());
    request2.set_offset(DefaultSegmentSize);
    request2.set_allocateifnotexist(false);
    stub.GetOrAllocateSegment(&cntl, &request2, &response2, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response2.statuscode(), StatusCode::kSegmentNotAllocated);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request2.set_filename("/file1");
    request2.set_owner("owner1");
    request2.set_date(TimeUtility::GetTimeofDayUs());
    request2.set_offset(DefaultSegmentSize);
    request2.set_allocateifnotexist(true);
    stub.GetOrAllocateSegment(&cntl, &request2, &response2, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response2.statuscode(), StatusCode::kOK);
        ASSERT_EQ(response2.pagefilesegment().segmentsize(),
            response1.fileinfo().segmentsize());
        ASSERT_EQ(response2.pagefilesegment().chunksize(),
            response1.fileinfo().chunksize());
        ASSERT_EQ(response2.pagefilesegment().startoffset(), request2.offset());

        int chunkNumber = response2.pagefilesegment().segmentsize()/
                            response2.pagefilesegment().chunksize();

        ASSERT_EQ(response2.pagefilesegment().chunks().size(), chunkNumber);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    GetOrAllocateSegmentRequest request3;
    GetOrAllocateSegmentResponse response3;
    request3.set_filename("/file1");
    request3.set_owner("owner1");
    request3.set_date(TimeUtility::GetTimeofDayUs());
    request3.set_offset(DefaultSegmentSize);
    request3.set_allocateifnotexist(false);
    stub.GetOrAllocateSegment(&cntl, &request3, &response3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response3.statuscode(), StatusCode::kOK);
        ASSERT_EQ(response3.pagefilesegment().SerializeAsString(),
            response2.pagefilesegment().SerializeAsString());
    } else {
        ASSERT_TRUE(false);
    }

    // test RenameFile
    // file1 重命名为file3
    // 第一次重命名到根目录下，非root owner，失败
    // 第二次重命名成功 /dir/file3 -> /dir/file4
    // 第三次原文件不存在，重命名失败
    // 第四次重命名到根目录下，root owner，成功 /dir/file4 -> /file4
    cntl.Reset();
    RenameFileRequest request4;
    RenameFileResponse response4;

    cntl.Reset();
    request4.set_oldfilename("/dir/file3");
    request4.set_newfilename("/file4");
    request4.set_owner("owner3");
    request4.set_date(TimeUtility::GetTimeofDayUs());
    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kOwnerAuthFail);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request4.set_oldfilename("/dir/file3");
    request4.set_newfilename("/dir/file4");
    request4.set_owner("owner3");
    request4.set_date(TimeUtility::GetTimeofDayUs());
    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request4.set_oldfilename("/dir/file3");
    request4.set_newfilename("/dir/file3");
    request4.set_owner("owner3");
    request4.set_date(TimeUtility::GetTimeofDayUs());
    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kFileNotExists);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();

    std::string oldname = "/dir/file4";
    uint64_t date = TimeUtility::GetTimeofDayUs();
    std::string str2sig = Authenticator::GetString2Signature(date,
                                                authOptions.rootOwner);
    std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                authOptions.rootPassword);

    request4.set_oldfilename(oldname);
    request4.set_newfilename("/file4");
    request4.set_owner(authOptions.rootOwner);
    request4.set_date(date);
    request4.set_signature(sig);

    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    // test ExtendFile
    // 扩容file2,第一次扩大，成功；第二次缩小，失败
    uint64_t newsize = kMiniFileLength * 2;
    cntl.Reset();
    ExtendFileRequest request5;
    ExtendFileResponse response5;
    request5.set_filename("/file2");
    request5.set_owner("owner2");
    request5.set_date(TimeUtility::GetTimeofDayUs());
    request5.set_newsize(newsize);
    stub.ExtendFile(&cntl, &request5, &response5, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response5.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request5.set_filename("/file2");
    request5.set_owner("owner2");
    request5.set_date(TimeUtility::GetTimeofDayUs());
    request5.set_newsize(kMiniFileLength);
    stub.ExtendFile(&cntl, &request5, &response5, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response5.statuscode(), StatusCode::kShrinkBiggerFile);
    } else {
        ASSERT_TRUE(false);
    }

    // begin session test，开始测试时，有/file1,/file2和/file4
    // OpenFile case1. 文件不存在，返回kFileNotExists
    cntl.Reset();
    OpenFileRequest request8;
    OpenFileResponse response8;
    request8.set_filename("/file3");
    request8.set_owner("owner3");
    request8.set_date(TimeUtility::GetTimeofDayUs());

    stub.OpenFile(&cntl, &request8, &response8, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response8.statuscode(), StatusCode::kFileNotExists);
    } else {
        ASSERT_TRUE(false);
    }

    // OpenFile case2. 文件存在，没有open过，返回成功、session、fileInfo
    cntl.Reset();
    OpenFileRequest request9;
    OpenFileResponse response9;
    request9.set_filename("/file2");
    request9.set_owner("owner2");
    request9.set_date(TimeUtility::GetTimeofDayUs());

    stub.OpenFile(&cntl, &request9, &response9, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response9.statuscode(), StatusCode::kOK);
        ASSERT_EQ(response9.protosession().sessionstatus(),
                                    SessionStatus::kSessionOK);
        ASSERT_EQ(response9.fileinfo().filename(), "file2");
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    OpenFileRequest request10;
    OpenFileResponse response10;
    request10.set_filename("/file1");
    request10.set_owner("owner1");
    request10.set_date(TimeUtility::GetTimeofDayUs());

    stub.OpenFile(&cntl, &request10, &response10, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response10.statuscode(), StatusCode::kOK);
        ASSERT_EQ(response10.protosession().sessionstatus(),
                                                SessionStatus::kSessionOK);
        ASSERT_EQ(response10.fileinfo().filename(), "file1");
    } else {
        ASSERT_TRUE(false);
    }

    // OpenFile case3. 文件存在，open过，返回session被占用
    cntl.Reset();
    OpenFileRequest request11;
    OpenFileResponse response11;
    request11.set_filename("/file2");
    request11.set_owner("owner2");
    request11.set_date(TimeUtility::GetTimeofDayUs());

    stub.OpenFile(&cntl, &request11, &response11, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response11.statuscode(), StatusCode::kFileOccupied);
    } else {
        ASSERT_TRUE(false);
    }

    // CloseFile case1. 文件不存在，返回kFileNotExists
    cntl.Reset();
    CloseFileRequest request12;
    CloseFileResponse response12;
    request12.set_filename("/file3");
    request12.set_owner("owner3");
    request12.set_date(TimeUtility::GetTimeofDayUs());
    request12.set_sessionid("test_session");

    stub.CloseFile(&cntl, &request12, &response12, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response12.statuscode(), StatusCode::kFileNotExists);
    } else {
        ASSERT_TRUE(false);
    }


    // CloseFile case2. 文件存在，session不存在，返回kSessionNotExist
    cntl.Reset();
    CloseFileRequest request13;
    CloseFileResponse response13;
    request13.set_filename("/file2");
    request13.set_owner("owner2");
    request13.set_date(TimeUtility::GetTimeofDayUs());
    request13.set_sessionid("test_session");

    stub.CloseFile(&cntl, &request13, &response13, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response13.statuscode(), StatusCode::kSessionNotExist);
    } else {
        ASSERT_TRUE(false);
    }

    // CloseFile case3. 文件存在，session存在，返回成功
    cntl.Reset();
    CloseFileRequest request14;
    CloseFileResponse response14;
    request14.set_filename("/file2");
    request14.set_owner("owner2");
    request14.set_date(TimeUtility::GetTimeofDayUs());
    request14.set_sessionid(response9.protosession().sessionid());

    stub.CloseFile(&cntl, &request14, &response14, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response14.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    // CloseFile case4. 文件存在，session不存在，返回成功
    cntl.Reset();
    request14.set_filename("/file2");
    request14.set_owner("owner2");
    request14.set_date(TimeUtility::GetTimeofDayUs());
    request14.set_sessionid(response9.protosession().sessionid());

    stub.CloseFile(&cntl, &request14, &response14, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response14.statuscode(), StatusCode::kSessionNotExist);
    } else {
        ASSERT_TRUE(false);
    }

    // RefreshSession case1. 文件不存在，返回kFileNotExists
    cntl.Reset();
    ReFreshSessionRequest request15;
    ReFreshSessionResponse response15;
    request15.set_filename("/file3");
    request15.set_owner("owner3");
    request15.set_date(TimeUtility::GetTimeofDayUs());
    request15.set_sessionid(response10.protosession().sessionid());
    request15.set_date(common::TimeUtility::GetTimeofDayUs());
    request15.set_signature("todo,signature");

    stub.RefreshSession(&cntl, &request15, &response15, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response15.statuscode(), StatusCode::kFileNotExists);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // RefreshSession case2. 文件存在，session没有open过，返回kSessionNotExist
    cntl.Reset();
    ReFreshSessionRequest request16;
    ReFreshSessionResponse response16;
    request16.set_filename("/file1");
    request16.set_owner("owner1");
    request16.set_date(TimeUtility::GetTimeofDayUs());
    request16.set_sessionid("test_session");
    request16.set_date(common::TimeUtility::GetTimeofDayUs());
    request16.set_signature("todo,signature");

    stub.RefreshSession(&cntl, &request16, &response16, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response16.statuscode(), StatusCode::kSessionNotExist);
    } else {
        ASSERT_TRUE(false);
    }

    // RefreshSession case3. 文件存在，session存在，session没有过期，返回成功
    cntl.Reset();
    ReFreshSessionRequest request18;
    ReFreshSessionResponse response18;
    request18.set_filename("/file1");
    request18.set_owner("owner1");
    request18.set_date(TimeUtility::GetTimeofDayUs());
    request18.set_sessionid(response10.protosession().sessionid());
    request18.set_date(common::TimeUtility::GetTimeofDayUs());
    request18.set_signature("todo,signature");

    stub.RefreshSession(&cntl, &request18, &response18, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response18.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }
    // end session test

    server.Stop(10);
    server.Join();
    return;
}


TEST_F(NameSpaceServiceTest, snapshottests) {
    brpc::Server server;
    std::string listenAddr = "0.0.0.0:9761";

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start(listenAddr.c_str(), &option), 0);

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(listenAddr.c_str(), nullptr), 0);

    CurveFSService_Stub stub(&channel);


    // test create file
    CreateFileRequest request;
    CreateFileResponse response;

    brpc::Controller cntl;
    uint64_t fileLength = kMiniFileLength;

    request.set_filename("/file1");
    request.set_owner("owner1");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(2);
    stub.CreateFile(&cntl,  &request, &response,  NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    // get the file
    cntl.Reset();
    GetFileInfoRequest request1;
    GetFileInfoResponse response1;
    request1.set_filename("/file1");
    request1.set_owner("owner1");
    request1.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        FileInfo  file = response1.fileinfo();
        ASSERT_EQ(response1.statuscode(), StatusCode::kOK);
        ASSERT_EQ(file.id(), 1);
        ASSERT_EQ(file.filename(), "file1");
        ASSERT_EQ(file.parentid(), 0);
        ASSERT_EQ(file.filetype(), INODE_PAGEFILE);
        ASSERT_EQ(file.chunksize(), DefaultChunkSize);
        ASSERT_EQ(file.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(file.length(), fileLength);
        ASSERT_EQ(file.fullpathname(), "/file1");
        ASSERT_EQ(file.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    // test createsnapshotfile
    cntl.Reset();
    CreateSnapShotRequest snapshotRequest;
    CreateSnapShotResponse snapshotResponses;
    snapshotRequest.set_filename("/file1");
    snapshotRequest.set_owner("owner1");
    snapshotRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.CreateSnapShot(&cntl, &snapshotRequest, &snapshotResponses, NULL);
    if (!cntl.Failed()) {
        FileInfo snapshotFileInfo;
        snapshotFileInfo.CopyFrom(snapshotResponses.snapshotfileinfo());
        ASSERT_EQ(snapshotResponses.statuscode(), StatusCode::kOK);
        ASSERT_EQ(snapshotFileInfo.id(), 2);
        ASSERT_EQ(snapshotFileInfo.parentid(), 1);
        ASSERT_EQ(snapshotFileInfo.filename(), "file1-1");
        ASSERT_EQ(snapshotFileInfo.filetype(), INODE_SNAPSHOT_PAGEFILE);
        ASSERT_EQ(snapshotFileInfo.fullpathname(), "/file1/file1-1");
        ASSERT_EQ(snapshotFileInfo.filestatus(), FileStatus::kFileCreated);
        ASSERT_EQ(snapshotFileInfo.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    // get the original file
    cntl.Reset();
    request1.set_filename("/file1");
    request1.set_owner("owner1");
    request1.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        FileInfo file = response1.fileinfo();
        ASSERT_EQ(response1.statuscode(), StatusCode::kOK);
        ASSERT_EQ(file.id(), 1);
        ASSERT_EQ(file.filename(), "file1");
        ASSERT_EQ(file.filetype(), INODE_PAGEFILE);
        ASSERT_EQ(file.chunksize(), DefaultChunkSize);
        ASSERT_EQ(file.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(file.length(), fileLength);
        ASSERT_EQ(file.fullpathname(), "/file1");
        ASSERT_EQ(file.seqnum(), 2);
    } else {
        ASSERT_TRUE(false);
    }

    // Check SnapShot Status
    cntl.Reset();
    CheckSnapShotStatusRequest checkRequest;
    CheckSnapShotStatusResponse checkResponse;
    checkRequest.set_filename("/file1");
    checkRequest.set_owner("owner1");
    checkRequest.set_date(TimeUtility::GetTimeofDayUs());
    checkRequest.set_seq(1);
    stub.CheckSnapShotStatus(&cntl, &checkRequest, &checkResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(checkResponse.statuscode(), StatusCode::kOK);
        ASSERT_EQ(checkResponse.filestatus(), FileStatus::kFileCreated);
        ASSERT_EQ(checkResponse.progress(), 0);
    } else {
        ASSERT_TRUE(false);
    }

    // test deletesnapshotfile
    cntl.Reset();
    DeleteSnapShotRequest deleteRequest;
    DeleteSnapShotResponse deleteResponse;
    deleteRequest.set_filename("/file1");
    deleteRequest.set_owner("owner1");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    deleteRequest.set_seq(1);
    stub.DeleteSnapShot(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        LOG(ERROR) << cntl.ErrorText();
        ASSERT_TRUE(false);
    }


    // list snapshotdelete ok
    cntl.Reset();
    ListSnapShotFileInfoRequest listRequest;
    ListSnapShotFileInfoResponse listResponse;

    listRequest.set_filename("/file1");
    listRequest.set_owner("owner1");
    listRequest.set_date(TimeUtility::GetTimeofDayUs());
    listRequest.add_seq(2);
    stub.ListSnapShot(&cntl, &listRequest, &listResponse, NULL);

    if (!cntl.Failed()) {
        auto snapshotFileNum = listResponse.fileinfo_size();
        if (snapshotFileNum == 0) {
            LOG(INFO) << "snapfile deleted";
        } else {
            FileInfo snapShotFileInfo = listResponse.fileinfo(0);
            ASSERT_EQ(snapShotFileInfo.id(), 2);
            ASSERT_EQ(snapShotFileInfo.filestatus(), FileStatus::kFileDeleting);
        }
    } else {
        ASSERT_TRUE(false);
    }
    server.Stop(10);
    server.Join();
}

TEST_F(NameSpaceServiceTest, deletefiletests) {
    brpc::Server server;
    std::string listenAddr = "0.0.0.0:9761";

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start(listenAddr.c_str(), &option), 0);

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(listenAddr.c_str(), nullptr), 0);

    CurveFSService_Stub stub(&channel);

    // 先创建文件/file1，目录/dir1，文件/dir1/file2
    CreateFileRequest request;
    CreateFileResponse response;

    brpc::Controller cntl;
    uint64_t fileLength = kMiniFileLength;

    request.set_filename("/file1");
    request.set_owner("owner");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(2);
    stub.CreateFile(&cntl,  &request, &response,  NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request.set_filename("/dir1");
    request.set_owner("owner");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_DIRECTORY);
    request.set_filelength(0);

    cntl.set_log_id(3);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    cntl.Reset();
    request.set_filename("/dir1/file2");
    request.set_owner("owner");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(3);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    // 查看文件/file1，目录/dir1，文件/dir1/file2的状态
    cntl.Reset();
    GetFileInfoRequest request1;
    GetFileInfoResponse response1;
    request1.set_filename("/file1");
    request1.set_owner("owner");
    request1.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        FileInfo  file = response1.fileinfo();
        ASSERT_EQ(response1.statuscode(), StatusCode::kOK);
        ASSERT_EQ(file.id(), 1);
        ASSERT_EQ(file.filename(), "file1");
        ASSERT_EQ(file.parentid(), 0);
        ASSERT_EQ(file.filetype(), INODE_PAGEFILE);
        ASSERT_EQ(file.chunksize(), DefaultChunkSize);
        ASSERT_EQ(file.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(file.length(), fileLength);
        ASSERT_EQ(file.fullpathname(), "/file1");
        ASSERT_EQ(file.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request1.set_filename("/dir1");
    request1.set_owner("owner");
    request1.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        FileInfo  file = response1.fileinfo();
        ASSERT_EQ(response1.statuscode(), StatusCode::kOK);
        ASSERT_EQ(file.id(), 2);
        ASSERT_EQ(file.filename(), "dir1");
        ASSERT_EQ(file.parentid(), 0);
        ASSERT_EQ(file.filetype(), INODE_DIRECTORY);
        ASSERT_EQ(file.fullpathname(), "/dir1");
        ASSERT_EQ(file.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request1.set_filename("/dir1/file2");
    request1.set_owner("owner");
    request1.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        FileInfo file = response1.fileinfo();
        ASSERT_EQ(response1.statuscode(), StatusCode::kOK);
        ASSERT_EQ(file.id(), 3);
        ASSERT_EQ(file.filename(), "file2");
        ASSERT_EQ(file.filetype(), INODE_PAGEFILE);
        ASSERT_EQ(file.chunksize(), DefaultChunkSize);
        ASSERT_EQ(file.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(file.length(), fileLength);
        ASSERT_EQ(file.fullpathname(), "/dir1/file2");
        ASSERT_EQ(file.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    // 开始测试删除文件逻辑
    // 1 如果文件有被session占用，那么返回kFileOccupied
    cntl.Reset();
    OpenFileRequest request2;
    OpenFileResponse response2;
    request2.set_filename("/file1");
    request2.set_owner("owner");
    request2.set_date(TimeUtility::GetTimeofDayUs());

    stub.OpenFile(&cntl, &request2, &response2, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response2.statuscode(), StatusCode::kOK);
        ASSERT_EQ(response2.protosession().sessionstatus(),
                                    SessionStatus::kSessionOK);
        ASSERT_EQ(response2.fileinfo().filename(), "file1");
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    DeleteFileRequest request3;
    DeleteFileResponse response3;
    request3.set_filename("/file1");
    request3.set_owner("owner");
    request3.set_date(TimeUtility::GetTimeofDayUs());

    stub.DeleteFile(&cntl, &request3, &response3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response3.statuscode(), StatusCode::kFileOccupied);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    CloseFileRequest request4;
    CloseFileResponse response4;
    request4.set_filename("/file1");
    request4.set_owner("owner");
    request4.set_date(TimeUtility::GetTimeofDayUs());
    request4.set_sessionid(response2.protosession().sessionid());

    stub.CloseFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    // 2 如果文件有快照，那么删除文件返回kFileUnderSnapShot
    cntl.Reset();
    CreateSnapShotRequest snapshotRequest;
    CreateSnapShotResponse snapshotResponses;
    snapshotRequest.set_filename("/file1");
    snapshotRequest.set_owner("owner");
    snapshotRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.CreateSnapShot(&cntl, &snapshotRequest, &snapshotResponses, NULL);
    if (!cntl.Failed()) {
        FileInfo snapshotFileInfo;
        snapshotFileInfo.CopyFrom(snapshotResponses.snapshotfileinfo());
        ASSERT_EQ(snapshotResponses.statuscode(), StatusCode::kOK);
        ASSERT_EQ(snapshotFileInfo.id(), 4);
        ASSERT_EQ(snapshotFileInfo.parentid(), 1);
        ASSERT_EQ(snapshotFileInfo.filename(), "file1-1");
        ASSERT_EQ(snapshotFileInfo.filetype(), INODE_SNAPSHOT_PAGEFILE);
        ASSERT_EQ(snapshotFileInfo.fullpathname(), "/file1/file1-1");
        ASSERT_EQ(snapshotFileInfo.filestatus(), FileStatus::kFileCreated);
        ASSERT_EQ(snapshotFileInfo.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request3.set_filename("/file1");
    request3.set_owner("owner");
    request3.set_date(TimeUtility::GetTimeofDayUs());

    stub.DeleteFile(&cntl, &request3, &response3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response3.statuscode(), StatusCode::kFileUnderSnapShot);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    DeleteSnapShotRequest deleteRequest;
    DeleteSnapShotResponse deleteResponse;
    deleteRequest.set_filename("/file1");
    deleteRequest.set_owner("owner");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    deleteRequest.set_seq(1);
    stub.DeleteSnapShot(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        LOG(ERROR) << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 3 如果目录下有文件，那么删除目录返回kDirNotEmpty
    cntl.Reset();
    request3.set_filename("/dir1");
    request3.set_owner("owner");
    request3.set_date(TimeUtility::GetTimeofDayUs());

    stub.DeleteFile(&cntl, &request3, &response3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response3.statuscode(), StatusCode::kDirNotEmpty);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 4 删除文件/file1成功，查询文件已经删除
    cntl.Reset();
    request3.set_filename("/file1");
    request3.set_owner("owner");
    request3.set_date(TimeUtility::GetTimeofDayUs());

    stub.DeleteFile(&cntl, &request3, &response3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response3.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request1.set_filename("/file1");
    request1.set_owner("owner");
    request1.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response1.statuscode(), StatusCode::kFileNotExists);
    } else {
        ASSERT_TRUE(false);
    }

    // 4 删除文件/dir1/file2成功，删除目录/dir1成功，查询目录和文件均已经删除
    cntl.Reset();
    request3.set_filename("/dir1/file2");
    request3.set_owner("owner");
    request3.set_date(TimeUtility::GetTimeofDayUs());

    stub.DeleteFile(&cntl, &request3, &response3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response3.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request1.set_filename("/dir1/file2");
    request1.set_owner("owner");
    request1.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response1.statuscode(), StatusCode::kFileNotExists);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request3.set_filename("/dir1");
    request3.set_owner("owner");
    request3.set_date(TimeUtility::GetTimeofDayUs());

    stub.DeleteFile(&cntl, &request3, &response3, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response3.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request1.set_filename("/dir1");
    request1.set_owner("owner");
    request1.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response1.statuscode(), StatusCode::kFileNotExists);
    } else {
        ASSERT_TRUE(false);
    }

    server.Stop(10);
    server.Join();
}

TEST_F(NameSpaceServiceTest, isPathValid) {
    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(isPathValid("/"), true);
    ASSERT_EQ(isPathValid("/a"), true);
    ASSERT_EQ(isPathValid("/a/b/c/a/d"), true);
    ASSERT_EQ(isPathValid("//"), false);
    ASSERT_EQ(isPathValid("/a/"), false);
    ASSERT_EQ(isPathValid("/a//b"), false);
    ASSERT_EQ(isPathValid("//a/b"), false);
    ASSERT_EQ(isPathValid("/a/b/"), false);
}

TEST_F(NameSpaceServiceTest, clonetest) {
    brpc::Server server;
    std::string listenAddr = "0.0.0.0:9761";

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start(listenAddr.c_str(), &option), 0);

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(listenAddr.c_str(), nullptr), 0);

    CurveFSService_Stub stub(&channel);

    // create clone file
    CreateCloneFileRequest request;
    CreateCloneFileResponse response;
    brpc::Controller cntl;

    request.set_filename("/clonefile1");
    request.set_filetype(FileType::INODE_PAGEFILE);
    request.set_filelength(kMiniFileLength);
    request.set_seq(10);
    request.set_chunksize(DefaultChunkSize);
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_owner("tom");
    cntl.set_log_id(1);

    stub.CreateCloneFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    // get file
    GetFileInfoRequest getRequest;
    GetFileInfoResponse getResponse;

    cntl.Reset();
    getRequest.set_filename("/clonefile1");
    getRequest.set_date(TimeUtility::GetTimeofDayUs());
    getRequest.set_owner("tom");

    stub.GetFileInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        FileInfo fileInfo = getResponse.fileinfo();
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
        ASSERT_EQ(fileInfo.filename(), "clonefile1");
        ASSERT_EQ(fileInfo.fullpathname(), "/clonefile1");
        ASSERT_EQ(fileInfo.filetype(), FileType::INODE_PAGEFILE);
        ASSERT_EQ(fileInfo.owner(), "tom");
        ASSERT_EQ(fileInfo.chunksize(), DefaultChunkSize);
        ASSERT_EQ(fileInfo.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(fileInfo.length(), kMiniFileLength);
        ASSERT_EQ(fileInfo.filestatus(), FileStatus::kFileCloning);
    } else {
        FAIL();
    }

    // set clone file status
    SetCloneFileStatusRequest setRequest;
    SetCloneFileStatusResponse setResponse;

    cntl.Reset();
    setRequest.set_filename("/clonefile1");
    setRequest.set_date(TimeUtility::GetTimeofDayUs());
    setRequest.set_owner("tom");
    setRequest.set_filestatus(FileStatus::kFileCloneMetaInstalled);

    stub.SetCloneFileStatus(&cntl, &setRequest, &setResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }
}

}  // namespace mds
}  // namespace curve


