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
 * Created Date: Wednesday September 26th 2018
 * Author: hzsunjianliang
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
#include "src/common/string_util.h"
#include "test/mds/nameserver2/fakes.h"
#include "test/mds/nameserver2/mock/mock_clean_manager.h"
#include "test/mds/nameserver2/mock/mock_chunk_allocate.h"
#include "src/mds/nameserver2/clean_manager.h"
#include "src/mds/nameserver2/clean_core.h"
#include "src/mds/nameserver2/clean_task_manager.h"
#include "src/common/authenticator.h"
#include "test/mds/mock/mock_topology.h"
#include "test/mds/mock/mock_chunkserver.h"
#include "src/mds/chunkserverclient/copyset_client.h"
#include "test/mds/mock/mock_alloc_statistic.h"

using curve::common::TimeUtility;
using curve::common::Authenticator;
using curve::mds::topology::MockTopology;
using ::curve::mds::chunkserverclient::ChunkServerClientOption;
using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Matcher;

namespace curve {
namespace mds {

class NameSpaceServiceTest : public ::testing::Test {
 public:
    struct RequestOption {
        bool isRootUser = false;
        bool forceDelete = false;

        RequestOption() = default;

        RequestOption(bool isRootUser, bool forceDelete)
            : isRootUser(isRootUser), forceDelete(forceDelete) {}
    };

 protected:
    void SetUp() override {
        // init the kcurvefs, use the fake element
        storage_ =  std::make_shared<FakeNameServerStorage>();
        inodeGenerator_ = std::make_shared<FakeInodeIDGenerator>(0);

        topology_ = std::make_shared<MockTopology>();
        ChunkServerClientOption option;
        auto channelPool = std::make_shared<ChannelPool>();
        auto client = std::make_shared<CopysetClient>(topology_,
                                                        option, channelPool);
        allocStatistic_ = std::make_shared<MockAllocStatistic>();
        cleanCore_ = std::make_shared<CleanCore>(
            storage_, client, allocStatistic_);

        // new taskmanger for 2 worker thread, and check thread period 2 second
        cleanTaskManager_ = std::make_shared<CleanTaskManager>(channelPool,
                                                                    2, 2000);

        cleanManager_ = std::make_shared<CleanManager>(cleanCore_,
                cleanTaskManager_, storage_);

        ASSERT_EQ(cleanManager_->Start(), true);

        std::shared_ptr<FackTopologyChunkAllocator> topologyChunkAllocator =
                                std::make_shared<FackTopologyChunkAllocator>();
        std::shared_ptr<FackChunkIDGenerator> chunkIdGenerator =
                            std::make_shared<FackChunkIDGenerator>();
        chunkSegmentAllocate_ =
                std::make_shared<ChunkSegmentAllocatorImpl>(
                        topologyChunkAllocator, chunkIdGenerator);

        fileRecordManager_ = std::make_shared<FileRecordManager>();
        fileRecordOptions.fileRecordExpiredTimeUs = 5 * 1000;
        fileRecordOptions.scanIntervalTimeUs = 1 * 1000;

        authOptions.rootOwner = "root";
        authOptions.rootPassword = "root_password";

        curveFSOptions.defaultChunkSize = 16 * kMB;
        curveFSOptions.defaultSegmentSize = 1 * kGB;
        curveFSOptions.minFileLength = 10 * kGB;
        curveFSOptions.maxFileLength = 20 * kTB;
        curveFSOptions.fileRecordOptions = fileRecordOptions;
        curveFSOptions.authOptions = authOptions;

        kCurveFS.Init(storage_, inodeGenerator_, chunkSegmentAllocate_,
                        cleanManager_,
                        fileRecordManager_,
                        allocStatistic_,
                        curveFSOptions, topology_,
                        nullptr);

        ASSERT_EQ(curveFSOptions.defaultChunkSize,
                       kCurveFS.GetDefaultChunkSize());
        ASSERT_EQ(curveFSOptions.defaultSegmentSize,
                       kCurveFS.GetDefaultSegmentSize());
        ASSERT_EQ(curveFSOptions.minFileLength, kCurveFS.GetMinFileLength());
        ASSERT_EQ(curveFSOptions.maxFileLength, kCurveFS.GetMaxFileLength());
        DefaultSegmentSize = kCurveFS.GetDefaultSegmentSize();
        kMiniFileLength = kCurveFS.GetMinFileLength();
        kMaxFileLength = kCurveFS.GetMaxFileLength();
        kCurveFS.Run();

        std::this_thread::sleep_for(std::chrono::microseconds(
            11 * fileRecordOptions.fileRecordExpiredTimeUs));
    }

    void TearDown() override {
        kCurveFS.Uninit();

        if (cleanManager_ != nullptr) {
            ASSERT_EQ(cleanManager_->Stop(), true);
        }
    }

    template<typename T>
    void SetRequestAuth(T* request, RequestOption option) {
        uint64_t date = TimeUtility::GetTimeofDayUs();
        request->set_date(date);

        if (!option.isRootUser) {
            request->set_owner("owner");
        } else {
            auto signature = Authenticator::CalcString2Signature(
                Authenticator::GetString2Signature(date, authOptions.rootOwner),
                authOptions.rootPassword);
            request->set_owner(authOptions.rootOwner);
            request->set_signature(signature);
        }
    }

    bool CheckFilename(const std::string& filename, uint64_t dtime) {
        // filename: file-14376-1620453738
        std::vector<std::string> items;
        ::curve::common::SplitString(filename, "-", &items);

        uint64_t time;
        auto n = items.size();
        if (n <= 2 || !::curve::common::StringToUll(items[n - 1], &time)
            || time < dtime || time - dtime > 1) {
            LOG(INFO) << "unexpected filename: " << filename
                      << ", dtime: " << dtime
                      << ", time in file: " << time;
            return false;
        }
        return true;
    }

    bool DeleteFile(const std::string& filename,
                    RequestOption option,
                    DeleteFileResponse* response) {
        brpc::Controller cntl;
        DeleteFileRequest request;

        request.set_filename(filename);
        request.set_forcedelete(option.forceDelete);
        SetRequestAuth(&request, option);
        stub_->DeleteFile(&cntl, &request, response, NULL);

        if (cntl.Failed()) {
            LOG(INFO) << "RPC error for DeleteFile: " << cntl.ErrorText();
            return false;
        }
        return true;
    }

    bool GetFileInfo(const std::string& filename,
                     RequestOption option,
                     GetFileInfoResponse* response) {
        brpc::Controller cntl;
        GetFileInfoRequest request;

        request.set_filename(filename);
        SetRequestAuth(&request, option);
        stub_->GetFileInfo(&cntl, &request, response, NULL);

        if (cntl.Failed()) {
            LOG(INFO) << "RPC error for GetFileInfo: " << cntl.ErrorText();
            return false;
        }
        return true;
    }

    bool ListDir(const std::string& dirname,
                 RequestOption option,
                 ListDirResponse* response) {
        brpc::Controller cntl;
        ListDirRequest request;

        request.set_filename(dirname);
        SetRequestAuth(&request, option);
        stub_->ListDir(&cntl, &request, response, NULL);

        if (cntl.Failed()) {
            LOG(INFO) << "RPC error for ListDir: " << cntl.ErrorText();
            return false;
        }
        return true;
    }

 public:
    std::shared_ptr<NameServerStorage> storage_;
    std::shared_ptr<InodeIDGenerator> inodeGenerator_;
    std::shared_ptr<ChunkSegmentAllocator> chunkSegmentAllocate_;

    std::shared_ptr<CleanCore> cleanCore_;
    std::shared_ptr<CleanTaskManager> cleanTaskManager_;
    std::shared_ptr<CleanManager> cleanManager_;
    std::shared_ptr<MockTopology> topology_;
    std::shared_ptr<MockAllocStatistic> allocStatistic_;
    std::shared_ptr<FileRecordManager> fileRecordManager_;
    std::shared_ptr<CurveFSService_Stub> stub_;
    struct FileRecordOptions fileRecordOptions;
    struct RootAuthOption authOptions;
    struct CurveFSOption curveFSOptions;
    uint64_t DefaultSegmentSize;
    uint64_t kMiniFileLength;
    uint64_t kMaxFileLength;
};

TEST_F(NameSpaceServiceTest, test1) {
    brpc::Server server;

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.Start("127.0.0.1", {8900, 8999}, &option));

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    CurveFSService_Stub stub(&channel);


    // test CreateFile
    // create /file1(owner1) , /file2(owner2), /dir/file3(owner3)
    std::vector<PoolIdType> logicalPools{1, 2, 3};
    EXPECT_CALL(*topology_, GetLogicalPoolInCluster(_))
        .Times(AtLeast(1))
        .WillRepeatedly(Return(logicalPools));
    CreateFileRequest request;
    CreateFileResponse response;
    brpc::Controller cntl;
    uint64_t fileLength = kMiniFileLength;

    // 创建file1,owner1
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

    // 如果创建一个已经存在的目录，会创建失败kFileExists
    cntl.Reset();
    request.set_filename("/dir");
    request.set_owner("owner3");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_DIRECTORY);
    request.set_filelength(0);

    cntl.set_log_id(3);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kFileExists);
    } else {
        FAIL();
    }

    // 创建其他类型文件，返回kNotSupported
    cntl.Reset();
    request.set_filename("/file4");
    request.set_owner("owner4");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_APPENDFILE);
    request.set_filelength(fileLength);
    cntl.set_log_id(3);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kNotSupported);
    } else {
        FAIL();
    }

    cntl.Reset();
    request.set_filename("/file4");
    request.set_owner("owner4");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_APPENDECFILE);
    request.set_filelength(fileLength);
    cntl.set_log_id(3);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kNotSupported);
    } else {
        FAIL();
    }

    cntl.Reset();
    request.set_filename("/file4");
    request.set_owner("owner4");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_SNAPSHOT_PAGEFILE);
    request.set_filelength(fileLength);
    cntl.set_log_id(3);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kNotSupported);
    } else {
        FAIL();
    }

    // 创建文件名不规范的文件会失败
    cntl.Reset();
    request.set_filename("/file4/");
    request.set_owner("owner4");
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_filetype(INODE_PAGEFILE);
    request.set_filelength(fileLength);

    cntl.set_log_id(2);  // set by user
    stub.CreateFile(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kParaError);
    } else {
        FAIL();
    }

    // test ListDir
    {
        ListDirRequest listRequest;
        ListDirResponse listResponse;
        cntl.Reset();
        listRequest.set_filename("/dir");
        listRequest.set_owner("owner3");
        listRequest.set_date(TimeUtility::GetTimeofDayUs());
        stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(listResponse.statuscode(), StatusCode::kOK);
            ASSERT_EQ(listResponse.fileinfo_size(), 1);
            FileInfo fileInfo = listResponse.fileinfo(0);
            ASSERT_EQ(fileInfo.filename(), "file3");
        } else {
            ASSERT_TRUE(false);
        }

        cntl.Reset();
        listRequest.set_filename("/dir2");
        listRequest.set_owner("owner");
        listRequest.set_date(TimeUtility::GetTimeofDayUs());
        stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(listResponse.statuscode(), StatusCode::kFileNotExists);
        } else {
            ASSERT_TRUE(false);
        }

        cntl.Reset();
        listRequest.set_filename("/dir2/");
        listRequest.set_owner("owner");
        listRequest.set_date(TimeUtility::GetTimeofDayUs());
        stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(listResponse.statuscode(), StatusCode::kParaError);
        } else {
            ASSERT_TRUE(false);
        }

        cntl.Reset();
        uint64_t date = TimeUtility::GetTimeofDayUs();
        std::string str2sig = Authenticator::GetString2Signature(date,
                                                authOptions.rootOwner);
        std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                authOptions.rootPassword);
        listRequest.set_signature(sig);
        listRequest.set_filename("/");
        listRequest.set_owner(authOptions.rootOwner);
        listRequest.set_date(date);
        stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(listResponse.statuscode(), StatusCode::kOK);
            ASSERT_EQ(listResponse.fileinfo_size(), 4);
         } else {
            ASSERT_TRUE(false);
        }
    }

    // test GetFileInfo
    cntl.Reset();
    GetFileInfoRequest request1;
    GetFileInfoResponse response1;
    request1.set_filename("/file1/");
    request1.set_owner("owner1");
    request1.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &request1, &response1, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response1.statuscode(), StatusCode::kParaError);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
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
        ASSERT_EQ(response1.fileinfo().chunksize(),
                            curveFSOptions.defaultChunkSize);
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
    request2.set_filename("/file1/");
    request2.set_owner("owner1");
    request2.set_date(TimeUtility::GetTimeofDayUs());
    request2.set_offset(DefaultSegmentSize);
    request2.set_allocateifnotexist(false);
    stub.GetOrAllocateSegment(&cntl, &request2, &response2, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response2.statuscode(), StatusCode::kParaError);
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

    // test get allocated size
    {
        cntl.Reset();
        // file not exist
        GetAllocatedSizeRequest request;
        GetAllocatedSizeResponse response;
        request.set_filename("/file-not-exist");
        stub.GetAllocatedSize(&cntl, &request, &response, NULL);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(StatusCode::kFileNotExists, response.statuscode());

        // normal
        cntl.Reset();
        request.set_filename("/file1");
        stub.GetAllocatedSize(&cntl, &request, &response, NULL);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(StatusCode::kOK, response.statuscode());
        ASSERT_EQ(DefaultSegmentSize, response.allocatedsize());
        ASSERT_EQ(1, response.allocsizemap_size());
        ASSERT_NE(response.allocsizemap().end(),
                  response.allocsizemap().find(0));
        ASSERT_EQ(DefaultSegmentSize, response.allocsizemap().at(0));
    }

    // test get file size
    {
        cntl.Reset();
        // file not exist
        GetFileSizeRequest request;
        GetFileSizeResponse response;
        request.set_filename("/file-not-exist");
        stub.GetFileSize(&cntl, &request, &response, NULL);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(StatusCode::kFileNotExists, response.statuscode());

        // normal
        cntl.Reset();
        request.set_filename("/file1");
        stub.GetFileSize(&cntl, &request, &response, NULL);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(StatusCode::kOK, response.statuscode());
        ASSERT_EQ(fileLength, response.filesize());
    }

    // test change owner
    {
        // 当前有文件 /file1(owner1) , /file2(owner2), /dir/file3(owner3)
        // changeowner success
        cntl.Reset();
        ChangeOwnerRequest request;
        ChangeOwnerResponse response;
        uint64_t date;
        std::string str2sig;
        std::string sig;

        request.set_filename("/file1");
        request.set_newowner("newowner1");
        date = TimeUtility::GetTimeofDayUs();
        str2sig = Authenticator::GetString2Signature(date,
                                                    authOptions.rootOwner);
        sig = Authenticator::CalcString2Signature(str2sig,
                                                    authOptions.rootPassword);
        request.set_rootowner(authOptions.rootOwner);
        request.set_signature(sig);
        request.set_date(date);
        stub.ChangeOwner(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(response.statuscode(), StatusCode::kOK);
        } else {
            ASSERT_TRUE(false);
        }

        // changeowner file owner == newowner
        cntl.Reset();
        request.set_filename("/file1");
        request.set_newowner("newowner1");
        date = TimeUtility::GetTimeofDayUs();
        str2sig = Authenticator::GetString2Signature(date,
                                                    authOptions.rootOwner);
        sig = Authenticator::CalcString2Signature(str2sig,
                                                    authOptions.rootPassword);
        request.set_rootowner(authOptions.rootOwner);
        request.set_signature(sig);
        request.set_date(date);
        stub.ChangeOwner(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(response.statuscode(), StatusCode::kOK);
        } else {
            ASSERT_TRUE(false);
        }

        // changeowner not root
        cntl.Reset();
        request.set_filename("/file1");
        request.set_newowner("owner1");
        date = TimeUtility::GetTimeofDayUs();
        str2sig = Authenticator::GetString2Signature(date,
                                                    authOptions.rootOwner);
        sig = Authenticator::CalcString2Signature(str2sig,
                                                    authOptions.rootPassword);
        request.set_rootowner("newowner1");
        request.set_signature(sig);
        request.set_date(date);
        stub.ChangeOwner(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(response.statuscode(), StatusCode::kOwnerAuthFail);
        } else {
            ASSERT_TRUE(false);
        }

        // changeowner signature calc mismatch
        cntl.Reset();
        request.set_filename("/file1");
        request.set_newowner("owner1");
        date = TimeUtility::GetTimeofDayUs();
        request.set_rootowner(authOptions.rootOwner);
        request.set_signature("wrongsignature");
        request.set_date(date);
        stub.ChangeOwner(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(response.statuscode(), StatusCode::kOwnerAuthFail);
        } else {
            ASSERT_TRUE(false);
        }

        // changeowner date timeout
        cntl.Reset();
        request.set_filename("/file1");
        request.set_newowner("owner1");
        date = TimeUtility::GetTimeofDayUs();
        str2sig = Authenticator::GetString2Signature(date,
                                                    authOptions.rootOwner);
        sig = Authenticator::CalcString2Signature(str2sig,
                                                    authOptions.rootPassword);
        request.set_rootowner(authOptions.rootOwner);
        request.set_signature(sig);
        request.set_date(date + kStaledRequestTimeIntervalUs * 2);
        stub.ChangeOwner(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(response.statuscode(), StatusCode::kOwnerAuthFail);
        } else {
            ASSERT_TRUE(false);
        }

        // changeowner back success
        cntl.Reset();
        request.set_filename("/file1");
        request.set_newowner("owner1");
        date = TimeUtility::GetTimeofDayUs();
        str2sig = Authenticator::GetString2Signature(date,
                                                    authOptions.rootOwner);
        sig = Authenticator::CalcString2Signature(str2sig,
                                                    authOptions.rootPassword);
        request.set_rootowner(authOptions.rootOwner);
        request.set_signature(sig);
        request.set_date(date);
        stub.ChangeOwner(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(response.statuscode(), StatusCode::kOK);
        } else {
            ASSERT_TRUE(false);
        }

        // changeowner 文件名不规范，失败
        cntl.Reset();
        request.set_filename("/file1/");
        request.set_newowner("owner1");
        date = TimeUtility::GetTimeofDayUs();
        str2sig = Authenticator::GetString2Signature(date,
                                                    authOptions.rootOwner);
        sig = Authenticator::CalcString2Signature(str2sig,
                                                    authOptions.rootPassword);
        request.set_rootowner(authOptions.rootOwner);
        request.set_signature(sig);
        request.set_date(date);
        stub.ChangeOwner(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(response.statuscode(), StatusCode::kParaError);
        } else {
            ASSERT_TRUE(false);
        }
    }

    // test RenameFile
    // 重命名到根目录下，非root owner，失败
    // fileinfoid不匹配，失败
    // 重命名成功 /dir/file3 -> /dir/file4
    // 原文件不存在，重命名失败
    // 重命名到根目录下，root owner，成功 /dir/file4 -> /file4
    // 文件名不规范，失败
    cntl.Reset();
    RenameFileRequest request4;
    RenameFileResponse response4;

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

    cntl.Reset();
    request4.set_oldfilename("/file4");
    request4.set_newfilename("/dir/file3");
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
    request4.set_newfilename("/file4");
    request4.set_owner("owner3");
    request4.set_date(TimeUtility::GetTimeofDayUs());
    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request4.set_oldfilename("/file4/");
    request4.set_newfilename("/file5");
    request4.set_owner("owner3");
    request4.set_date(TimeUtility::GetTimeofDayUs());
    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kParaError);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request4.set_oldfilename("/file4");
    request4.set_newfilename("/file5/");
    request4.set_owner("owner3");
    request4.set_date(TimeUtility::GetTimeofDayUs());
    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kParaError);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request4.set_oldfilename("/file4");
    request4.set_newfilename("/file4/file5");
    request4.set_owner("owner3");
    request4.set_date(TimeUtility::GetTimeofDayUs());
    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kParaError);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    request4.set_oldfilename("/file4");
    request4.set_newfilename("/dir/file5");
    request4.set_owner("owner3");
    request4.set_date(TimeUtility::GetTimeofDayUs());
    request4.set_oldfileid(10000);
    request4.set_newfileid(100);
    stub.RenameFile(&cntl, &request4, &response4, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response4.statuscode(), StatusCode::kFileIdNotMatch);
    } else {
        ASSERT_TRUE(false);
    }

    // test ExtendFile
    // 扩容file2,第一次扩大，成功；第二次缩小，失败
    // 扩容的文件名不符合规范，失败
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

    cntl.Reset();
    request5.set_filename("/file2/");
    request5.set_owner("owner2");
    request5.set_date(TimeUtility::GetTimeofDayUs());
    request5.set_newsize(newsize);
    stub.ExtendFile(&cntl, &request5, &response5, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response5.statuscode(), StatusCode::kParaError);
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

    // openFile case3, 文件名不符合规范
    OpenFileRequest request11;
    OpenFileResponse response11;
    cntl.Reset();
    request11.set_filename("/file2/");
    request11.set_owner("owner2");
    request11.set_date(TimeUtility::GetTimeofDayUs());

    stub.OpenFile(&cntl, &request11, &response11, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response11.statuscode(), StatusCode::kParaError);
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

    // CloseFile case2. 文件存在，session存在，返回成功
    CloseFileRequest request13;
    CloseFileResponse response13;
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

    // CloseFile case3. 文件名不符合规范
    cntl.Reset();
    request14.set_filename("/file2/");
    request14.set_owner("owner2");
    request14.set_date(TimeUtility::GetTimeofDayUs());
    request14.set_sessionid(response9.protosession().sessionid());

    stub.CloseFile(&cntl, &request14, &response14, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response14.statuscode(), StatusCode::kParaError);
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

    stub.RefreshSession(&cntl, &request15, &response15, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response15.statuscode(), StatusCode::kFileNotExists);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // RefreshSession case2. 文件名不符合规范
    ReFreshSessionRequest request18;
    ReFreshSessionResponse response18;
    cntl.Reset();

    request18.set_filename("/file1/");
    request18.set_owner("owner1");
    request18.set_date(TimeUtility::GetTimeofDayUs());
    request18.set_sessionid(response10.protosession().sessionid());
    request18.set_date(common::TimeUtility::GetTimeofDayUs());

    stub.RefreshSession(&cntl, &request18, &response18, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response18.statuscode(), StatusCode::kParaError);
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

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.Start("127.0.0.1", {8900, 8999}, &option));

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    CurveFSService_Stub stub(&channel);


    // test create file
    std::vector<PoolIdType> logicalPools{1, 2, 3};
    EXPECT_CALL(*topology_, GetLogicalPoolInCluster(_))
        .Times(AtLeast(1))
        .WillRepeatedly(Return(logicalPools));
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
        ASSERT_EQ(file.chunksize(), curveFSOptions.defaultChunkSize);
        ASSERT_EQ(file.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(file.length(), fileLength);
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
        ASSERT_EQ(snapshotFileInfo.filestatus(), FileStatus::kFileCreated);
        ASSERT_EQ(snapshotFileInfo.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    snapshotRequest.set_filename("/file1/");
    snapshotRequest.set_owner("owner1");
    snapshotRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.CreateSnapShot(&cntl, &snapshotRequest, &snapshotResponses, NULL);
    if (!cntl.Failed()) {
         ASSERT_EQ(snapshotResponses.statuscode(), StatusCode::kParaError);
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
        ASSERT_EQ(file.chunksize(), curveFSOptions.defaultChunkSize);
        ASSERT_EQ(file.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(file.length(), fileLength);
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

    cntl.Reset();
    checkRequest.set_filename("/file1/");
    checkRequest.set_owner("owner1");
    checkRequest.set_date(TimeUtility::GetTimeofDayUs());
    checkRequest.set_seq(1);
    stub.CheckSnapShotStatus(&cntl, &checkRequest, &checkResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(checkResponse.statuscode(), StatusCode::kParaError);
    } else {
        ASSERT_TRUE(false);
    }

    // get SnapShot segment
    cntl.Reset();
    GetOrAllocateSegmentRequest getSegmentRequest;
    GetOrAllocateSegmentResponse getSegmentResponse;
    getSegmentRequest.set_filename("/file1");
    getSegmentRequest.set_owner("owner1");
    getSegmentRequest.set_date(TimeUtility::GetTimeofDayUs());
    getSegmentRequest.set_offset(DefaultSegmentSize);
    getSegmentRequest.set_allocateifnotexist(false);
    getSegmentRequest.set_seqnum(1);
    stub.GetSnapShotFileSegment(&cntl, &getSegmentRequest,
                            &getSegmentResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getSegmentResponse.statuscode(),
                        StatusCode::kSegmentNotAllocated);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    getSegmentRequest.set_filename("/file1/");
    getSegmentRequest.set_owner("owner1");
    getSegmentRequest.set_date(TimeUtility::GetTimeofDayUs());
    getSegmentRequest.set_offset(DefaultSegmentSize);
    getSegmentRequest.set_allocateifnotexist(false);
    getSegmentRequest.set_seqnum(1);
    stub.GetSnapShotFileSegment(&cntl, &getSegmentRequest,
                            &getSegmentResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(getSegmentResponse.statuscode(), StatusCode::kParaError);
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

    cntl.Reset();
    deleteRequest.set_filename("/file1/");
    deleteRequest.set_owner("owner1");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    deleteRequest.set_seq(1);
    stub.DeleteSnapShot(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kParaError);
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

    cntl.Reset();
    listRequest.set_filename("/file1/");
    listRequest.set_owner("owner1");
    listRequest.set_date(TimeUtility::GetTimeofDayUs());
    listRequest.add_seq(2);
    stub.ListSnapShot(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), StatusCode::kParaError);
    } else {
        ASSERT_TRUE(false);
    }
    server.Stop(10);
    server.Join();
}

TEST_F(NameSpaceServiceTest, deletefiletests) {
    brpc::Server server;

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    using ::curve::chunkserver::MockChunkService;
    MockChunkService *chunkService = new MockChunkService();
    ASSERT_EQ(server.AddService(chunkService,
                                      brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.Start("127.0.0.1", {8900, 8999}, &option));
    butil::EndPoint listenAddr = server.listen_address();

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    CurveFSService_Stub stub(&channel);

    // 先创建文件/file1，目录/dir1，文件/dir1/file2
    std::vector<PoolIdType> logicalPools{1, 2, 3};
    EXPECT_CALL(*topology_, GetLogicalPoolInCluster(_))
        .Times(AtLeast(1))
        .WillRepeatedly(Return(logicalPools));
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
        ASSERT_EQ(file.chunksize(), curveFSOptions.defaultChunkSize);
        ASSERT_EQ(file.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(file.length(), fileLength);
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
        ASSERT_EQ(file.chunksize(), curveFSOptions.defaultChunkSize);
        ASSERT_EQ(file.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(file.length(), fileLength);
        ASSERT_EQ(file.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    // 文件/dir1/file2申请segment
    GetOrAllocateSegmentRequest allocRequest;
    GetOrAllocateSegmentResponse allocResponse;
    for (int i = 0; i < 10; i++) {
        cntl.Reset();
        allocRequest.set_filename("/dir1/file2");
        allocRequest.set_owner("owner");
        allocRequest.set_date(TimeUtility::GetTimeofDayUs());
        allocRequest.set_offset(DefaultSegmentSize * i);
        allocRequest.set_allocateifnotexist(true);
        stub.GetOrAllocateSegment(&cntl, &allocRequest, &allocResponse, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(allocResponse.statuscode(),
                                        StatusCode::kOK);
        } else {
            ASSERT_TRUE(false);
        }
    }

    // 开始测试删除文件逻辑
    // 1 如果文件有快照，那么删除文件返回kFileUnderSnapShot
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
        ASSERT_EQ(snapshotFileInfo.filestatus(), FileStatus::kFileCreated);
        ASSERT_EQ(snapshotFileInfo.seqnum(), 1);
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

    int attempts = 0;
    while (attempts < 100) {
        cntl.Reset();
        CheckSnapShotStatusRequest checkRequest;
        CheckSnapShotStatusResponse checkResponse;
        checkRequest.set_filename("/file1");
        checkRequest.set_owner("owner");
        checkRequest.set_date(TimeUtility::GetTimeofDayUs());
        checkRequest.set_seq(1);
        stub.CheckSnapShotStatus(&cntl, &checkRequest, &checkResponse, NULL);
        if (!cntl.Failed()) {
            if (checkResponse.statuscode() ==
                                    StatusCode::kSnapshotFileNotExists) {
                break;
            } else {
                ASSERT_EQ(checkResponse.statuscode(), StatusCode::kOK);
                sleep(1);
                ++attempts;
            }
        } else {
            ASSERT_TRUE(false);
            break;
        }
    }
    ASSERT_LE(attempts, 100)
            << "max attempts for check snapshot status exhausted";


    // 2 如果目录下有文件，那么删除目录返回kDirNotEmpty
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

    // 3 如果传入的fileid不匹配，删除文件失败
    cntl.Reset();
    DeleteFileRequest request5;
    DeleteFileResponse response5;
    request5.set_filename("/file1");
    request5.set_owner("owner");
    request5.set_date(TimeUtility::GetTimeofDayUs());
    request5.set_fileid(100000);

    stub.DeleteFile(&cntl, &request5, &response5, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response5.statuscode(), StatusCode::kFileIdNotMatch);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 4 删除文件/file1成功，查询文件已经删除
    cntl.Reset();
    request3.set_filename("/file1");
    request3.set_owner("owner");
    request3.set_date(TimeUtility::GetTimeofDayUs());

    auto dtime = TimeUtility::GetTimeofDaySec();
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

    // 查询垃圾箱
    ListDirRequest listRequest;
    ListDirResponse listResponse;
    cntl.Reset();
    uint64_t date = TimeUtility::GetTimeofDayUs();
    std::string str2sig = Authenticator::GetString2Signature(date,
                                            authOptions.rootOwner);
    std::string sig = Authenticator::CalcString2Signature(str2sig,
                                            authOptions.rootPassword);
    listRequest.set_signature(sig);
    listRequest.set_filename(RECYCLEBINDIR);
    listRequest.set_owner(authOptions.rootOwner);
    listRequest.set_date(date);
    stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), StatusCode::kOK);
        ASSERT_EQ(listResponse.fileinfo_size(), 1);
        FileInfo file = listResponse.fileinfo(0);
        ASSERT_TRUE(CheckFilename(file.filename(), dtime));  // file1-1-${dtime}
        ASSERT_EQ(file.filestatus(), FileStatus::kFileCreated);
        } else {
        ASSERT_TRUE(false);
    }

    // 删除文件/dir1/file2成功，删除目录/dir1成功，查询目录和文件均已经删除
    using ::curve::mds::topology::ChunkServerStatus;
    using ::curve::mds::topology::OnlineState;
    using ::curve::chunkserver::ChunkRequest;
    using ::curve::chunkserver::ChunkResponse;
    using ::curve::chunkserver::CHUNK_OP_STATUS;

    CopySetInfo copyset(1, 1);
    copyset.SetLeader(1);
    EXPECT_CALL(*topology_, GetCopySet(_, _))
            .WillRepeatedly(
                    DoAll(SetArgPointee<1>(copyset), Return(true)));
    ChunkServer chunkserver(1, "", "", 1, "127.0.0.1", listenAddr.port, "",
            ChunkServerStatus::READWRITE, OnlineState::ONLINE);
    EXPECT_CALL(*topology_, GetChunkServer(_, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(chunkserver), Return(true)));

    ChunkResponse chunkResponse;
    chunkResponse.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    EXPECT_CALL(*chunkService, DeleteChunk(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(chunkResponse),
                Invoke([](RpcController *controller,
                          const ChunkRequest *chunkRequest,
                          ChunkResponse *chunkResponse,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));

    stub_ = std::make_shared<CurveFSService_Stub>(&channel);

    // Delete "/dir1/file2"
    dtime = TimeUtility::GetTimeofDaySec();
    {
        RequestOption option;
        DeleteFileResponse response;
        ASSERT_TRUE(DeleteFile("/dir1/file2", option, &response));
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);

        GetFileInfoResponse response2;
        ASSERT_TRUE(GetFileInfo("/dir1/file2", option, &response2));
        ASSERT_EQ(response2.statuscode(), StatusCode::kFileNotExists);
    }

    // Delete "/dir1"
    {
        RequestOption option;
        DeleteFileResponse response;
        ASSERT_TRUE(DeleteFile("/dir1", option, &response));
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);

        GetFileInfoResponse response2;
        ASSERT_TRUE(GetFileInfo("/dir1", option, &response2));
        ASSERT_EQ(response2.statuscode(), StatusCode::kFileNotExists);
    }

    // List recycle bin
    std::vector<std::string> dfiles;
    {
        RequestOption option(true, false);
        ListDirResponse response;
        ASSERT_TRUE(ListDir(RECYCLEBINDIR, option, &response));
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
        ASSERT_EQ(response.fileinfo_size(), 2);

        for (int i = 0; i < 2; i++) {
            FileInfo file = response.fileinfo(i);
            ASSERT_TRUE(CheckFilename(file.filename(), dtime));
            ASSERT_EQ(response.fileinfo(i).filestatus(),
                      FileStatus::kFileCreated);
            dfiles.push_back(RECYCLEBINDIR + "/" + file.filename());
        }
    }

    // Force delete file in recycle bin, then list it
    {
        RequestOption option(true, true);
        DeleteFileResponse response;
        ASSERT_TRUE(DeleteFile(dfiles[0], option, &response));
        ASSERT_TRUE(DeleteFile(dfiles[1], option, &response));

        // Wait for clean task done
        ListDirResponse response2;
        bool success = false;
        for (int i = 1; i <= 100; i++) {
            ASSERT_TRUE(ListDir(RECYCLEBINDIR, option, &response2));
            if (0 == response2.fileinfo_size()) {
                success = true;
                break;
            }
            sleep(1);
        }

        ASSERT_TRUE(success) << "max attempts for list /RecycleBin exhausted";
    }

    // Delete file failed with invalid filename
    {
        RequestOption option;
        DeleteFileResponse response;
        ASSERT_TRUE(DeleteFile("/file1/", option, &response));
        ASSERT_EQ(response.statuscode(), StatusCode::kParaError);
    }

    server.Stop(10);
    server.Join();
    delete chunkService;
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
    ASSERT_EQ(IsRenamePathValid("/a/b", "/a"), false);
    ASSERT_EQ(IsRenamePathValid("/a", "/a/b"), false);
    ASSERT_EQ(IsRenamePathValid("/a", "/a"), true);
    ASSERT_EQ(IsRenamePathValid("/", "/a"), false);
    ASSERT_EQ(IsRenamePathValid("/a", "/"), false);
    ASSERT_EQ(IsRenamePathValid("/a/b", "/"), false);
    ASSERT_EQ(IsRenamePathValid("/", "/a/b"), false);
    ASSERT_EQ(IsRenamePathValid("/", "/"), false);
    ASSERT_EQ(IsRenamePathValid("/a", "/c"), true);
    ASSERT_EQ(IsRenamePathValid("/a/b", "/a/c"), true);
    ASSERT_EQ(IsRenamePathValid("/a/b", "/c"), true);
    ASSERT_EQ(IsRenamePathValid("/c", "/a/b"), true);
}

TEST_F(NameSpaceServiceTest, clonetest) {
    brpc::Server server;

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.Start("127.0.0.1", {8900, 8999}, &option));

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    CurveFSService_Stub stub(&channel);

    // create clone file
    CreateCloneFileRequest request;
    CreateCloneFileResponse response;
    brpc::Controller cntl;

    request.set_filename("/clonefile1");
    request.set_filetype(FileType::INODE_PAGEFILE);
    request.set_filelength(kMiniFileLength);
    request.set_seq(10);
    request.set_chunksize(curveFSOptions.defaultChunkSize);
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_owner("tom");
    request.set_clonesource("/sourcefile1");
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
        ASSERT_EQ(getResponse.statuscode(), StatusCode::kOK);
        ASSERT_EQ(fileInfo.filename(), "clonefile1");
        ASSERT_EQ(fileInfo.filetype(), FileType::INODE_PAGEFILE);
        ASSERT_EQ(fileInfo.owner(), "tom");
        ASSERT_EQ(fileInfo.chunksize(), curveFSOptions.defaultChunkSize);
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
        ASSERT_EQ(setResponse.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    server.Stop(10);
    server.Join();
}

TEST_F(NameSpaceServiceTest, listClientTest) {
    brpc::Server server;

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.Start("127.0.0.1", {8900, 8999}, &option));

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    CurveFSService_Stub stub(&channel);

    ListClientRequest request;
    ListClientResponse response;
    brpc::Controller cntl;

    cntl.set_log_id(1);

    stub.ListClient(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    server.Stop(10);
    server.Join();
}

TEST_F(NameSpaceServiceTest, listAllClientTest) {
    brpc::Server server;

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.Start("127.0.0.1", {8900, 8999}, &option));

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    CurveFSService_Stub stub(&channel);

    ListClientRequest request;
    ListClientResponse response;
    brpc::Controller cntl;

    request.set_listallclient(true);
    cntl.set_log_id(1);

    stub.ListClient(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    server.Stop(10);
    server.Join();
}

TEST_F(NameSpaceServiceTest, FindFileMountPointTest) {
    brpc::Server server;

    std::string testFileName = "/test_filename";
    std::string testClientIP = "127.0.0.1";
    uint32_t testClientPort = 1234;
    std::string testClientIP2 = "127.0.0.1";
    uint32_t testClientPort2 = 1235;

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(
        server.AddService(&namespaceService, brpc::SERVER_DOESNT_OWN_SERVICE),
        0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.Start("127.0.0.1", {8900, 8999}, &option));

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    fileRecordManager_->UpdateFileRecord(testFileName, "0.0.6", testClientIP,
                                         testClientPort);
    fileRecordManager_->UpdateFileRecord(testFileName, "1.0.0", testClientIP2,
                                         testClientPort2);

    CurveFSService_Stub stub(&channel);

    FindFileMountPointRequest request;
    request.set_filename("/test_filename");
    FindFileMountPointResponse response;
    brpc::Controller cntl;

    cntl.set_log_id(1);

    stub.FindFileMountPoint(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(response.statuscode(), StatusCode::kOK);
        ASSERT_EQ(2, response.clientinfo_size());

        ASSERT_EQ(response.clientinfo(0).ip(), testClientIP);
        ASSERT_EQ(response.clientinfo(0).port(), testClientPort);
        ASSERT_EQ(response.clientinfo(1).ip(), testClientIP2);
        ASSERT_EQ(response.clientinfo(1).port(), testClientPort2);
    } else {
        LOG(INFO) << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    request.Clear();
    response.Clear();
    cntl.Reset();

    request.set_filename("/test_filename100");
    cntl.set_log_id(1);

    stub.FindFileMountPoint(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
        ASSERT_NE(response.statuscode(), StatusCode::kOK);
    } else {
        LOG(INFO) << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    server.Stop(10);
    server.Join();
}

TEST_F(NameSpaceServiceTest, ListVolumesOnCopysets) {
    brpc::Server server;

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.Start("127.0.0.1", {8900, 8999}, &option));

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    CurveFSService_Stub stub(&channel);
    ListVolumesOnCopysetsRequest request;
    ListVolumesOnCopysetsResponse response;
    for (int i = 1; i <= 3; ++i) {
        auto copyset = request.add_copysets();
        copyset->set_logicalpoolid(i);
        copyset->set_copysetid(100 + i);
    }
    brpc::Controller cntl;
    stub.ListVolumesOnCopysets(&cntl, &request, &response, NULL);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(StatusCode::kOK, response.statuscode());
    ASSERT_EQ(0, response.filenames_size());
}

TEST_F(NameSpaceServiceTest, testRecoverFile) {
    brpc::Server server;

    // start server
    NameSpaceService namespaceService(new FileLockManager(8));
    ASSERT_EQ(server.AddService(&namespaceService,
            brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.Start("127.0.0.1", {8900, 8999}, &option));

    // init client
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    CurveFSService_Stub stub(&channel);

    // create file /file1，dir /dir1 and file /dir1/file2
    std::vector<PoolIdType> logicalPools{1, 2, 3};
    EXPECT_CALL(*topology_, GetLogicalPoolInCluster(_))
        .Times(AtLeast(1))
        .WillRepeatedly(Return(logicalPools));
    CreateFileRequest createRequest;
    CreateFileResponse createResponse;
    brpc::Controller cntl;
    uint64_t fileLength = kMiniFileLength;
    createRequest.set_filename("/file1");
    createRequest.set_owner("owner");
    createRequest.set_date(TimeUtility::GetTimeofDayUs());
    createRequest.set_filetype(INODE_PAGEFILE);
    createRequest.set_filelength(fileLength);
    stub.CreateFile(&cntl,  &createRequest, &createResponse,  NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    createRequest.set_filename("/dir1");
    createRequest.set_owner("owner");
    createRequest.set_date(TimeUtility::GetTimeofDayUs());
    createRequest.set_filetype(INODE_DIRECTORY);
    createRequest.set_filelength(0);
    stub.CreateFile(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    cntl.Reset();
    createRequest.set_filename("/dir1/file2");
    createRequest.set_owner("owner");
    createRequest.set_date(TimeUtility::GetTimeofDayUs());
    createRequest.set_filetype(INODE_PAGEFILE);
    createRequest.set_filelength(fileLength);
    stub.CreateFile(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    // check file status, /file1，/dir1，/dir1/file2
    cntl.Reset();
    GetFileInfoRequest getRequest;
    GetFileInfoResponse getResponse;
    getRequest.set_filename("/file1");
    getRequest.set_owner("owner");
    getRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        FileInfo  file = getResponse.fileinfo();
        ASSERT_EQ(getResponse.statuscode(), StatusCode::kOK);
        ASSERT_EQ(file.id(), 1);
        ASSERT_EQ(file.filename(), "file1");
        ASSERT_EQ(file.parentid(), 0);
        ASSERT_EQ(file.filetype(), INODE_PAGEFILE);
        ASSERT_EQ(file.chunksize(), curveFSOptions.defaultChunkSize);
        ASSERT_EQ(file.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(file.length(), fileLength);
        ASSERT_EQ(file.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    getRequest.set_filename("/dir1");
    getRequest.set_owner("owner");
    getRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        FileInfo  file = getResponse.fileinfo();
        ASSERT_EQ(getResponse.statuscode(), StatusCode::kOK);
        ASSERT_EQ(file.id(), 2);
        ASSERT_EQ(file.filename(), "dir1");
        ASSERT_EQ(file.parentid(), 0);
        ASSERT_EQ(file.filetype(), INODE_DIRECTORY);
        ASSERT_EQ(file.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    getRequest.set_filename("/dir1/file2");
    getRequest.set_owner("owner");
    getRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.GetFileInfo(&cntl, &getRequest, &getResponse, NULL);
    if (!cntl.Failed()) {
        FileInfo file = getResponse.fileinfo();
        ASSERT_EQ(getResponse.statuscode(), StatusCode::kOK);
        ASSERT_EQ(file.id(), 3);
        ASSERT_EQ(file.filename(), "file2");
        ASSERT_EQ(file.filetype(), INODE_PAGEFILE);
        ASSERT_EQ(file.chunksize(), curveFSOptions.defaultChunkSize);
        ASSERT_EQ(file.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(file.length(), fileLength);
        ASSERT_EQ(file.seqnum(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    // /dir1/file2 apply segment
    GetOrAllocateSegmentRequest allocRequest;
    GetOrAllocateSegmentResponse allocResponse;
    for (int i = 0; i < 10; i++) {
        cntl.Reset();
        allocRequest.set_filename("/dir1/file2");
        allocRequest.set_owner("owner");
        allocRequest.set_date(TimeUtility::GetTimeofDayUs());
        allocRequest.set_offset(DefaultSegmentSize * i);
        allocRequest.set_allocateifnotexist(true);
        stub.GetOrAllocateSegment(&cntl, &allocRequest, &allocResponse, NULL);
        if (!cntl.Failed()) {
            ASSERT_EQ(allocResponse.statuscode(),
                                        StatusCode::kOK);
        } else {
            ASSERT_TRUE(false);
        }
    }

    // recover file successfully
    // 1. delete file /dir1/file2
    cntl.Reset();
    auto dtime = TimeUtility::GetTimeofDaySec();
    DeleteFileRequest deleteRequest;
    DeleteFileResponse deleteResponse;
    deleteRequest.set_filename("/dir1/file2");
    deleteRequest.set_owner("owner");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.DeleteFile(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 2. check the RecycleBIn
    cntl.Reset();
    ListDirRequest listRequest;
    ListDirResponse listResponse;
    uint64_t date = TimeUtility::GetTimeofDayUs();
    std::string str2sig = Authenticator::GetString2Signature(date,
                                            authOptions.rootOwner);
    std::string sig = Authenticator::CalcString2Signature(str2sig,
                                            authOptions.rootPassword);
    listRequest.set_signature(sig);
    listRequest.set_filename(RECYCLEBINDIR);
    listRequest.set_owner(authOptions.rootOwner);
    listRequest.set_date(date);
    stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), StatusCode::kOK);
        FileInfo file = listResponse.fileinfo(0);
        ASSERT_TRUE(CheckFilename(file.filename(), dtime));  // file2-3-${dtime}
        ASSERT_EQ(file.owner(), "owner");
        ASSERT_EQ(file.originalfullpathname(), "/dir1/file2");
        ASSERT_EQ(file.filestatus(), FileStatus::kFileCreated);
        ASSERT_EQ(listResponse.fileinfo_size(), 1);
        } else {
        ASSERT_TRUE(false);
    }

    // 3. recover file
    cntl.Reset();
    RecoverFileRequest recoverRequest;
    RecoverFileResponse recoverRresponse;
    recoverRequest.set_filename("/dir1/file2");
    recoverRequest.set_owner("owner");
    recoverRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.RecoverFile(&cntl, &recoverRequest, &recoverRresponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(recoverRresponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 4. check the file recovered successfully
    cntl.Reset();
    date = TimeUtility::GetTimeofDayUs();
    listRequest.set_filename("/dir1");
    listRequest.set_owner("owner");
    listRequest.set_date(date);
    stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), StatusCode::kOK);
        FileInfo file = listResponse.fileinfo(0);
        ASSERT_EQ(file.filename(), "file2");
        ASSERT_EQ(file.filestatus(), FileStatus::kFileCreated);
        ASSERT_EQ(listResponse.fileinfo_size(), 1);
        } else {
        ASSERT_TRUE(false);
    }

    // the same file exit, recover fail
    // 1. delete file /dir1/file2
    cntl.Reset();
    deleteRequest.set_filename("/dir1/file2");
    deleteRequest.set_owner("owner");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.DeleteFile(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 2. make the file /dir1/file2
    cntl.Reset();
    createRequest.set_filename("/dir1/file2");
    createRequest.set_owner("owner");
    createRequest.set_date(TimeUtility::GetTimeofDayUs());
    createRequest.set_filetype(INODE_PAGEFILE);
    createRequest.set_filelength(fileLength);
    stub.CreateFile(&cntl, &createRequest, &createResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createResponse.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    // 3. recover file /dir1/file2
    cntl.Reset();
    recoverRequest.set_filename("/dir1/file2");
    recoverRequest.set_owner("owner");
    recoverRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.RecoverFile(&cntl, &recoverRequest, &recoverRresponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(recoverRresponse.statuscode(), StatusCode::kFileExists);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // delete the files with same name and recover the new one
    // 1. delete file /dir1/file2
    cntl.Reset();
    deleteRequest.set_filename("/dir1/file2");
    deleteRequest.set_owner("owner");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.DeleteFile(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 2. recover the file /dir1/file2
    cntl.Reset();
    recoverRequest.set_filename("/dir1/file2");
    recoverRequest.set_owner("owner");
    recoverRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.RecoverFile(&cntl, &recoverRequest, &recoverRresponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(recoverRresponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 3. check the ctime of recovered file is greater than the other in recyclebin //NOLINT
    FileInfo recycleFile;
    cntl.Reset();
    date = TimeUtility::GetTimeofDayUs();
    str2sig = Authenticator::GetString2Signature(date,
                                            authOptions.rootOwner);
    sig = Authenticator::CalcString2Signature(str2sig,
                                            authOptions.rootPassword);
    listRequest.set_signature(sig);
    listRequest.set_filename(RECYCLEBINDIR);
    listRequest.set_owner(authOptions.rootOwner);
    listRequest.set_date(date);
    stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), StatusCode::kOK);
        recycleFile = listResponse.fileinfo(0);
        ASSERT_EQ(recycleFile.owner(), "owner");
        ASSERT_EQ(recycleFile.originalfullpathname(), "/dir1/file2");
        ASSERT_EQ(listResponse.fileinfo_size(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    date = TimeUtility::GetTimeofDayUs();
    listRequest.set_filename("/dir1");
    listRequest.set_owner("owner");
    listRequest.set_date(date);
    stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), StatusCode::kOK);
        ASSERT_EQ(listResponse.fileinfo_size(), 1);
        FileInfo file = listResponse.fileinfo(0);
        ASSERT_EQ(file.filename(), "file2");
        ASSERT_EQ(file.id(), 4);
        ASSERT_GT(file.ctime(), recycleFile.ctime());
    } else {
        ASSERT_TRUE(false);
    }

    // delete the files by fileId
    // 1. delete file /dir1/file2(id=4)
    cntl.Reset();
    deleteRequest.set_filename("/dir1/file2");
    deleteRequest.set_owner("owner");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.DeleteFile(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 2. recover the file /dir1/file2(id=3)
    cntl.Reset();
    recoverRequest.set_filename("/dir1/file2");
    recoverRequest.set_owner("owner");
    recoverRequest.set_date(TimeUtility::GetTimeofDayUs());
    recoverRequest.set_fileid(3);
    stub.RecoverFile(&cntl, &recoverRequest, &recoverRresponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(recoverRresponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 3. check the fileId of recovered file 3 and not recovered is 4
    cntl.Reset();
    date = TimeUtility::GetTimeofDayUs();
    str2sig = Authenticator::GetString2Signature(date,
                                            authOptions.rootOwner);
    sig = Authenticator::CalcString2Signature(str2sig,
                                            authOptions.rootPassword);
    listRequest.set_signature(sig);
    listRequest.set_filename(RECYCLEBINDIR);
    listRequest.set_owner(authOptions.rootOwner);
    listRequest.set_date(date);
    stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), StatusCode::kOK);
        recycleFile = listResponse.fileinfo(0);
        ASSERT_EQ(recycleFile.owner(), "owner");
        ASSERT_EQ(recycleFile.originalfullpathname(), "/dir1/file2");
        ASSERT_EQ(recycleFile.id(), 4);
        ASSERT_EQ(listResponse.fileinfo_size(), 1);
    } else {
        ASSERT_TRUE(false);
    }

    cntl.Reset();
    date = TimeUtility::GetTimeofDayUs();
    listRequest.set_filename("/dir1");
    listRequest.set_owner("owner");
    listRequest.set_date(date);
    stub.ListDir(&cntl, &listRequest, &listResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(listResponse.statuscode(), StatusCode::kOK);
        ASSERT_EQ(listResponse.fileinfo_size(), 1);
        FileInfo file = listResponse.fileinfo(0);
        ASSERT_EQ(file.filename(), "file2");
        ASSERT_EQ(file.id(), 3);
    } else {
        ASSERT_TRUE(false);
    }

    // recover file which upper dir not exist
    // 1. delete file /dir1/file2
    cntl.Reset();
    deleteRequest.set_filename("/dir1/file2");
    deleteRequest.set_owner("owner");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.DeleteFile(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 2. delete dir /dir1
    cntl.Reset();
    deleteRequest.set_filename("/dir1");
    deleteRequest.set_owner("owner");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.DeleteFile(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 3. recover file /dir1/file2
    cntl.Reset();
    recoverRequest.set_filename("/dir1/file2");
    recoverRequest.set_owner("owner");
    recoverRequest.set_fileid(0);
    recoverRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.RecoverFile(&cntl, &recoverRequest, &recoverRresponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(recoverRresponse.statuscode(), StatusCode::kFileNotExists);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // recover file not exist in recyclebin
    cntl.Reset();
    recoverRequest.set_filename("/file3");
    recoverRequest.set_owner("owner");
    recoverRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.RecoverFile(&cntl, &recoverRequest, &recoverRresponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(recoverRresponse.statuscode(), StatusCode::kFileNotExists);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // recover filename is invalid
    cntl.Reset();
    recoverRequest.set_filename("//file1");
    recoverRequest.set_owner("owner");
    recoverRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.RecoverFile(&cntl, &recoverRequest, &recoverRresponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(recoverRresponse.statuscode(), StatusCode::kParaError);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // recover file in cloneMetaInstalled status
    // 1. create clone file /file3
    CreateCloneFileRequest createCloneRequest;
    CreateCloneFileResponse createCloneResponse;
    createCloneRequest.set_filename("/file3");
    createCloneRequest.set_filetype(FileType::INODE_PAGEFILE);
    createCloneRequest.set_filelength(kMiniFileLength);
    createCloneRequest.set_seq(10);
    createCloneRequest.set_chunksize(curveFSOptions.defaultChunkSize);
    createCloneRequest.set_date(TimeUtility::GetTimeofDayUs());
    createCloneRequest.set_owner("owner");
    createCloneRequest.set_clonesource("/sourcefile1");
    cntl.Reset();
    stub.CreateCloneFile(&cntl, &createCloneRequest,
                         &createCloneResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createCloneResponse.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    // 2. set the filestatus
    SetCloneFileStatusRequest setRequest;
    SetCloneFileStatusResponse setResponse;
    cntl.Reset();
    setRequest.set_filename("/file3");
    setRequest.set_owner("owner");
    setRequest.set_date(TimeUtility::GetTimeofDayUs());
    setRequest.set_filestatus(FileStatus::kFileCloneMetaInstalled);
    stub.SetCloneFileStatus(&cntl, &setRequest, &setResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(setResponse.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    // 3. delete the file /file3
    cntl.Reset();
    deleteRequest.set_filename("/file3");
    deleteRequest.set_owner("owner");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.DeleteFile(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 4. recover file
    cntl.Reset();
    recoverRequest.set_filename("/file3");
    recoverRequest.set_owner("owner");
    recoverRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.RecoverFile(&cntl, &recoverRequest, &recoverRresponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(recoverRresponse.statuscode(),
        StatusCode::kRecoverFileCloneMetaInstalled);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // recover file in cloning status
    // 1. create clone file /file4
    createCloneRequest.set_filename("/file4");
    createCloneRequest.set_filetype(FileType::INODE_PAGEFILE);
    createCloneRequest.set_filelength(kMiniFileLength);
    createCloneRequest.set_seq(10);
    createCloneRequest.set_chunksize(curveFSOptions.defaultChunkSize);
    createCloneRequest.set_date(TimeUtility::GetTimeofDayUs());
    createCloneRequest.set_owner("owner");
    createCloneRequest.set_clonesource("/sourcefile1");
    cntl.Reset();
    stub.CreateCloneFile(&cntl, &createCloneRequest,
                         &createCloneResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(createCloneResponse.statuscode(), StatusCode::kOK);
    } else {
        ASSERT_TRUE(false);
    }

    // 2. set the filestatus
    cntl.Reset();
    setRequest.set_filename("/file4");
    setRequest.set_owner("owner");
    setRequest.set_date(TimeUtility::GetTimeofDayUs());
    setRequest.set_filestatus(FileStatus::kFileCloning);
    stub.SetCloneFileStatus(&cntl, &setRequest, &setResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(setResponse.statuscode(), StatusCode::kOK);
    } else {
        FAIL();
    }

    // 3. delete the file /file4
    cntl.Reset();
    deleteRequest.set_filename("/file4");
    deleteRequest.set_owner("owner");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.DeleteFile(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 4. recover file
    cntl.Reset();
    recoverRequest.set_filename("/file4");
    recoverRequest.set_owner("owner");
    recoverRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.RecoverFile(&cntl, &recoverRequest, &recoverRresponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(recoverRresponse.statuscode(),
        StatusCode::kRecoverFileError);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // recover file with mismatch inodeid
    // 1. delete file /file1
    cntl.Reset();
    deleteRequest.set_filename("/file1");
    deleteRequest.set_owner("owner");
    deleteRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.DeleteFile(&cntl, &deleteRequest, &deleteResponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(deleteResponse.statuscode(), StatusCode::kOK);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    // 2. recover file with mismatch inodeid
    cntl.Reset();
    recoverRequest.set_filename("/file1");
    recoverRequest.set_owner("owner");
    recoverRequest.set_fileid(1000);
    recoverRequest.set_date(TimeUtility::GetTimeofDayUs());
    stub.RecoverFile(&cntl, &recoverRequest, &recoverRresponse, NULL);
    if (!cntl.Failed()) {
        ASSERT_EQ(recoverRresponse.statuscode(), StatusCode::kFileIdNotMatch);
    } else {
        std::cout << cntl.ErrorText();
        ASSERT_TRUE(false);
    }

    server.Stop(10);
    server.Join();
}

TEST_F(NameSpaceServiceTest, TestDeAllocateSegment) {
    brpc::Server server;

    NameSpaceService nameSpaceSerivce(new FileLockManager(13));

    ASSERT_EQ(0, server.AddService(&nameSpaceSerivce,
                                   brpc::SERVER_DOESNT_OWN_SERVICE));

    brpc::ServerOptions opt;
    opt.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.Start("127.0.0.1", {8900, 8999}, &opt));

    // init channel
    brpc::Channel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

    CurveFSService_Stub stub(&channel);
    brpc::Controller cntl;
    uint64_t fileLength = 100 * kGB;
    std::string filename = "/TestDeAllocateSegment";
    std::string owner = "curve";

    // create file and allocate segment
    {
        std::vector<PoolIdType> logicalPools{1, 2, 3};
         EXPECT_CALL(*topology_, GetLogicalPoolInCluster(_))
         .Times(AtLeast(1))
         .WillRepeatedly(Return(logicalPools));
        CreateFileRequest createRequest;
        CreateFileResponse createResponse;
        createRequest.set_filename(filename);
        createRequest.set_owner(owner);
        createRequest.set_date(TimeUtility::GetTimeofDayUs());
        createRequest.set_filetype(INODE_PAGEFILE);
        createRequest.set_filelength(fileLength);

        cntl.set_log_id(1);
        stub.CreateFile(&cntl, &createRequest, &createResponse, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(StatusCode::kOK, createResponse.statuscode());

        // allocate segment
        cntl.Reset();
        GetOrAllocateSegmentRequest allocateRequest;
        GetOrAllocateSegmentResponse allocateResponse;

        allocateRequest.set_filename(filename);
        allocateRequest.set_offset(50ull * kGB);
        allocateRequest.set_allocateifnotexist(true);
        allocateRequest.set_owner(owner);
        allocateRequest.set_date(TimeUtility::GetTimeofDayUs());

        stub.GetOrAllocateSegment(&cntl, &allocateRequest, &allocateResponse,
                                  nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(StatusCode::kOK, allocateResponse.statuscode());
    }

    // 1. filename not valid
    {
        cntl.Reset();
        DeAllocateSegmentRequest request;
        DeAllocateSegmentResponse response;

        request.set_filename(filename + "/");
        request.set_offset(0);
        request.set_owner("curve");
        request.set_date(TimeUtility::GetTimeofDayUs());

        stub.DeAllocateSegment(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(StatusCode::kParaError, response.statuscode());
    }

    // 2. segment not allocated
    {
        cntl.Reset();
        DeAllocateSegmentRequest request;
        DeAllocateSegmentResponse response;

        request.set_filename(filename);
        request.set_offset(0);
        request.set_owner(owner);
        request.set_date(TimeUtility::GetTimeofDayUs());

        stub.DeAllocateSegment(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(StatusCode::kSegmentNotAllocated, response.statuscode());
    }

    // 3. deallocate success
    {
        cntl.Reset();
        DeAllocateSegmentRequest request;
        DeAllocateSegmentResponse response;

        request.set_filename(filename);
        request.set_offset(50ull * kGB);
        request.set_owner(owner);
        request.set_date(TimeUtility::GetTimeofDayUs());

        stub.DeAllocateSegment(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(StatusCode::kOK, response.statuscode());
    }

    // 4. retry rpc
    {
        cntl.Reset();
        DeAllocateSegmentRequest request;
        DeAllocateSegmentResponse response;

        request.set_filename(filename);
        request.set_offset(50ull * kGB);
        request.set_owner(owner);
        request.set_date(TimeUtility::GetTimeofDayUs());

        stub.DeAllocateSegment(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(StatusCode::kSegmentNotAllocated, response.statuscode());
    }
}

}  // namespace mds
}  // namespace curve
