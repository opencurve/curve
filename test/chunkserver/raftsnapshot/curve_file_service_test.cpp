/*
 * Project: curve
 * Created Date: 2020-06-17
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include "src/chunkserver/raftsnapshot/curve_file_service.h"
#include "src/chunkserver/raftsnapshot/curve_filesystem_adaptor.h"
#include "test/chunkserver/raftsnapshot/mock_file_reader.h"
#include "test/chunkserver/raftsnapshot/mock_snapshot_attachment.h"

namespace curve {
namespace chunkserver {

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::DoAll;
using ::testing::SetArgPointee;

const char serverAddr[] = "127.0.0.1:9501";

class CurveFileServiceTest : public testing::Test {
 protected:
    static void SetUpTestCase() {
        ASSERT_EQ(0, server_.AddService(&kCurveFileService,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(serverAddr, nullptr));
    }
    static void TearDownTestCase() {
        server_.Stop(0);
        server_.Join();
    }

    void SetUp() {
        reader_ = new MockFileReader(new CurveFilesystemAdaptor(),
                    new braft::ThroughputSnapshotThrottle(1, 1));
        attachment_ = new MockSnapshotAttachment();
    }

    static brpc::Server server_;
    scoped_refptr<MockFileReader> reader_;
    scoped_refptr<MockSnapshotAttachment> attachment_;
};

brpc::Server CurveFileServiceTest::server_;

TEST_F(CurveFileServiceTest, success_normal_file) {
    int64_t reader_id;
    ASSERT_EQ(0, kCurveFileService.add_reader(reader_, &reader_id));
    butil::IOBuf buf;
    buf.append("file1\nfile2\n");
    EXPECT_CALL(*reader_, read_file(_, _, _, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<0>(buf), Return(0)));
    std::string path = "/test";
    EXPECT_CALL(*reader_, path())
        .WillRepeatedly(ReturnRef(path));
    brpc::Channel channel;
    brpc::Controller cntl;
    ASSERT_EQ(channel.Init(serverAddr, nullptr), 0);
    braft::FileService_Stub stub(&channel);
    braft::GetFileRequest request;
    request.set_reader_id(reader_id);
    request.set_filename("test");
    request.set_count(buf.size());
    request.set_offset(0);
    braft::GetFileResponse response;
    stub.get_file(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    kCurveFileService.remove_reader(reader_id);
}

TEST_F(CurveFileServiceTest, success_attach_file) {
    int64_t reader_id;
    ASSERT_EQ(0, kCurveFileService.add_reader(reader_, &reader_id));
    kCurveFileService.set_snapshot_attachment(attachment_);
    std::string path = "/test";
    EXPECT_CALL(*reader_, path())
        .WillRepeatedly(ReturnRef(path));
    std::vector<std::string> files = {"file1", "file2", "file3"};
    EXPECT_CALL(*attachment_, list_attach_files(_, _))
        .WillOnce(SetArgPointee<0>(files));
    brpc::Channel channel;
    brpc::Controller cntl;
    ASSERT_EQ(channel.Init(serverAddr, nullptr), 0);
    braft::FileService_Stub stub(&channel);
    braft::GetFileRequest request;
    request.set_reader_id(reader_id);
    request.set_filename(BRAFT_SNAPSHOT_ATTACH_META_FILE);
    request.set_count(1024);
    request.set_offset(0);
    braft::GetFileResponse response;
    stub.get_file(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    kCurveFileService.remove_reader(reader_id);
    kCurveFileService.set_snapshot_attachment(NULL);
}

TEST_F(CurveFileServiceTest, error_reader_not_found) {
    brpc::Channel channel;
    brpc::Controller cntl;
    ASSERT_EQ(channel.Init(serverAddr, nullptr), 0);
    braft::FileService_Stub stub(&channel);
    braft::GetFileRequest request;
    request.set_reader_id(111);
    request.set_filename("test");
    request.set_count(1024);
    request.set_offset(0);
    braft::GetFileResponse response;
    stub.get_file(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ(ENXIO, cntl.ErrorCode());
}

TEST_F(CurveFileServiceTest, error_request_invalid) {
    int64_t reader_id;
    ASSERT_EQ(0, kCurveFileService.add_reader(reader_, &reader_id));
    std::string path = "/test";
    EXPECT_CALL(*reader_, path())
        .WillRepeatedly(ReturnRef(path));
    brpc::Channel channel;
    brpc::Controller cntl;
    ASSERT_EQ(channel.Init(serverAddr, nullptr), 0);
    braft::FileService_Stub stub(&channel);
    braft::GetFileRequest request;
    request.set_reader_id(reader_id);
    request.set_filename("test");
    request.set_count(0);
    request.set_offset(0);
    braft::GetFileResponse response;
    stub.get_file(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ(brpc::EREQUEST, cntl.ErrorCode());

    request.set_offset(-1);
    stub.get_file(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ(brpc::EREQUEST, cntl.ErrorCode());
    kCurveFileService.remove_reader(reader_id);
}

TEST_F(CurveFileServiceTest, error_normal_file_read_fail) {
    int64_t reader_id;
    ASSERT_EQ(0, kCurveFileService.add_reader(reader_, &reader_id));
    std::string path = "/test";
    EXPECT_CALL(*reader_, path())
        .WillRepeatedly(ReturnRef(path));
    EXPECT_CALL(*reader_, read_file(_, _, _, _, _, _, _))
        .WillOnce(Return(EPERM));
    brpc::Channel channel;
    brpc::Controller cntl;
    ASSERT_EQ(channel.Init(serverAddr, nullptr), 0);
    braft::FileService_Stub stub(&channel);
    braft::GetFileRequest request;
    request.set_reader_id(reader_id);
    request.set_filename("test");
    request.set_count(1024);
    request.set_offset(0);
    braft::GetFileResponse response;
    stub.get_file(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ(EPERM, cntl.ErrorCode());
    kCurveFileService.remove_reader(reader_id);
}

TEST_F(CurveFileServiceTest, snapshot_attachment_not_set) {
    int64_t reader_id;
    ASSERT_EQ(0, kCurveFileService.add_reader(reader_, &reader_id));
    std::string path = "/test";
    EXPECT_CALL(*reader_, path())
        .WillRepeatedly(ReturnRef(path));
    brpc::Channel channel;
    brpc::Controller cntl;
    ASSERT_EQ(channel.Init(serverAddr, nullptr), 0);
    braft::FileService_Stub stub(&channel);
    braft::GetFileRequest request;
    request.set_reader_id(reader_id);
    request.set_filename(BRAFT_SNAPSHOT_ATTACH_META_FILE);
    request.set_count(1024);
    request.set_offset(0);
    braft::GetFileResponse response;
    stub.get_file(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(0, response.read_size());
    ASSERT_EQ(true, response.eof());
    kCurveFileService.remove_reader(reader_id);
}

TEST(getCurveRaftBaseDir, test) {
    const struct {
        std::string first;
        std::string second;
        std::string expect;
    }testCases[] = {
        {
            "/data/copyset/3121/raft_snapshot/snapshot_sds",
            "raft_snapshot",
            "/data/copyset/3121/"
        },
        {
            "/raft_snapshot",
            "raft_snapshot",
            "/",
        },
        {
            "raft_snapshot",
            "raft_snapshot",
            "",
        },
        {
            "/data/copyset/3121/raftsnapshot/snapshot_sds",
            "raft_snapshot",
            "",
        }
    };

    for (int i = 0; i < sizeof(testCases)/ sizeof(testCases[0]); i++) {
        auto ret = getCurveRaftBaseDir(testCases[i].first,
                    testCases[i].second);
        ASSERT_EQ(ret , testCases[i].expect);
    }
}

}  // namespace chunkserver
}  // namespace curve
