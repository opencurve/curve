/*
 * Project: curve
 * Created Date: Fri Jan 04 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */


#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <memory>

#include "src/snapshotcloneserver/snapshotclone_service.h"
#include "test/snapshotcloneserver/mock_snapshot_server.h"

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;

namespace curve {
namespace snapshotcloneserver {

class TestSnapshotCloneServiceImpl : public ::testing::Test {
 protected:
    TestSnapshotCloneServiceImpl() {}
    ~TestSnapshotCloneServiceImpl() {}

    virtual void SetUp() {
        listenAddr_ = "127.0.0.1:5555";
        server_ = new brpc::Server();

        snapshotManager_ = std::make_shared<MockSnapshotServiceManager>();
        cloneManager_ = std::make_shared<MockCloneServiceManager>();

        SnapshotCloneServiceImpl *snapService =
            new SnapshotCloneServiceImpl(snapshotManager_, cloneManager_);

        ASSERT_EQ(0, server_->AddService(snapService,
                brpc::SERVER_OWNS_SERVICE));

        ASSERT_EQ(0, server_->Start(listenAddr_.c_str(), nullptr));
    }

    virtual void TearDown() {
        snapshotManager_ = nullptr;
        cloneManager_ = nullptr;
        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
    }

 protected:
    std::shared_ptr<MockSnapshotServiceManager> snapshotManager_;
    std::shared_ptr<MockCloneServiceManager> cloneManager_;
    std::string listenAddr_;
    brpc::Server *server_;
};

TEST_F(TestSnapshotCloneServiceImpl, TestCreateSnapShotSuccess) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*snapshotManager_, CreateSnapshot(_, _, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(uuid),
                    Return(kErrCodeSnapshotServerSuccess)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=CreateSnapshot&Version=1&User=test&File=test&Name=test"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestDeleteSnapShotSuccess) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";

    EXPECT_CALL(*snapshotManager_, DeleteSnapshot(uuid, user, file))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=DeleteSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestCancelSnapShotSuccess) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";

    EXPECT_CALL(*snapshotManager_, CancelSnapshot(uuid, user, file))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=CancelSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestGetFileSnapshotInfoSuccess) {
    std::string file = "test";
    std::string user = "test";

    std::vector<FileSnapshotInfo> info;
    EXPECT_CALL(*snapshotManager_, GetFileSnapshotInfo(file, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(info),
                    Return(kErrCodeSnapshotServerSuccess)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=GetFileSnapshotInfo&Version=1&User=test&File=test&Limit=10"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestActionIsNull) {
    std::string file = "test";
    std::string user = "test";

    std::vector<FileSnapshotInfo> info;

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Version=1&User=test&File=test&Limit=10"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestCreateSnapShotMissingParam) {
    UUID uuid = "uuid1";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=CreateSnapshot&Version=1&User=test&Name=test"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestDeleteSnapShotMissingParam) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=DeleteSnapshot&Version=1&User=test&File=test"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestCancelSnapShotMissingParam) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";


    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=CancelSnapshot&Version=1&User=test&File=test"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestGetFileSnapshotInfoMissingParam) {
    std::string file = "test";
    std::string user = "test";

    std::vector<FileSnapshotInfo> info;

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=GetFileSnapshotInfo&Version=1&User=test&Limit=10"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestCreateSnapShotFail) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*snapshotManager_, CreateSnapshot(_, _, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(uuid),
                    Return(kErrCodeSnapshotInternalError)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=CreateSnapshot&Version=1&User=test&File=test&Name=test"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestDeleteSnapShotFail) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";

    EXPECT_CALL(*snapshotManager_, DeleteSnapshot(uuid, user, file))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=DeleteSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestCancelSnapShotFail) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";

    EXPECT_CALL(*snapshotManager_, CancelSnapshot(uuid, user, file))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=CancelSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestGetFileSnapshotInfoFail) {
    std::string file = "test";
    std::string user = "test";

    std::vector<FileSnapshotInfo> info;
    EXPECT_CALL(*snapshotManager_, GetFileSnapshotInfo(file, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(info),
                    Return(kErrCodeSnapshotInternalError)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=GetFileSnapshotInfo&Version=1&User=test&File=test&Limit=10"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestCreateSnapShotBadRequest) {
    UUID uuid = "uuid1";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=xxx&Version=1&User=test&File=test&Name=test"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestCloneFileSuccess) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*cloneManager_, CloneFile(_, _, _, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=Clone&Version=1&User=test&Source=abc&Destination=file1&Lazy=true"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestRecoverFileSuccess) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*cloneManager_, RecoverFile(_, _, _, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=Recover&Version=1&User=test&Source=abc&Destination=file1&Lazy=true"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}

TEST_F(TestSnapshotCloneServiceImpl, TestGetCloneTaskSuccess) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*cloneManager_, GetCloneTaskInfo(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:5555/SnapshotCloneService?Action=GetCloneTasks&Version=1&User=test"; //NOLINT

    if (channel.Init(url.c_str(), "", &option) != 0) {
        FAIL() << "Fail to init channel"
               << std::endl;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    LOG(ERROR) << cntl.response_attachment();
}


}  // namespace snapshotcloneserver
}  // namespace curve

