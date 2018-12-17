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

#include "src/snapshot/snapshot_service.h"
#include "test/snapshot/mock_snapshot_server.h"

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;

namespace curve {
namespace snapshotserver {

class TestSnapshotServiceImpl : public ::testing::Test {
 protected:
    TestSnapshotServiceImpl() {}
    ~TestSnapshotServiceImpl() {}

    virtual void SetUp() {
        listenAddr_ = "127.0.0.1:8200";
        server_ = new brpc::Server();

        manager_ = std::make_shared<MockSnapshotServiceManager>();

        SnapshotServiceImpl *snapService =
            new SnapshotServiceImpl(manager_);

        ASSERT_EQ(0, server_->AddService(snapService,
                brpc::SERVER_OWNS_SERVICE));

        ASSERT_EQ(0, server_->Start(listenAddr_.c_str(), nullptr));
    }

    virtual void TearDown() {
        manager_ = nullptr;
        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
    }

 protected:
    std::shared_ptr<MockSnapshotServiceManager> manager_;
    std::string listenAddr_;
    brpc::Server *server_;
};

TEST_F(TestSnapshotServiceImpl, TestCreateSnapShotSuccess) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*manager_, CreateSnapshot(_, _, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(uuid),
                    Return(kErrCodeSnapshotServerSuccess)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=CreateSnapshot&Version=1&User=test&File=test&Name=test"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestDeleteSnapShotSuccess) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";

    EXPECT_CALL(*manager_, DeleteSnapshot(uuid, user, file))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=DeleteSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestCancelSnapShotSuccess) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";

    EXPECT_CALL(*manager_, CancelSnapshot(uuid, user, file))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=CancelSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestGetFileSnapshotInfoSuccess) {
    std::string file = "test";
    std::string user = "test";

    std::vector<FileSnapshotInfo> info;
    EXPECT_CALL(*manager_, GetFileSnapshotInfo(file, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(info),
                    Return(kErrCodeSnapshotServerSuccess)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=GetFileSnapshotInfo&Version=1&User=test&File=test&Limit=10"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestActionIsNull) {
    std::string file = "test";
    std::string user = "test";

    std::vector<FileSnapshotInfo> info;

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Version=1&User=test&File=test&Limit=10"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestCreateSnapShotMissingParam) {
    UUID uuid = "uuid1";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=CreateSnapshot&Version=1&User=test&Name=test"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestDeleteSnapShotMissingParam) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=DeleteSnapshot&Version=1&User=test&File=test"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestCancelSnapShotMissingParam) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";


    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=CancelSnapshot&Version=1&User=test&File=test"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestGetFileSnapshotInfoMissingParam) {
    std::string file = "test";
    std::string user = "test";

    std::vector<FileSnapshotInfo> info;

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=GetFileSnapshotInfo&Version=1&User=test&Limit=10"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestCreateSnapShotFail) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*manager_, CreateSnapshot(_, _, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(uuid),
                    Return(kErrCodeSnapshotServerFail)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=CreateSnapshot&Version=1&User=test&File=test&Name=test"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestDeleteSnapShotFail) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";

    EXPECT_CALL(*manager_, DeleteSnapshot(uuid, user, file))
        .WillOnce(Return(kErrCodeSnapshotServerFail));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=DeleteSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestCancelSnapShotFail) {
    UUID uuid = "uuid1";
    std::string user = "test";
    std::string file = "test";

    EXPECT_CALL(*manager_, CancelSnapshot(uuid, user, file))
        .WillOnce(Return(kErrCodeSnapshotServerFail));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=CancelSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestGetFileSnapshotInfoFail) {
    std::string file = "test";
    std::string user = "test";

    std::vector<FileSnapshotInfo> info;
    EXPECT_CALL(*manager_, GetFileSnapshotInfo(file, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(info),
                    Return(kErrCodeSnapshotServerFail)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=GetFileSnapshotInfo&Version=1&User=test&File=test&Limit=10"; //NOLINT

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

TEST_F(TestSnapshotServiceImpl, TestCreateSnapShotBadRequest) {
    UUID uuid = "uuid1";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = "http://127.0.0.1:8200/SnapshotService?Action=xxx&Version=1&User=test&File=test&Name=test"; //NOLINT

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

}  // namespace snapshotserver
}  // namespace curve
