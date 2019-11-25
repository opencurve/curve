/*
 * Project: curve
 * Created Date: Fri Jan 04 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */


#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <json/json.h>
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
        server_ = new brpc::Server();

        snapshotManager_ = std::make_shared<MockSnapshotServiceManager>();
        cloneManager_ = std::make_shared<MockCloneServiceManager>();

        SnapshotCloneServiceImpl *snapService =
            new SnapshotCloneServiceImpl(snapshotManager_, cloneManager_);

        ASSERT_EQ(0, server_->AddService(snapService,
                brpc::SERVER_OWNS_SERVICE));

        ASSERT_EQ(0, server_->Start("127.0.0.1", {8900, 8999}, nullptr));
        listenAddr_ = server_->listen_address();
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
    butil::EndPoint listenAddr_;
    brpc::Server *server_;
};

TEST_F(TestSnapshotCloneServiceImpl, TestCreateSnapShotSuccess) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*snapshotManager_, CreateSnapshot(_, _, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(uuid),
                    Return(kErrCodeSuccess)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=CreateSnapshot&Version=1&User=test&File=test&Name=test"; //NOLINT

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
        .WillOnce(Return(kErrCodeSuccess));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=DeleteSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

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
        .WillOnce(Return(kErrCodeSuccess));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=CancelSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

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

    std::vector<FileSnapshotInfo> infoVec;
    FileSnapshotInfo info;
    SnapshotInfo sinfo("uuid1",
        "user1",
        "file1",
        "snap1",
         100,
         1024,
         1024,
         2048,
         100,
         Status::pending);
    info.SetSnapshotInfo(sinfo);
    info.SetSnapProgress(50);
    infoVec.push_back(info);
    EXPECT_CALL(*snapshotManager_, GetFileSnapshotInfo(file, user, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(infoVec),
                    Return(kErrCodeSuccess)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=GetFileSnapshotInfo&Version=1&User=test&File=test&Limit=10"; //NOLINT

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

    std::stringstream ss;
    ss << cntl.response_attachment();
    std::string data = ss.str();
    Json::Reader jsonReader;
    Json::Value jsonObj;
    if (!jsonReader.parse(data, jsonObj)) {
        FAIL() << "parse json fail, data = " << data;
    }
    ASSERT_STREQ("0", jsonObj["Code"].asCString());
    ASSERT_EQ(1, jsonObj["TotalCount"].asInt());
    ASSERT_EQ(1, jsonObj["Snapshots"].size());
    ASSERT_STREQ("uuid1", jsonObj["Snapshots"][0]["UUID"].asCString());
    ASSERT_STREQ("user1", jsonObj["Snapshots"][0]["User"].asCString());
    ASSERT_STREQ("file1", jsonObj["Snapshots"][0]["File"].asCString());
    ASSERT_EQ(100, jsonObj["Snapshots"][0]["SeqNum"].asInt());
    ASSERT_STREQ("snap1", jsonObj["Snapshots"][0]["Name"].asCString());
    ASSERT_EQ(100, jsonObj["Snapshots"][0]["Time"].asInt());
    ASSERT_EQ(2048, jsonObj["Snapshots"][0]["FileLength"].asInt());
    ASSERT_EQ(1, jsonObj["Snapshots"][0]["Status"].asInt());
    ASSERT_EQ(50, jsonObj["Snapshots"][0]["Progress"].asInt());
}

TEST_F(TestSnapshotCloneServiceImpl,
    TestGetFileSnapshotInfoUseLimitOffsetSuccess) {
    std::string file = "test";
    std::string user = "test";

    std::vector<FileSnapshotInfo> infoVec;
    FileSnapshotInfo info1, info2, info3;
    SnapshotInfo sinfo1, sinfo2, sinfo3;
    sinfo1.SetUuid("1");
    sinfo2.SetUuid("2");
    sinfo3.SetUuid("3");

    info1.SetSnapshotInfo(sinfo1);
    info2.SetSnapshotInfo(sinfo2);
    info3.SetSnapshotInfo(sinfo3);
    infoVec.push_back(info1);
    infoVec.push_back(info2);
    infoVec.push_back(info3);

    EXPECT_CALL(*snapshotManager_, GetFileSnapshotInfo(file, user, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(infoVec),
                    Return(kErrCodeSuccess)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=GetFileSnapshotInfo&Version=1&User=test&File=test&Limit=10&Offset=1"; //NOLINT

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
    std::stringstream ss;
    ss << cntl.response_attachment();
    std::string data = ss.str();
    Json::Reader jsonReader;
    Json::Value jsonObj;
    if (!jsonReader.parse(data, jsonObj)) {
        FAIL() << "parse json fail, data = " << data;
    }
    ASSERT_STREQ("0", jsonObj["Code"].asCString());
    ASSERT_EQ(3, jsonObj["TotalCount"].asInt());
    ASSERT_EQ(2, jsonObj["Snapshots"].size());
    ASSERT_STREQ("2", jsonObj["Snapshots"][0]["UUID"].asCString());
    ASSERT_STREQ("3", jsonObj["Snapshots"][1]["UUID"].asCString());
}

TEST_F(TestSnapshotCloneServiceImpl, TestActionIsNull) {
    std::string file = "test";
    std::string user = "test";

    std::vector<FileSnapshotInfo> info;

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Version=1&User=test&File=test&Limit=10"; //NOLINT

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

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=CreateSnapshot&Version=1&User=test&Name=test"; //NOLINT

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

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=DeleteSnapshot&Version=1&User=test&File=test"; //NOLINT

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

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=CancelSnapshot&Version=1&User=test&File=test"; //NOLINT

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

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=GetFileSnapshotInfo&Version=1&User=test&Limit=10"; //NOLINT

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
                    Return(kErrCodeInternalError)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=CreateSnapshot&Version=1&User=test&File=test&Name=test"; //NOLINT

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
        .WillOnce(Return(kErrCodeInternalError));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=DeleteSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

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
        .WillOnce(Return(kErrCodeInternalError));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=CancelSnapshot&Version=1&User=test&File=test&UUID=uuid1"; //NOLINT

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
    EXPECT_CALL(*snapshotManager_, GetFileSnapshotInfo(file, user, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(info),
                    Return(kErrCodeInternalError)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=GetFileSnapshotInfo&Version=1&User=test&File=test&Limit=10"; //NOLINT

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

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=xxx&Version=1&User=test&File=test&Name=test"; //NOLINT

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

    EXPECT_CALL(*cloneManager_, CloneFile(_, _, _, _, _, _))
        .WillOnce(Invoke([](const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag,
        std::shared_ptr<CloneClosure> closure,
        TaskIdType *taskId){
            brpc::ClosureGuard guard(closure.get());
            return kErrCodeSuccess;
                    }));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=Clone&Version=1&User=test&Source=abc&Destination=file1&Lazy=false"; //NOLINT

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

    EXPECT_CALL(*cloneManager_, RecoverFile(_, _, _, _, _, _))
        .WillOnce(Invoke([](const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag,
        std::shared_ptr<CloneClosure> closure,
        TaskIdType *taskId){
            brpc::ClosureGuard guard(closure.get());
            return kErrCodeSuccess;
                    }));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=Recover&Version=1&User=test&Source=abc&Destination=file1&Lazy=false"; //NOLINT

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

    std::vector<TaskCloneInfo> infoVec;
    TaskCloneInfo info;
    CloneInfo cinfo("uuid1",
        "user1",
        CloneTaskType::kClone,
        "source",
        "dest",
        100,
        200,
        100,
        CloneFileType::kSnapshot,
        true,
        CloneStep::kCreateCloneFile,
        CloneStatus::cloning);
    info.SetCloneInfo(cinfo);
    info.SetCloneProgress(50);
    infoVec.push_back(info);
    EXPECT_CALL(*cloneManager_, GetCloneTaskInfo(_, _, _))
        .WillOnce(DoAll(
                SetArgPointee<2>(infoVec),
                Return(kErrCodeSuccess)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=GetCloneTasks&Version=1&User=test"; //NOLINT

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
    std::stringstream ss;
    ss << cntl.response_attachment();
    std::string data = ss.str();
    Json::Reader jsonReader;
    Json::Value jsonObj;
    if (!jsonReader.parse(data, jsonObj)) {
        FAIL() << "parse json fail, data = " << data;
    }

    ASSERT_STREQ("0", jsonObj["Code"].asCString());
    ASSERT_EQ(1, jsonObj["TotalCount"].asInt());
    ASSERT_EQ(1, jsonObj["TaskInfos"].size());
    ASSERT_STREQ("uuid1", jsonObj["TaskInfos"][0]["UUID"].asCString());
    ASSERT_STREQ("user1", jsonObj["TaskInfos"][0]["User"].asCString());
    ASSERT_STREQ("dest", jsonObj["TaskInfos"][0]["File"].asCString());
    ASSERT_EQ(0,
        jsonObj["TaskInfos"][0]["TaskType"].asInt());
    ASSERT_EQ(1,
        jsonObj["TaskInfos"][0]["TaskStatus"].asInt());
    ASSERT_EQ(100, jsonObj["TaskInfos"][0]["Time"].asInt());
}

TEST_F(TestSnapshotCloneServiceImpl,
    TestGetCloneTaskUseLimitOffsetSuccess) {
    UUID uuid = "uuid1";

    std::vector<TaskCloneInfo> infoVec;
    TaskCloneInfo info1, info2 , info3;
    CloneInfo cinfo1, cinfo2, cinfo3;
    cinfo1.SetTaskId("1");
    cinfo2.SetTaskId("2");
    cinfo3.SetTaskId("3");
    info1.SetCloneInfo(cinfo1);
    info2.SetCloneInfo(cinfo2);
    info3.SetCloneInfo(cinfo3);
    infoVec.push_back(info1);
    infoVec.push_back(info2);
    infoVec.push_back(info3);
    EXPECT_CALL(*cloneManager_, GetCloneTaskInfo(_, _, _))
        .WillOnce(DoAll(
                SetArgPointee<2>(infoVec),
                Return(kErrCodeSuccess)));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=GetCloneTasks&Version=1&User=test&Limit=10&Offset=1"; //NOLINT

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
    std::stringstream ss;
    ss << cntl.response_attachment();
    std::string data = ss.str();
    Json::Reader jsonReader;
    Json::Value jsonObj;
    if (!jsonReader.parse(data, jsonObj)) {
        FAIL() << "parse json fail, data = " << data;
    }

    ASSERT_STREQ("0", jsonObj["Code"].asCString());
    ASSERT_EQ(3, jsonObj["TotalCount"].asInt());
    ASSERT_EQ(2, jsonObj["TaskInfos"].size());
    ASSERT_STREQ("2", jsonObj["TaskInfos"][0]["UUID"].asCString());
    ASSERT_STREQ("3", jsonObj["TaskInfos"][1]["UUID"].asCString());
}

TEST_F(TestSnapshotCloneServiceImpl, TestCloneFileMissingParam) {
    UUID uuid = "uuid1";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=Clone&Version=1&User=test&Source=abc&Destination=file1"; //NOLINT

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

TEST_F(TestSnapshotCloneServiceImpl, TestRecoverFileMissingParam) {
    UUID uuid = "uuid1";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=Recover&Version=1&User=test&Source=abc&Destination=file1"; //NOLINT

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

TEST_F(TestSnapshotCloneServiceImpl, TestGetCloneTaskMissingParam) {
    UUID uuid = "uuid1";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=GetCloneTasks&User=test"; //NOLINT

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

TEST_F(TestSnapshotCloneServiceImpl, TestCloneFileFail) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*cloneManager_, CloneFile(_, _, _, _, _, _))
        .WillOnce(Invoke([](const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag,
        std::shared_ptr<CloneClosure> closure,
        TaskIdType *taskId){
            brpc::ClosureGuard guard(closure.get());
            return kErrCodeInternalError;
                    }));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=Clone&Version=1&User=test&Source=abc&Destination=file1&Lazy=false"; //NOLINT

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

TEST_F(TestSnapshotCloneServiceImpl, TestRecoverFileFail) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*cloneManager_, RecoverFile(_, _, _, _, _, _))
        .WillOnce(Invoke([](const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag,
        std::shared_ptr<CloneClosure> closure,
        TaskIdType *taskId){
            brpc::ClosureGuard guard(closure.get());
            return kErrCodeInternalError;
                    }));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=Recover&Version=1&User=test&Source=abc&Destination=file1&Lazy=false"; //NOLINT

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

TEST_F(TestSnapshotCloneServiceImpl, TestGetCloneTaskFail) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*cloneManager_, GetCloneTaskInfo(_, _, _))
        .WillOnce(Return(kErrCodeInternalError));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=GetCloneTasks&Version=1&User=test"; //NOLINT

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

TEST_F(TestSnapshotCloneServiceImpl, TestCloneFileInvalidParam) {
    UUID uuid = "uuid1";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=Clone&Version=1&User=test&Source=abc&Destination=file1&Lazy=tru"; //NOLINT

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

TEST_F(TestSnapshotCloneServiceImpl, TestRecoverFileInvalidParam) {
    UUID uuid = "uuid1";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=Recover&Version=1&User=test&Source=abc&Destination=file1&Lazy=fal"; //NOLINT

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

TEST_F(TestSnapshotCloneServiceImpl, TestCleanCloneTasksSuccess) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*cloneManager_, CleanCloneTask(_, _))
        .WillOnce(Return(kErrCodeSuccess));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=CleanCloneTask&Version=1&User=test&UUID=aaa"; //NOLINT

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

TEST_F(TestSnapshotCloneServiceImpl, TestCleanCloneTasksMissingParam) {
    UUID uuid = "uuid1";

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=CleanCloneTask&Version=1&User=test"; //NOLINT

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

TEST_F(TestSnapshotCloneServiceImpl, TestCleanCloneTasksFail) {
    UUID uuid = "uuid1";

    EXPECT_CALL(*cloneManager_, CleanCloneTask(_, _))
        .WillOnce(Return(kErrCodeInternalError));

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = std::string("http://127.0.0.1:")
                    + std::to_string(listenAddr_.port)
                    + "/SnapshotCloneService?Action=CleanCloneTask&Version=1&User=test&UUID=aaa"; //NOLINT

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

