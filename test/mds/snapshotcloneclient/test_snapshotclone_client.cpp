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
 * Created Date: 2020/12/02
 * Author: hzchenwei7
 */


#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/server.h>
#include "src/mds/snapshotcloneclient/snapshotclone_client.h"
#include "test/mds/mock/mock_snapshotcloneserver.h"
#include "json/json.h"
#include "src/common/snapshotclone/snapshotclone_define.h"

using ::curve::snapshotcloneserver::MockSnapshotCloneService;
using ::curve::snapshotcloneserver::HttpRequest;
using ::curve::snapshotcloneserver::HttpResponse;
using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::curve::snapshotcloneserver::kCodeStr;
using ::curve::snapshotcloneserver::kErrCodeSuccess;
using ::curve::snapshotcloneserver::kErrCodeInternalError;
using ::curve::snapshotcloneserver::kRefStatusStr;
using ::curve::snapshotcloneserver::CloneRefStatus;

using ::testing::Return;
using ::testing::_;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;

namespace curve {
namespace mds {
namespace snapshotcloneclient {

class TestSnapshotCloneClient : public ::testing::Test {
 protected:
    TestSnapshotCloneClient() {}
    void SetUp() {
        server_ = new brpc::Server();
        client_ = std::make_shared<SnapshotCloneClient>();

        mocksnapshotcleonservice_ = new MockSnapshotCloneService();
        ASSERT_EQ(server_->AddService(mocksnapshotcleonservice_,
                                      brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(0, server_->Start("127.0.0.1", {8900, 8999}, nullptr));
        listenAddr_ = server_->listen_address();
    }
    void TearDown() {
        client_ = nullptr;
        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
        delete mocksnapshotcleonservice_;
        mocksnapshotcleonservice_ = nullptr;
    }

 protected:
    std::shared_ptr<SnapshotCloneClient> client_;
    butil::EndPoint listenAddr_;
    brpc::Server *server_;
    MockSnapshotCloneService *mocksnapshotcleonservice_;
    SnapshotCloneClientOption option;
};

TEST_F(TestSnapshotCloneClient, TestInitSuccess) {
    uint32_t port = listenAddr_.port;
    option.snapshotCloneAddr = "127.0.0.1:" + std::to_string(port);
    client_->Init(option);
    ASSERT_TRUE(client_->GetInitStatus());
}

TEST_F(TestSnapshotCloneClient, TestInitFalse) {
    option.snapshotCloneAddr = "";
    client_->Init(option);
    ASSERT_FALSE(client_->GetInitStatus());
}

TEST_F(TestSnapshotCloneClient, TestGetCloneRefStatusFalseNotInit) {
    std::string filename = "/file";
    std::string user = "test";
    CloneRefStatus status;
    std::vector<DestFileInfo> fileCheckList;
    auto ret = client_->GetCloneRefStatus(filename, user,
                                &status, &fileCheckList);
    ASSERT_EQ(ret, StatusCode::kSnapshotCloneServerNotInit);
}

TEST_F(TestSnapshotCloneClient, TestGetCloneRefStatusFalseConnectFail) {
    option.snapshotCloneAddr = "aa";
    client_->Init(option);
    ASSERT_TRUE(client_->GetInitStatus());

    std::string filename = "/file";
    std::string user = "test";
    CloneRefStatus status;
    std::vector<DestFileInfo> fileCheckList;
    auto ret = client_->GetCloneRefStatus(filename, user,
                                &status, &fileCheckList);
    ASSERT_EQ(ret, StatusCode::kSnapshotCloneConnectFail);
}

TEST_F(TestSnapshotCloneClient, TestGetCloneRefStatusFalseCallFail) {
    option.snapshotCloneAddr = "127.0.0.1:" + std::to_string(0);
    client_->Init(option);
    ASSERT_TRUE(client_->GetInitStatus());

    std::string filename = "/file";
    std::string user = "test";
    CloneRefStatus status;
    std::vector<DestFileInfo> fileCheckList;
    auto ret = client_->GetCloneRefStatus(filename, user,
                                &status, &fileCheckList);
    ASSERT_EQ(ret, StatusCode::KInternalError);
}

TEST_F(TestSnapshotCloneClient, TestGetCloneRefStatusFalseParseFail) {
    uint32_t port = listenAddr_.port;
    option.snapshotCloneAddr = "127.0.0.1:" + std::to_string(port);
    client_->Init(option);
    ASSERT_TRUE(client_->GetInitStatus());

    HttpResponse response;
    EXPECT_CALL(*mocksnapshotcleonservice_, default_method(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const HttpRequest *request,
                          HttpResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));

    std::string filename = "/file";
    std::string user = "test";
    CloneRefStatus status;
    std::vector<DestFileInfo> fileCheckList;
    auto ret = client_->GetCloneRefStatus(filename, user,
                                &status, &fileCheckList);
    ASSERT_EQ(ret, StatusCode::KInternalError);
}

TEST_F(TestSnapshotCloneClient, TestGetCloneRefStatusFalseRetNot0) {
    uint32_t port = listenAddr_.port;
    option.snapshotCloneAddr = "127.0.0.1:" + std::to_string(port);
    client_->Init(option);
    ASSERT_TRUE(client_->GetInitStatus());

    HttpResponse response;
    EXPECT_CALL(*mocksnapshotcleonservice_, default_method(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const HttpRequest *request,
                          HttpResponse *response,
                          Closure *done){
                        brpc::Controller* bcntl =
                            static_cast<brpc::Controller*>(controller);
                        bcntl->http_response().set_status_code(
                                    brpc::HTTP_STATUS_OK);
                        butil::IOBufBuilder os;
                        Json::Value mainObj;
                        mainObj[kCodeStr] = std::to_string(
                                    kErrCodeInternalError);
                        os << mainObj.toStyledString();
                        os.move_to(bcntl->response_attachment());
                        brpc::ClosureGuard doneGuard(done);
                    })));

    std::string filename = "/file";
    std::string user = "test";
    CloneRefStatus status;
    std::vector<DestFileInfo> fileCheckList;
    auto ret = client_->GetCloneRefStatus(filename, user,
                                &status, &fileCheckList);
    ASSERT_EQ(ret, StatusCode::KInternalError);
}

TEST_F(TestSnapshotCloneClient, TestGetCloneRefStatusFalseInvalidStatus) {
    uint32_t port = listenAddr_.port;
    option.snapshotCloneAddr = "127.0.0.1:" + std::to_string(port);
    client_->Init(option);
    ASSERT_TRUE(client_->GetInitStatus());

    HttpResponse response;
    EXPECT_CALL(*mocksnapshotcleonservice_, default_method(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const HttpRequest *request,
                          HttpResponse *response,
                          Closure *done){
                        brpc::Controller* bcntl =
                            static_cast<brpc::Controller*>(controller);
                        bcntl->http_response().set_status_code(
                                            brpc::HTTP_STATUS_OK);
                        butil::IOBufBuilder os;
                        Json::Value mainObj;
                        mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
                        mainObj[kRefStatusStr] = 4;
                        os << mainObj.toStyledString();
                        os.move_to(bcntl->response_attachment());
                        brpc::ClosureGuard doneGuard(done);
                    })));

    std::string filename = "/file";
    std::string user = "test";
    CloneRefStatus status;
    std::vector<DestFileInfo> fileCheckList;
    auto ret = client_->GetCloneRefStatus(filename, user,
                                &status, &fileCheckList);
    ASSERT_EQ(ret, StatusCode::KInternalError);
}

TEST_F(TestSnapshotCloneClient, TestGetCloneRefStatusSuccessNoRef) {
    uint32_t port = listenAddr_.port;
    option.snapshotCloneAddr = "127.0.0.1:" + std::to_string(port);
    client_->Init(option);
    ASSERT_TRUE(client_->GetInitStatus());

    HttpResponse response;
    EXPECT_CALL(*mocksnapshotcleonservice_, default_method(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const HttpRequest *request,
                          HttpResponse *response,
                          Closure *done){
                        brpc::Controller* bcntl =
                            static_cast<brpc::Controller*>(controller);
                        bcntl->http_response().set_status_code(
                                            brpc::HTTP_STATUS_OK);
                        butil::IOBufBuilder os;
                        Json::Value mainObj;
                        mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
                        CloneRefStatus refStatus = CloneRefStatus::kNoRef;
                        mainObj[kRefStatusStr] = static_cast<int> (refStatus);
                        os << mainObj.toStyledString();
                        os.move_to(bcntl->response_attachment());
                        brpc::ClosureGuard doneGuard(done);
                    })));

    std::string filename = "/file";
    std::string user = "test";
    CloneRefStatus status;
    std::vector<DestFileInfo> fileCheckList;
    auto ret = client_->GetCloneRefStatus(filename, user,
                                &status, &fileCheckList);
    ASSERT_EQ(ret, StatusCode::kOK);
    ASSERT_EQ(status, CloneRefStatus::kNoRef);
    ASSERT_EQ(0, fileCheckList.size());
}

TEST_F(TestSnapshotCloneClient, TestGetCloneRefStatusSuccessHasRef) {
    uint32_t port = listenAddr_.port;
    option.snapshotCloneAddr = "127.0.0.1:" + std::to_string(port);
    client_->Init(option);
    ASSERT_TRUE(client_->GetInitStatus());
    CloneRefStatus refStatus = CloneRefStatus::kHasRef;
    HttpResponse response;
    EXPECT_CALL(*mocksnapshotcleonservice_, default_method(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([refStatus](RpcController *controller,
                          const HttpRequest *request,
                          HttpResponse *response,
                          Closure *done){
                        brpc::Controller* bcntl =
                            static_cast<brpc::Controller*>(controller);
                        bcntl->http_response().set_status_code(
                                            brpc::HTTP_STATUS_OK);
                        butil::IOBufBuilder os;
                        Json::Value mainObj;
                        mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
                        mainObj[kRefStatusStr] = static_cast<int> (refStatus);
                        os << mainObj.toStyledString();
                        os.move_to(bcntl->response_attachment());
                        brpc::ClosureGuard doneGuard(done);
                    })));

    std::string filename = "/file";
    std::string user = "test";
    CloneRefStatus status;
    std::vector<DestFileInfo> fileCheckList;
    auto ret = client_->GetCloneRefStatus(filename, user,
                                &status, &fileCheckList);
    ASSERT_EQ(ret, StatusCode::kOK);
    ASSERT_EQ(status, refStatus);
    ASSERT_EQ(0, fileCheckList.size());
}

TEST_F(TestSnapshotCloneClient, TestGetCloneRefStatusSuccessNeedCheck) {
    uint32_t port = listenAddr_.port;
    option.snapshotCloneAddr = "127.0.0.1:" + std::to_string(port);
    client_->Init(option);
    ASSERT_TRUE(client_->GetInitStatus());
    CloneRefStatus refStatus = CloneRefStatus::kNeedCheck;
    HttpResponse response;
    EXPECT_CALL(*mocksnapshotcleonservice_, default_method(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([refStatus](RpcController *controller,
                          const HttpRequest *request,
                          HttpResponse *response,
                          Closure *done){
                        brpc::Controller* bcntl =
                            static_cast<brpc::Controller*>(controller);
                        bcntl->http_response().set_status_code(
                                            brpc::HTTP_STATUS_OK);
                        butil::IOBufBuilder os;
                        Json::Value mainObj;
                        mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
                        mainObj[kRefStatusStr] = static_cast<int> (refStatus);
                        mainObj["TotalCount"] = 1;
                        Json::Value listObj;
                        Json::Value cloneTaskObj;
                        cloneTaskObj["User"] = "user";
                        cloneTaskObj["File"] = "/dest";
                        cloneTaskObj["Inode"] = 100;
                        listObj.append(cloneTaskObj);
                        mainObj["CloneFileInfo"] = listObj;
                        os << mainObj.toStyledString();
                        os.move_to(bcntl->response_attachment());
                        brpc::ClosureGuard doneGuard(done);
                    })));

    std::string filename = "/file";
    std::string user = "test";
    CloneRefStatus status;
    std::vector<DestFileInfo> fileCheckList;
    auto ret = client_->GetCloneRefStatus(filename, user,
                                &status, &fileCheckList);
    ASSERT_EQ(ret, StatusCode::kOK);
    ASSERT_EQ(status, refStatus);
    ASSERT_EQ(1, fileCheckList.size());
}

}  // namespace snapshotcloneclient
}  // namespace mds
}  // namespace curve
