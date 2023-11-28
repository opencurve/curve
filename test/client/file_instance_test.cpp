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
 * Created Date: 19-11-15
 * Author: wuhanqing
 */

#include "src/client/file_instance.h"

#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "test/client/mock/mock_namespace_service.h"

namespace curve {
namespace client {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::SaveArgPointee;

TEST(FileInstanceTest, CommonTest) {
    UserInfo userInfo{"test", "passwd"};
    std::shared_ptr<MDSClient> mdsclient = std::make_shared<MDSClient>();

    // user info invlaid
    FileInstance fi;
    ASSERT_FALSE(fi.Initialize("/test", mdsclient, UserInfo{}, OpenFlags{},
                               FileServiceOption{}));

    // mdsclient is nullptr
    FileInstance fi2;
    ASSERT_FALSE(fi2.Initialize("/test", nullptr, userInfo, OpenFlags{},
                                FileServiceOption{}));

    // iomanager4file init failed
    FileInstance fi3;
    FileServiceOption opts;
    opts.ioOpt.taskThreadOpt.isolationTaskQueueCapacity = 0;
    opts.ioOpt.taskThreadOpt.isolationTaskThreadPoolSize = 0;

    ASSERT_FALSE(
        fi3.Initialize("/test", mdsclient, userInfo, OpenFlags{}, opts));

    // readonly
    FileInstance fi4;
    ASSERT_TRUE(fi4.Initialize("/test", mdsclient, userInfo, OpenFlags{},
                               FileServiceOption{}, true));
    ASSERT_EQ(-1, fi4.Write("", 0, 0));

    fi4.UnInitialize();
}

TEST(FileInstanceTest, IoAlignmentTest) {
    ASSERT_TRUE(CheckAlign(4096, 4096, 4096));

    ASSERT_FALSE(CheckAlign(512, 4096, 4096));
    ASSERT_FALSE(CheckAlign(4096, 512, 4096));

    ASSERT_TRUE(CheckAlign(4096, 4096, 512));
    ASSERT_TRUE(CheckAlign(512, 4096, 512));
    ASSERT_TRUE(CheckAlign(512, 512, 512));

    ASSERT_FALSE(CheckAlign(511, 511, 512));
}

constexpr size_t kMiB = 1 << 20;
constexpr size_t kGiB = 1 << 30;

class FileInstanceGetFileInfoTest : public ::testing::Test {
 protected:
    void SetUp() override {
        FileServiceOption opts;
        opts.metaServerOpt.mdsAddrs = {kSvrAddr};

        ASSERT_EQ(0, mdsclient_->Initialize(opts.metaServerOpt));
        ASSERT_EQ(0, server_.AddService(&nameService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(kSvrAddr, nullptr));

        UserInfo user("test", "");
        instance_.reset(FileInstance::NewInitedFileInstance(
            opts, mdsclient_, "/test", user, OpenFlags{},
            /*readonly=*/false));

        EXPECT_CALL(nameService_, OpenFile(_, _, _, _))
            .WillOnce(Invoke([](::google::protobuf::RpcController* cntl,
                                const curve::mds::OpenFileRequest*,
                                curve::mds::OpenFileResponse* response,
                                google::protobuf::Closure* done) {
                response->set_statuscode(curve::mds::StatusCode::kOK);
                auto* info = response->mutable_fileinfo();
                info->set_id(2);
                info->set_parentid(1);
                info->set_chunksize(16ULL * kMiB);
                info->set_segmentsize(1ULL * kGiB);
                info->set_length(1ULL * kGiB);
                auto* session = response->mutable_protosession();
                session->set_sessionid("1");
                session->set_leasetime(60);
                session->set_createtime(0);
                session->set_sessionstatus(
                    curve::mds::SessionStatus::kSessionOK);
                done->Run();
            }));

        EXPECT_CALL(nameService_, CloseFile(_, _, _, _))
            .WillOnce(Invoke([](::google::protobuf::RpcController* cntl,
                                const curve::mds::CloseFileRequest*,
                                curve::mds::CloseFileResponse* response,
                                google::protobuf::Closure* done) {
                response->set_statuscode(curve::mds::StatusCode::kOK);
                done->Run();
            }));

        EXPECT_CALL(nameService_, RefreshSession(_, _, _, _))
            .WillRepeatedly(
                Invoke([](::google::protobuf::RpcController* cntl,
                          const curve::mds::ReFreshSessionRequest*,
                          curve::mds::ReFreshSessionResponse* response,
                          google::protobuf::Closure* done) {
                    response->set_statuscode(curve::mds::StatusCode::kOK);
                    response->set_sessionid("");
                    done->Run();
                }));

        ASSERT_EQ(0, instance_->Open());
    }

    void TearDown() override {
        ASSERT_EQ(0, instance_->Close());

        server_.Stop(0);
        server_.Join();
    }

 protected:
    const char* kSvrAddr = "127.0.0.1:9610";

    std::shared_ptr<MDSClient> mdsclient_ = std::make_shared<MDSClient>();
    std::unique_ptr<FileInstance> instance_;

    brpc::Server server_;
    curve::mds::MockNameService nameService_;
};

TEST_F(FileInstanceGetFileInfoTest, TestGetInfoFromCache) {
    auto info = instance_->GetCurrentFileInfo();
    ASSERT_EQ(1ULL * kGiB, info.length);
}

TEST_F(FileInstanceGetFileInfoTest, TestForceGetInfo) {
    // get info from mds success, return cached fileinfo
    {
        EXPECT_CALL(nameService_, GetFileInfo(_, _, _, _))
            .WillOnce(Invoke([](::google::protobuf::RpcController* cntl,
                                const curve::mds::GetFileInfoRequest*,
                                curve::mds::GetFileInfoResponse* response,
                                google::protobuf::Closure* done) {
                response->set_statuscode(
                    curve::mds::StatusCode::kFileNotExists);
                done->Run();
            }));

        auto info = instance_->GetCurrentFileInfo(/*force=*/true);
        ASSERT_EQ(1ULL * kGiB, info.length);
    }

    // get info from mds success, return new fileinfo
    {
        EXPECT_CALL(nameService_, GetFileInfo(_, _, _, _))
            .WillOnce(Invoke([](::google::protobuf::RpcController* cntl,
                                const curve::mds::GetFileInfoRequest*,
                                curve::mds::GetFileInfoResponse* response,
                                google::protobuf::Closure* done) {
                response->set_statuscode(curve::mds::StatusCode::kOK);
                auto* info = response->mutable_fileinfo();
                info->set_id(2);
                info->set_parentid(1);
                info->set_chunksize(16ULL * kMiB);
                info->set_segmentsize(1ULL * kGiB);
                info->set_length(2ULL * kGiB);  // increase file size to 2GiB
                done->Run();
            }));

        auto info = instance_->GetCurrentFileInfo(/*force=*/true);
        ASSERT_EQ(2ULL * kGiB, info.length);
    }

    // get info from mds success, return cached fileinfo
    {
        EXPECT_CALL(nameService_, GetFileInfo(_, _, _, _))
            .WillOnce(Invoke([](::google::protobuf::RpcController* cntl,
                                const curve::mds::GetFileInfoRequest*,
                                curve::mds::GetFileInfoResponse* response,
                                google::protobuf::Closure* done) {
                response->set_statuscode(
                    curve::mds::StatusCode::kFileNotExists);
                done->Run();
            }));

        auto info = instance_->GetCurrentFileInfo(/*force=*/true);
        ASSERT_EQ(2ULL * kGiB, info.length);
    }
}

}  // namespace client
}  // namespace curve
