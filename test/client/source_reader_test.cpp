/*
 *  Copyright (c) 2023 NetEase Inc.
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

#include "src/client/source_reader.h"

#include <gtest/gtest.h>

#include "src/client/mds_client.h"
#include "test/client/fake/fakeMDS.h"

namespace curve {
namespace client {

class SourceReaderTest : public ::testing::Test {
 protected:
    void SetUp() override {
        opt_.metaServerOpt.mdsAddrs.push_back(kServerAddr);
        opt_.leaseOpt.mdsRefreshTimesPerLease = 1;

        ASSERT_EQ(
            0, server_.AddService(&service_, brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(kServerAddr, nullptr));

        mdsclient_ = std::make_shared<MDSClient>();
        ASSERT_EQ(0, mdsclient_->Initialize(opt_.metaServerOpt));

        PrepareFakeResponse();
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

    void PrepareFakeResponse() {
        curve::mds::GetFileInfoResponse* getFileInfoResponse =
            new curve::mds::GetFileInfoResponse();
        auto* info = getFileInfoResponse->mutable_fileinfo();
        info->set_id(1);
        info->set_filename("/clone-source");
        info->set_parentid(0);
        info->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        info->set_chunksize(16 * 1024 * 1024);
        info->set_length(16 * 1024 * 1024 * 1024UL);
        info->set_segmentsize(1 * 1024 * 1024 * 1024ul);

        getFileInfoResponse->set_statuscode(curve::mds::StatusCode::kOK);

        auto* fakeReturn = new FakeReturn(nullptr, getFileInfoResponse);
        responses_.emplace_back(getFileInfoResponse, fakeReturn);

        service_.SetGetFileInfoFakeReturn(fakeReturn);
    }

 protected:
    FileServiceOption opt_;
    FakeMDSCurveFSService service_;
    brpc::Server server_;
    const char* kServerAddr = "127.0.0.1:9611";
    std::shared_ptr<MDSClient> mdsclient_;
    std::vector<std::pair<std::unique_ptr<google::protobuf::Message>,
                          std::unique_ptr<FakeReturn>>>
        responses_;
};

TEST_F(SourceReaderTest, ConcurrentTest) {
    SourceReader::GetInstance().SetOption(opt_);
    UserInfo user{"user", "pass"};

    SourceReader::ReadHandler* handler1 = nullptr;
    SourceReader::ReadHandler* handler2 = nullptr;

    std::thread th1([&]() {
        handler1 = SourceReader::GetInstance().GetReadHandler("/file1", user,
                                                              mdsclient_.get());
    });
    std::thread th2([&]() {
        handler2 = SourceReader::GetInstance().GetReadHandler("/file1", user,
                                                              mdsclient_.get());
    });

    th1.join();
    th2.join();

    ASSERT_EQ(handler1, handler2);
}

}  // namespace client
}  // namespace curve
