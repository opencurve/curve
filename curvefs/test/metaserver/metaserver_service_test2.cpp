/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Tue Sep  7 16:18:37 CST 2021
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include "absl/memory/memory.h"
#include "curvefs/src/metaserver/inflight_throttle.h"
#include "curvefs/src/metaserver/metaserver_service.h"

namespace curvefs {
namespace metaserver {

class FakeClosure : public google::protobuf::Closure {
 public:
    void Run() override {
        std::lock_guard<std::mutex> lk(mtx_);
        runned_ = true;
        cond_.notify_one();
    }

    void WaitRunned() {
        std::unique_lock<std::mutex> lk(mtx_);
        cond_.wait(lk, [this]() { return runned_; });
    }

 private:
    std::mutex mtx_;
    std::condition_variable cond_;
    bool runned_ = false;
};

class MetaServerServiceTest2 : public testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}

 protected:
    std::unique_ptr<InflightThrottle> throttle_;
    std::unique_ptr<MetaServerServiceImpl> metaServer_;
};

TEST_F(MetaServerServiceTest2, ServiceOverload) {
    throttle_ = absl::make_unique<InflightThrottle>(0);
    metaServer_ = absl::make_unique<MetaServerServiceImpl>(
        nullptr, throttle_.get());

    throttle_->Increment();

#define TEST_SERVICE_OVERLOAD(TYPE)                                 \
    do {                                                            \
        TYPE##Request request;                                      \
        TYPE##Response response;                                    \
        FakeClosure closure;                                        \
        metaServer_->TYPE(nullptr, &request, &response, &closure);  \
        closure.WaitRunned();                                       \
        EXPECT_EQ(MetaStatusCode::OVERLOAD, response.statuscode()); \
    } while (0)

    TEST_SERVICE_OVERLOAD(GetDentry);
    TEST_SERVICE_OVERLOAD(ListDentry);
    TEST_SERVICE_OVERLOAD(CreateDentry);
    TEST_SERVICE_OVERLOAD(DeleteDentry);
    TEST_SERVICE_OVERLOAD(GetInode);
    TEST_SERVICE_OVERLOAD(CreateInode);
    TEST_SERVICE_OVERLOAD(UpdateInode);
    TEST_SERVICE_OVERLOAD(DeleteInode);
    TEST_SERVICE_OVERLOAD(CreateRootInode);
    TEST_SERVICE_OVERLOAD(UpdateInodeS3Version);
    TEST_SERVICE_OVERLOAD(CreatePartition);
    TEST_SERVICE_OVERLOAD(DeletePartition);
    TEST_SERVICE_OVERLOAD(PrepareRenameTx);

#undef TEST_SERVICE_OVERLOAD
}

TEST_F(MetaServerServiceTest2, CopysetNodeNotFound) {
    throttle_ = absl::make_unique<InflightThrottle>(1);
    metaServer_ = absl::make_unique<MetaServerServiceImpl>(
        &CopysetNodeManager::GetInstance(), throttle_.get());

#define TEST_COPYSETNODE_NOTFOUND(TYPE)                                     \
    do {                                                                    \
        TYPE##Request request;                                              \
        TYPE##Response response;                                            \
        FakeClosure closure;                                                \
        metaServer_->TYPE(nullptr, &request, &response, &closure);          \
        closure.WaitRunned();                                               \
        EXPECT_EQ(MetaStatusCode::COPYSET_NOTEXIST, response.statuscode()); \
    } while (0)

    TEST_COPYSETNODE_NOTFOUND(GetDentry);
    TEST_COPYSETNODE_NOTFOUND(ListDentry);
    TEST_COPYSETNODE_NOTFOUND(CreateDentry);
    TEST_COPYSETNODE_NOTFOUND(DeleteDentry);
    TEST_COPYSETNODE_NOTFOUND(GetInode);
    TEST_COPYSETNODE_NOTFOUND(CreateInode);
    TEST_COPYSETNODE_NOTFOUND(UpdateInode);
    TEST_COPYSETNODE_NOTFOUND(DeleteInode);
    TEST_COPYSETNODE_NOTFOUND(CreateRootInode);
    TEST_COPYSETNODE_NOTFOUND(UpdateInodeS3Version);
    TEST_COPYSETNODE_NOTFOUND(CreatePartition);
    TEST_COPYSETNODE_NOTFOUND(DeletePartition);
    TEST_COPYSETNODE_NOTFOUND(PrepareRenameTx);

#undef TEST_COPYSETNODE_NOTFOUND
}

}  // namespace metaserver
}  // namespace curvefs
