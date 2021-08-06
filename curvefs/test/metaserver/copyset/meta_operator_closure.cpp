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
 * Date: Mon Sep  6 22:31:17 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/meta_operator_closure.h"

#include <gtest/gtest.h>

#include "absl/memory/memory.h"
#include "curvefs/src/metaserver/copyset/meta_operator.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

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

    bool Runned() const {
        return runned_;
    }

 private:
    std::mutex mtx_;
    std::condition_variable cond_;
    bool runned_ = false;
};

TEST(MetaOperatorClosureTest, TestOperatorSuccess) {
    FakeClosure done;
    CreateInodeRequest request;
    CreateInodeResponse response;

    auto op = absl::make_unique<CreateInodeOperator>(nullptr, nullptr, &request,
                                                     &response, &done);

    MetaOperatorClosure* metaClosure = new MetaOperatorClosure(op.release());
    metaClosure->status().reset();
    metaClosure->Run();

    done.WaitRunned();
    EXPECT_TRUE(done.Runned());
    EXPECT_NE(MetaStatusCode::REDIRECTED, response.statuscode());
}

TEST(MetaOperatorClosureTest, TestLeaderRedirect) {
    FakeClosure done;
    CreateInodeRequest request;
    CreateInodeResponse response;

    auto op = absl::make_unique<CreateInodeOperator>(nullptr, nullptr, &request,
                                                     &response, &done);

    MetaOperatorClosure* metaClosure = new MetaOperatorClosure(op.release());
    metaClosure->status().set_error(EINVAL, "invalid");
    metaClosure->Run();

    done.WaitRunned();
    EXPECT_TRUE(done.Runned());
    EXPECT_EQ(MetaStatusCode::REDIRECTED, response.statuscode());
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
