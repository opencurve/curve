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
 * Created Date: 18-10-13
 * Author: wudemiao
 */

#ifndef TEST_CLIENT_MOCK_REQUEST_CONTEXT_H_
#define TEST_CLIENT_MOCK_REQUEST_CONTEXT_H_

#include "src/client/client_common.h"
#include "src/client/request_context.h"
#include "src/client/request_closure.h"
#include "src/common/concurrent/count_down_event.h"

namespace curve {
namespace client {

using FakeRequestContext = RequestContext;

class FakeRequestClosure : public RequestClosure {
 public:
    FakeRequestClosure(curve::common::CountDownEvent *cond,
                       RequestContext *reqctx)
        : RequestClosure(reqctx), cond_(cond) {}

    void Run() override {
        if (0 == GetErrorCode()) {
            LOG(INFO) << "success";
        } else {
            LOG(INFO) << "errno: " << GetErrorCode();
        }
        if (nullptr != cond_) {
            cond_->Signal();
        }
    }

 private:
    curve::common::CountDownEvent *cond_;
};

}   // namespace client
}   // namespace curve

#endif  // TEST_CLIENT_MOCK_REQUEST_CONTEXT_H_
