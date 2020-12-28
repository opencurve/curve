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
 * File Created: Tuesday, 9th October 2018 7:03:23 pm
 * Author: tongguangxun
 */

#include "src/client/request_closure.h"

#include "src/client/io_tracker.h"
#include "src/client/iomanager.h"
#include "src/client/request_context.h"

namespace curve {
namespace client {

void RequestClosure::Run() {
    ReleaseInflightRPCToken();
    if (suspendRPC_) {
        MetricHelper::DecremIOSuspendNum(metric_);
    }
    tracker_->HandleResponse(reqCtx_);
}

void RequestClosure::GetInflightRPCToken() {
    if (ioManager_ != nullptr) {
        ioManager_->GetInflightRpcToken();
        MetricHelper::IncremInflightRPC(metric_);
        ownInflight_ = true;
    }
}

void RequestClosure::ReleaseInflightRPCToken() {
    if (ioManager_ != nullptr && ownInflight_) {
        ioManager_->ReleaseInflightRpcToken();
        MetricHelper::DecremInflightRPC(metric_);
    }
}

}  // namespace client
}  // namespace curve
