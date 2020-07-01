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
 * File Created: Monday, 17th September 2018 4:19:54 pm
 * Author: tongguangxun
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "src/client/request_context.h"
#include "src/client/request_closure.h"

namespace curve {
namespace client {

std::atomic<uint64_t> RequestContext::reqCtxID_(1);

RequestContext::RequestContext() {
    readBuffer_ = nullptr;
    writeBuffer_ = nullptr;
    chunkinfodetail_ = nullptr;

    id_         = reqCtxID_.fetch_add(1);

    seq_        = 0;
    offset_     = 0;
    rawlength_  = 0;

    appliedindex_ = 0;
}
bool RequestContext::Init() {
    done_ = new (std::nothrow) RequestClosure(this);
    return done_ != nullptr;
}

void RequestContext::UnInit() {
    delete done_;
}

}  // namespace client
}  // namespace curve
