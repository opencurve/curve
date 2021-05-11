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
 * Created Date: 18-10-11
 * Author: wudemiao
 */

#include "src/chunkserver/chunk_closure.h"
#include <memory>

namespace curve {
namespace chunkserver {

void ChunkClosure::Run() {
    /**
     * After Run，deconstruct itself，to avoid the bugs of the deconstructor
     */
    std::unique_ptr<ChunkClosure> selfGuard(this);
    /**
     * Make sure done be called，so that rpc will definitely return
     */
    brpc::ClosureGuard doneGuard(request_->Closure());
    /**
     * Although leader's indentification has been confirmed before the request
     * is proposed to copyset, it's status may turn into non-leader while
     * copyset is processing the request. So, we need to check the status of the
     * request when ChunkClosure is called. If the status is ok, it means
     * everything goes well. Otherwise, Redirect is needed.
     */
    if (status().ok()) {
        return;
    }

    request_->RedirectChunkRequest();
}

}  // namespace chunkserver
}  // namespace curve
