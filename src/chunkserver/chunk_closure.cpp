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
     * 在Run结束之后，自动析构自己，这样可以避免
     * 析构函数漏调
     */
    std::unique_ptr<ChunkClosure> selfGuard(this);
    /**
     * 确保done能够被调用，目的是保证rpc一定会返回
     */
    brpc::ClosureGuard doneGuard(request_->Closure());
    /**
     * 尽管在request propose给copyset的之前已经
     * 对leader身份进行了确认，但是在copyset处理
     * request的时候，当前copyset的身份还是有可能
     * 变成非leader，所以需要判断ChunkClosure被调
     * 用的时候，request的status，如果 ok，说明是
     * 正常的apply处理，否则将请求转发
     */
    if (status().ok()) {
        return;
    }

    request_->RedirectChunkRequest();
}

}  // namespace chunkserver
}  // namespace curve
