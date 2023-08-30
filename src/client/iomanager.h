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
 * File Created: Monday, 25th February 2019 9:52:28 am
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_IOMANAGER_H_
#define SRC_CLIENT_IOMANAGER_H_

#include "src/client/io_tracker.h"
#include "src/client/client_common.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace client {

using curve::common::Atomic;

class IOManager {
 public:
    IOManager() {
        id_ = idRecorder_.fetch_add(1, std::memory_order_relaxed);
    }
    virtual ~IOManager() = default;

    /**
     * @brief Get the ID information of the current iomanager
     */
    virtual IOManagerID ID() const {
        return id_;
    }

    /**
     * @brief Get rpc send token
     */
    virtual void GetInflightRpcToken() {
        return;
    }

    /**
     * @brief Release RPC Send Token
     */
    virtual void ReleaseInflightRpcToken() {
        return;
    }

    /**
     * @brief handles response returned asynchronously
     * @param: Iotracker is the ownership of the current reponse
     */
    virtual void HandleAsyncIOResponse(IOTracker* iotracker) = 0;

 protected:
    //The purpose of the iomanager id is to let the underlying RPC know which iomanager it belongs to
    IOManagerID id_;

 private:
    // global id recorder
    static Atomic<uint64_t>   idRecorder_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_IOMANAGER_H_
