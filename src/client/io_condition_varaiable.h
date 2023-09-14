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
 * File Created: Friday, 21st September 2018 11:21:15 am
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_IO_CONDITION_VARAIABLE_H_
#define SRC_CLIENT_IO_CONDITION_VARAIABLE_H_

#include <condition_variable>   //NOLINT
#include <mutex>    //NOLINT

namespace curve {
namespace client {
// IOConditionVariable is the IO waiting condition variable in the user synchronous IO scenario
class IOConditionVariable {
 public:
    IOConditionVariable() : retCode_(-1), done_(false), mtx_(), cv_() {}

    ~IOConditionVariable() = default;

    /**
     * Condition variable wakeup function. Since the underlying RPC requests are asynchronous,
     * when users initiate synchronous IO, they need to pause and wait for the IO to return while sending read/write requests.
     * @param: retcode is the return value of the current IO.
     */
    void Complete(int retcode) {
        std::unique_lock<std::mutex> lk(mtx_);
        retCode_ = retcode;
        done_ = true;
        cv_.notify_one();
    }

    /**
     * This is a function called when user IO needs to wait, and this function will return when Complete is called
     */
    int Wait() {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_.wait(lk, [&]() { return done_; });
        done_ = false;
        return retCode_;
    }

 private:
    // The return value of the current IO
    int     retCode_;

    // Is the current IO completed
    bool    done_;

    // Locks used by conditional variables
    std::mutex  mtx_;

    // Condition variable used for waiting
    std::condition_variable cv_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_IO_CONDITION_VARAIABLE_H_
