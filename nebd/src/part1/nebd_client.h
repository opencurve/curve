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
 * Project: nebd
 * File Created: 2019-10-08
 * Author: hzchenwei7
 */

#ifndef NEBD_SRC_PART1_NEBD_CLIENT_H_
#define NEBD_SRC_PART1_NEBD_CLIENT_H_

#include <brpc/channel.h>
#include <bthread/execution_queue.h>

#include <functional>
#include <string>
#include <memory>
#include <vector>

#include "nebd/src/part1/nebd_common.h"
#include "nebd/src/common/configuration.h"
#include "nebd/proto/client.pb.h"
#include "nebd/src/part1/libnebd.h"
#include "nebd/src/part1/heartbeat_manager.h"
#include "nebd/src/part1/nebd_metacache.h"

#include "include/curve_compiler_specific.h"

namespace nebd {
namespace client {

using RpcTask = std::function<int64_t (brpc::Controller* cntl,
                                       brpc::Channel* channel,
                                       bool* rpcFailed)>;
using nebd::common::Configuration;

class NebdClient {
 public:
    static NebdClient &GetInstance() {
        static NebdClient client;
        return client;
    }

    ~NebdClient() = default;

    /**
     * @brief initializes nebd and only executes the initialization logic on the first call
     * @param none
     * @return returns 0 for success, -1 for failure
     */
    int Init(const char* confpath);

    /**
     * @brief uninitialize nebd
     * @param none
     * @return returns 0 for success, -1 for failure
     */
    void Uninit();

    /**
     * @brief open file
     * @param filename: File name
     * @return successfully returned the file fd, but failed with an error code
     */
    int Open(const char* filename, const NebdOpenFlags* flags);

    /**
     * @brief close file
     * @param fd: fd of the file
     * @return success returns 0, failure returns error code
     */
    int Close(int fd);

    /**
     * @brief resize file
     * @param fd: fd of the file
     *Size: adjusted file size
     * @return success returns 0, failure returns error code
     */
    int Extend(int fd, int64_t newsize);

    /**
     * @brief Get file size
     * @param fd: fd of the file
     * @return successfully returned the file size, but failed with an error code
     */
    int64_t GetFileSize(int fd);

    int64_t GetBlockSize(int fd);

    /**
     * @brief discard file, asynchronous function
     * @param fd: fd of the file
     *          context: The context of an asynchronous request, including the information required for the request and the callback
     * @return success returns 0, failure returns error code
     */
    int Discard(int fd, NebdClientAioContext* aioctx);

    /**
     * @brief Read file, asynchronous function
     * @param fd: fd of the file
     *          context: The context of an asynchronous request, including the information required for the request and the callback
     * @return success returns 0, failure returns error code
     */
    int AioRead(int fd, NebdClientAioContext* aioctx);

    /**
     * @brief write file, asynchronous function
     * @param fd: fd of the file
     *          context: The context of an asynchronous request, including the information required for the request and the callback
     * @return success returns 0, failure returns error code
     */
    int AioWrite(int fd, NebdClientAioContext* aioctx);

    /**
     * @brief flush file, asynchronous function
     * @param fd: fd of the file
     *          context: The context of an asynchronous request, including the information required for the request and the callback
     * @return success returns 0, failure returns error code
     */
    int Flush(int fd, NebdClientAioContext* aioctx);

    /**
     * @brief Get file information
     * @param fd: fd of the file
     * @return successfully returned the file object size, but failed with an error code
     */
    int64_t GetInfo(int fd);

    /**
     * @brief refresh cache, wait for all asynchronous requests to return
     * @param fd: fd of the file
     * @return success returns 0, failure returns error code
     */
    int InvalidCache(int fd);

 private:
    int InitNebdClientOption(Configuration* conf);

    int InitHeartBeatOption(Configuration* conf,
                            HeartbeatOption* hearbeatOption);

    int InitChannel();

    void InitLogger(const LogOption& logOption);

    /**
     * @brief replaces'/'with'+'in the string
     *
     * @param str The string that needs to be replaced
     * @return The replaced string
     */
    std::string ReplaceSlash(const std::string& str);

    int64_t ExecuteSyncRpc(RpcTask task);
    // Heartbeat management module
    std::shared_ptr<HeartbeatManager> heartbeatMgr_;
    // Cache module
    std::shared_ptr<NebdClientMetaCache> metaCache_;

    NebdClientOption option_;

    brpc::Channel channel_;

    std::atomic<uint64_t> logId_{1};

 private:
    using AsyncRpcTask = std::function<void()>;

    std::vector<bthread::ExecutionQueueId<AsyncRpcTask>> rpcTaskQueues_;

    static int ExecAsyncRpcTask(void* meta, bthread::TaskIterator<AsyncRpcTask>& iter);  // NOLINT

    void PushAsyncTask(const AsyncRpcTask& task) {
        static thread_local unsigned int seed = time(nullptr);

        int idx = rand_r(&seed) % rpcTaskQueues_.size();
        int rc = bthread::execution_queue_execute(rpcTaskQueues_[idx], task);

        if (CURVE_UNLIKELY(rc != 0)) {
            task();
        }
    }
};

extern NebdClient &nebdClient;

}  // namespace client
}  // namespace nebd

#endif  // NEBD_SRC_PART1_NEBD_CLIENT_H_
