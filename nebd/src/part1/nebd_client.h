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
     *  @brief 初始化nebd，仅在第一次调用的时候真正执行初始化逻辑
     *  @param none
     *  @return 成功返回0，失败返回-1
     */
    int Init(const char* confpath);

    /**
     *  @brief 反初始化nebd
     *  @param none
     *  @return 成功返回0，失败返回-1
     */
    void Uninit();

    /**
     *  @brief open文件
     *  @param filename：文件名
     *  @return 成功返回文件fd，失败返回错误码
     */
    int Open(const char* filename, const NebdOpenFlags* flags);

    /**
     *  @brief close文件
     *  @param fd：文件的fd
     *  @return 成功返回0，失败返回错误码
     */
    int Close(int fd);

    /**
     *  @brief resize文件
     *  @param fd：文件的fd
     *         size：调整后的文件size
     *  @return 成功返回0，失败返回错误码
     */
    int Extend(int fd, int64_t newsize);

    /**
     *  @brief 获取文件size
     *  @param fd：文件的fd
     *  @return 成功返回文件size，失败返回错误码
     */
    int64_t GetFileSize(int fd);

    /**
     *  @brief discard文件，异步函数
     *  @param fd：文件的fd
     *         context：异步请求的上下文，包含请求所需的信息以及回调
     *  @return 成功返回0，失败返回错误码
     */
    int Discard(int fd, NebdClientAioContext* aioctx);

    /**
     *  @brief 读文件，异步函数
     *  @param fd：文件的fd
     *         context：异步请求的上下文，包含请求所需的信息以及回调
     *  @return 成功返回0，失败返回错误码
     */
    int AioRead(int fd, NebdClientAioContext* aioctx);

    /**
     *  @brief 写文件，异步函数
     *  @param fd：文件的fd
     *         context：异步请求的上下文，包含请求所需的信息以及回调
     *  @return 成功返回0，失败返回错误码
     */
    int AioWrite(int fd, NebdClientAioContext* aioctx);

    /**
     *  @brief flush文件，异步函数
     *  @param fd：文件的fd
     *         context：异步请求的上下文，包含请求所需的信息以及回调
     *  @return 成功返回0，失败返回错误码
     */
    int Flush(int fd, NebdClientAioContext* aioctx);

    /**
     *  @brief 获取文件info
     *  @param fd：文件的fd
     *  @return 成功返回文件对象size，失败返回错误码
     */
    int64_t GetInfo(int fd);

    /**
     *  @brief 刷新cache，等所有异步请求返回
     *  @param fd：文件的fd
     *  @return 成功返回0，失败返回错误码
     */
    int InvalidCache(int fd);

 private:
    int InitNebdClientOption(Configuration* conf);

    int InitHeartBeatOption(Configuration* conf,
                            HeartbeatOption* hearbeatOption);

    int InitChannel();

    void InitLogger(const LogOption& logOption);

    /**
     * @brief 替换字符串中的 '/' 为 '+'
     *
     * @param str 需要替换的字符串
     * @return 替换后的字符串
     */
    std::string ReplaceSlash(const std::string& str);

    int64_t ExecuteSyncRpc(RpcTask task);
    // 心跳管理模块
    std::shared_ptr<HeartbeatManager> heartbeatMgr_;
    // 缓存模块
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
