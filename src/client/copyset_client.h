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
 * Created Date: 18-9-25
 * Author: wudemiao
 */

#ifndef SRC_CLIENT_COPYSET_CLIENT_H_
#define SRC_CLIENT_COPYSET_CLIENT_H_

#include <google/protobuf/stubs/callback.h>
#include <butil/iobuf.h>

#include <string>
#include <memory>
#include <vector>

#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/client_metric.h"
#include "src/client/config_info.h"
#include "src/client/request_context.h"
#include "src/client/request_sender_manager.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace client {

using curve::common::Uncopyable;
using ::google::protobuf::Closure;

// TODO(tongguangxun) :后续除了read、write的接口也需要调整重试逻辑
class MetaCache;
class RequestScheduler;
/**
 * 负责管理 ChunkServer 的链接，向上层提供访问
 * 指定 copyset 的 chunk 的 read/write 等接口
 */
class CopysetClient {
 public:
    CopysetClient() :
        sessionNotValid_(false),
        metaCache_(nullptr),
        senderManager_(nullptr),
        scheduler_(nullptr),
        exitFlag_(false) {}

    CopysetClient(const CopysetClient&) = delete;
    CopysetClient& operator=(const CopysetClient&) = delete;

    virtual ~CopysetClient() {
        delete senderManager_;
        senderManager_ = nullptr;
    }

    int Init(MetaCache *metaCache,
             const IOSenderOption& ioSenderOpt,
             RequestScheduler* scheduler = nullptr,
             FileMetric* fileMetic = nullptr);
    /**
     * 返回依赖的Meta Cache
     */
    MetaCache* GetMetaCache() {
        return metaCache_;
    }

    /**
    * write Chunk
    * @param ctx: request context
    * @param done: request closure
    */
    int WriteChunk(RequestContext* ctx, Closure *done);

    /**
    * read Chunk
    * @param ctx: request context
    * @param done: request closure
    */
    int ReadChunk(RequestContext* ctx, Closure* done);

    /**
     * 读Chunk快照文件
     * @param idinfo为chunk相关的id信息
     * @param sn:文件版本号
     * @param snaps:当前chunk所有快照序号列表
     * @param offset:读的偏移
     * @param length:读的长度
     * @param done:上一层异步回调的closure
     */
    int ReadChunkSnapshot(const ChunkIDInfo& idinfo,
                  uint64_t sn,
                  const std::vector<uint64_t>& snaps,
                  off_t offset,
                  size_t length,
                  Closure *done);

    /**
     * 删除此次转储时产生的或者历史遗留的快照
     * 如果转储过程中没有产生快照，则修改chunk的correctedSn
     * @param idinfo为chunk相关的id信息
     * @param correctedSn:需要修正的版本号
     * @param done:上一层异步回调的closure
     */
    int DeleteChunkSnapshotOrCorrectSn(const ChunkIDInfo& idinfo,
                  uint64_t correctedSn,
                  Closure *done);

    /**
     * 获取chunk文件的信息
     * @param idinfo为chunk相关的id信息
     * @param done:上一层异步回调的closure
     */
    int GetChunkInfo(const ChunkIDInfo& idinfo,
                  Closure *done);

    /**
    * @brief lazy 创建clone chunk
    * @param idinfo为chunk相关的id信息
    * @param:location 数据源的url
    * @param:sn chunk的序列号
    * @param:correntSn CreateCloneChunk时候用于修改chunk的correctedSn
    * @param:chunkSize chunk的大小
    * @param done:上一层异步回调的closure
    * @return 错误码
    */
    int CreateCloneChunk(const ChunkIDInfo& idinfo,
                  const std::string &location,
                  uint64_t sn,
                  uint64_t correntSn,
                  uint64_t chunkSize,
                  Closure *done);

   /**
    * @brief 实际恢复chunk数据
    * @param idinfo为chunk相关的id信息
    * @param:offset 偏移
    * @param:len 长度
    * @param done:上一层异步回调的closure
    * @return 错误码
    */
    int RecoverChunk(const ChunkIDInfo& idinfo,
                  uint64_t offset,
                  uint64_t len,
                  Closure *done);

    /**
     * @brief 如果csId对应的RequestSender不健康，就进行重置
     * @param csId chunkserver id
     */
    void ResetSenderIfNotHealth(const ChunkServerID& csId) {
        senderManager_->ResetSenderIfNotHealth(csId);
    }

    /**
     * session过期，需要将重试RPC停住
     */
    void StartRecycleRetryRPC() {
        sessionNotValid_ = true;
    }

    /**
     * session恢复通知不再回收重试的RPC
     */
    void ResumeRPCRetry() {
        sessionNotValid_ = false;
    }

    /**
     * 在文件关闭的时候接收上层关闭通知, 根据session有效状态
     * 置位exitFlag, 如果sessio无效状态下再有rpc超时返回，这
     * 些RPC会直接错误返回，如果session正常，则将继续正常下发
     * RPC，直到重试次数结束或者成功返回
     */
    void ResetExitFlag() {
        if (sessionNotValid_) {
            exitFlag_ = true;
        }
    }

 private:
    friend class WriteChunkClosure;
    friend class ReadChunkClosure;

    // 拉取新的leader信息
    bool FetchLeader(LogicPoolID lpid,
                     CopysetID cpid,
                     ChunkServerID* leaderid,
                     butil::EndPoint* leaderaddr);

    /**
     * 执行发送rpc task，并进行错误重试
     * @param[in]: idinfo为当前rpc task的id信息
     * @param[in]: task为本次要执行的rpc task
     * @param[in]: done是本次rpc 任务的异步回调
     * @return: 成功返回0， 否则-1
     */
    int DoRPCTask(const ChunkIDInfo& idinfo,
        std::function<void(Closure*, std::shared_ptr<RequestSender>)> task,
        Closure *done);

 private:
    // 元数据缓存
    MetaCache            *metaCache_;
    // 所有ChunkServer的链接管理者
    RequestSenderManager *senderManager_;
    // 配置
    IOSenderOption iosenderopt_;

    // session是否有效，如果session无效那么需要将重试的RPC停住
    // RPC停住通过将这个rpc重新push到request scheduler队列，这样不会
    // 阻塞brpc内部的线程，防止一个文件的操作影响到其他文件
    bool sessionNotValid_;

    // request 调度器，在session过期的时候重新将RPC push到调度队列
    RequestScheduler* scheduler_;

    // 当前copyset client对应的文件metric
    FileMetric* fileMetric_;

    // 是否在停止状态中，如果是在关闭过程中且session失效，需要将rpc直接返回不下发
    bool exitFlag_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_COPYSET_CLIENT_H_
