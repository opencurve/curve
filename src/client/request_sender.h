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

#ifndef SRC_CLIENT_REQUEST_SENDER_H_
#define SRC_CLIENT_REQUEST_SENDER_H_

#include <brpc/channel.h>
#include <butil/endpoint.h>
#include <butil/iobuf.h>

#include <string>

#include "src/client/client_config.h"
#include "src/client/client_common.h"
#include "src/client/chunk_closure.h"
#include "include/curve_compiler_specific.h"
#include "src/client/request_context.h"

namespace curve {
namespace client {

/**
 * 一个RequestSender负责管理一个ChunkServer的所有
 * connection，目前一个ChunkServer仅有一个connection
 */
class RequestSender {
 public:
    RequestSender(ChunkServerID chunkServerId,
                  butil::EndPoint serverEndPoint)
        : chunkServerId_(chunkServerId),
          serverEndPoint_(serverEndPoint),
          channel_() {}
    virtual ~RequestSender() {}

    int Init(const IOSenderOption& ioSenderOpt);

    /**
     * 读Chunk
     * @param idinfo为chunk相关的id信息
     * @param sn:文件版本号
     * @param offset:读的偏移
     * @param length:读的长度
     * @param appliedindex:需要读到>=appliedIndex的数据
     * @param sourceInfo 数据源信息
     * @param done:上一层异步回调的closure
     */
    int ReadChunk(const ChunkIDInfo& idinfo,
                  uint64_t sn,
                  off_t offset,
                  size_t length,
                  uint64_t appliedindex,
                  const RequestSourceInfo& sourceInfo,
                  ClientClosure *done);

    /**
   * 写Chunk
   * @param idinfo为chunk相关的id信息
   * @param fileId: file id
   * @param epoch: file epoch
   * @param sn:文件版本号
   * @param snaps: 现有快照版本号集合
   * @param data 要写入的数据
    *@param offset:写的偏移
   * @param length:写的长度
   * @param sourceInfo 数据源信息
   * @param done:上一层异步回调的closure
   */
    int WriteChunk(const ChunkIDInfo& idinfo,
                   uint64_t fileId,
                   uint64_t epoch,
                   uint64_t sn,
                   const std::vector<uint64_t>& snaps,
                   const butil::IOBuf& data,
                   off_t offset,
                   size_t length,
                   const RequestSourceInfo& sourceInfo,
                   ClientClosure *done);

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
                          ClientClosure *done);

    /**
     * 删除此次转储时产生的或者历史遗留的快照
     * 如果转储过程中没有产生快照，则修改chunk的correctedSn
     * @param idinfo为chunk相关的id信息
     * @param correctedSn:chunk需要修正的版本号
     * @param done:上一层异步回调的closure
     */
    int DeleteChunkSnapshotOrCorrectSn(const ChunkIDInfo& idinfo,
                            uint64_t correctedSn,
                            ClientClosure *done);

    /**
     * 获取chunk文件的信息
     * @param idinfo为chunk相关的id信息
     * @param done:上一层异步回调的closure
     * @param retriedTimes:已经重试了几次
     */
    int GetChunkInfo(const ChunkIDInfo& idinfo,
                     ClientClosure *done);

    /**
    * @brief lazy 创建clone chunk
    * @detail
    *  - location的格式定义为 A@B的形式。
    *  - 如果源数据在s3上，则location格式为uri@s3，uri为实际chunk对象的地址；
    *  - 如果源数据在curvefs上，则location格式为/filename/chunkindex@cs
    *
    * @param idinfo为chunk相关的id信息
    * @param done:上一层异步回调的closure
    * @param:location 数据源的url
    * @param:sn chunk的序列号
    * @param:correntSn CreateCloneChunk时候用于修改chunk的correctedSn
    * @param:chunkSize chunk的大小
    * @param retriedTimes:已经重试了几次
    *
    * @return 错误码
    */
    int CreateCloneChunk(const ChunkIDInfo& idinfo,
                  ClientClosure *done,
                  const std::string &location,
                  uint64_t sn,
                  uint64_t correntSn,
                  uint64_t chunkSize);

   /**
    * @brief 实际恢复chunk数据
    * @param idinfo为chunk相关的id信息
    * @param done:上一层异步回调的closure
    * @param:offset 偏移
    * @param:len 长度
    * @param retriedTimes:已经重试了几次
    *
    * @return 错误码
    */
    int RecoverChunk(const ChunkIDInfo& idinfo,
                     ClientClosure* done, uint64_t offset, uint64_t len);
    /**
     * 重置和Chunk Server的链接
     * @param chunkServerId:Chunk Server唯一标识
     * @param serverEndPoint:Chunk Server
     * @return 0成功，-1失败
     */
    int ResetSender(ChunkServerID chunkServerId,
                    butil::EndPoint serverEndPoint);

    bool IsSocketHealth() {
       return channel_.CheckHealth() == 0;
    }

 private:
    void UpdateRpcRPS(ClientClosure* done, OpType type) const;

    void SetRpcStuff(ClientClosure* done, brpc::Controller* cntl,
                     google::protobuf::Message* rpcResponse) const;

 private:
    // Rpc stub配置
    IOSenderOption iosenderopt_;
    // ChunkServer 的唯一标识 id
    ChunkServerID chunkServerId_;
    // ChunkServer 的地址
    butil::EndPoint serverEndPoint_;
    brpc::Channel channel_; /* TODO(wudemiao): 后期会维护多个 channel */
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_REQUEST_SENDER_H_
