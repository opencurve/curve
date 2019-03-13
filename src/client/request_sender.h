/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CLIENT_CHUNK_REQUEST_SENDER_H
#define CURVE_CLIENT_CHUNK_REQUEST_SENDER_H

#include <brpc/channel.h>
#include <butil/endpoint.h>

#include "src/client/client_config.h"
#include "src/client/client_common.h"
#include "src/client/chunk_closure.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {

using curve::chunkserver::ChunkRequest;
using curve::chunkserver::ChunkResponse;
using curve::chunkserver::GetChunkInfoRequest;
using curve::chunkserver::GetChunkInfoResponse;
using curve::chunkserver::ChunkService_Stub;
using ::google::protobuf::Closure;

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

    int Init(IOSenderOption_t ioSenderOpt);

    /**
     * 读Chunk
     * @param logicPoolId:逻辑池id
     * @param copysetId:复制组id
     * @param chunkId:Chunk文件id
     * @param sn:文件版本号
     * @param offset:读的偏移
     * @param length:读的长度
     * @param appliedindex:需要读到>=appliedIndex的数据
     * @param done:上一层异步回调的closure
     */
    int ReadChunk(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkID chunkId,
                  uint64_t sn,
                  off_t offset,
                  size_t length,
                  uint64_t appliedindex,
                  ClientClosure *done);

    /**
   * 写Chunk
   * @param logicPoolId:逻辑池id
   * @param copysetId:复制组id
   * @param chunkId:Chunk文件id
   * @param sn:文件版本号
   * @param buf:要写入的数据
    *@param offset:写的偏移
   * @param length:写的长度
   * @param done:上一层异步回调的closure
   */
    int WriteChunk(LogicPoolID logicPoolId,
                   CopysetID copysetId,
                   ChunkID chunkId,
                   uint64_t sn,
                   const char *buf,
                   off_t offset,
                   size_t length,
                   ClientClosure *done);

    /**
     * 读Chunk快照文件
     * @param logicPoolId:逻辑池id
     * @param copysetId:复制组id
     * @param chunkId:Chunk文件id
     * @param sn:文件版本号
     * @param offset:读的偏移
     * @param length:读的长度
     * @param done:上一层异步回调的closure
     */
    int ReadChunkSnapshot(LogicPoolID logicPoolId,
                          CopysetID copysetId,
                          ChunkID chunkId,
                          uint64_t sn,
                          off_t offset,
                          size_t length,
                          ClientClosure *done);

    /**
     * 删除Chunk快照文件
     * @param logicPoolId:逻辑池id
     * @param copysetId:复制组id
     * @param chunkId:Chunk文件id
     * @param sn:文件版本号
     * @param done:上一层异步回调的closure
     */
    int DeleteChunkSnapshot(LogicPoolID logicPoolId,
                            CopysetID copysetId,
                            ChunkID chunkId,
                            uint64_t sn,
                            ClientClosure *done);

    /**
     * 获取chunk文件的信息
     * @param logicPoolId:逻辑池id
     * @param copysetId:复制组id
     * @param chunkId:Chunk文件id
     * @param done:上一层异步回调的closure
     * @param retriedTimes:已经重试了几次
     */
    int GetChunkInfo(LogicPoolID logicPoolId,
                     CopysetID copysetId,
                     ChunkID chunkId,
                     ClientClosure *done);

    /**
     * 重置和Chunk Server的链接
     * @param chunkServerId:Chunk Server唯一标识
     * @param serverEndPoint:Chunk Server
     * @return 0成功，-1失败
     */
    int ResetSender(ChunkServerID chunkServerId,
                    butil::EndPoint serverEndPoint);

 private:
    // Rpc stub配置
    IOSenderOption_t iosenderopt_;
    // ChunkServer 的唯一标识 id
    ChunkServerID chunkServerId_;
    // ChunkServer 的地址
    butil::EndPoint serverEndPoint_;
    brpc::Channel channel_; /* TODO(wudemiao): 后期会维护多个 channel */
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_CHUNK_REQUEST_SENDER_H
