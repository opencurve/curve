/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CLIENT_REQUEST_SENDER_H_
#define SRC_CLIENT_REQUEST_SENDER_H_

#include <brpc/channel.h>
#include <butil/endpoint.h>

#include <string>

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
     * @param idinfo为chunk相关的id信息
     * @param sn:文件版本号
     * @param offset:读的偏移
     * @param length:读的长度
     * @param appliedindex:需要读到>=appliedIndex的数据
     * @param done:上一层异步回调的closure
     */
    int ReadChunk(ChunkIDInfo idinfo,
                  uint64_t sn,
                  off_t offset,
                  size_t length,
                  uint64_t appliedindex,
                  ClientClosure *done);

    /**
   * 写Chunk
   * @param idinfo为chunk相关的id信息
   * @param sn:文件版本号
   * @param buf:要写入的数据
    *@param offset:写的偏移
   * @param length:写的长度
   * @param done:上一层异步回调的closure
   */
    int WriteChunk(ChunkIDInfo idinfo,
                   uint64_t sn,
                   const char *buf,
                   off_t offset,
                   size_t length,
                   ClientClosure *done);

    /**
     * 读Chunk快照文件
     * @param idinfo为chunk相关的id信息
     * @param sn:文件版本号
     * @param offset:读的偏移
     * @param length:读的长度
     * @param done:上一层异步回调的closure
     */
    int ReadChunkSnapshot(ChunkIDInfo idinfo,
                          uint64_t sn,
                          off_t offset,
                          size_t length,
                          ClientClosure *done);

    /**
     * 删除Chunk快照文件
     * @param idinfo为chunk相关的id信息
     * @param sn:文件版本号
     * @param done:上一层异步回调的closure
     */
    int DeleteChunkSnapshot(ChunkIDInfo idinfo,
                            uint64_t sn,
                            ClientClosure *done);

    /**
     * 获取chunk文件的信息
     * @param idinfo为chunk相关的id信息
     * @param done:上一层异步回调的closure
     * @param retriedTimes:已经重试了几次
     */
    int GetChunkInfo(ChunkIDInfo idinfo,
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
    int CreateCloneChunk(ChunkIDInfo idinfo,
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
    int RecoverChunk(ChunkIDInfo idinfo,
                  ClientClosure *done,
                  uint64_t offset,
                  uint64_t len);
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

#endif  // SRC_CLIENT_REQUEST_SENDER_H_
