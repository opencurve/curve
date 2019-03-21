/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CLIENT_COPYSETCLIENT_H
#define CURVE_CLIENT_COPYSETCLIENT_H

#include <google/protobuf/stubs/callback.h>
#include <gflags/gflags.h>

#include <cstdio>
#include <string>

#include "src/client/client_common.h"
#include "src/client/config_info.h"
#include "src/common/uncopyable.h"
#include "src/client/request_sender_manager.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {

using curve::common::Uncopyable;
using ::google::protobuf::Closure;

class MetaCache;
/**
 * 负责管理 ChunkServer 的链接，向上层提供访问
 * 指定 copyset 的 chunk 的 read/write 等接口
 */
class CopysetClient : public Uncopyable {
 public:
    CopysetClient() :
        metaCache_(nullptr),
        senderManager_(nullptr) {}

    virtual  ~CopysetClient() {
        if (nullptr != senderManager_) {
            delete senderManager_;
            senderManager_ = nullptr;
        }
    }

    int Init(MetaCache *metaCache,
             IOSenderOption_t iosenderopt);

    /**
     * 返回依赖的Meta Cache
     */
    MetaCache *GetMetaCache() { return metaCache_; }

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
     * @param retriedTimes:已经重试了几次
     */
    int ReadChunk(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkID chunkId,
                  uint64_t sn,
                  off_t offset,
                  size_t length,
                  uint64_t appliedindex,
                  google::protobuf::Closure *done,
                  uint16_t retriedTimes = 0);

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
    * @param retriedTimes:已经重试了几次
    */
    int WriteChunk(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkID chunkId,
                  uint64_t sn,
                  const char *buf,
                  off_t offset,
                  size_t length,
                  Closure *done,
                  uint16_t retriedTimes = 0);

    /**
     * 读Chunk快照文件
     * @param logicPoolId:逻辑池id
     * @param copysetId:复制组id
     * @param chunkId:Chunk文件id
     * @param sn:文件版本号
     * @param offset:读的偏移
     * @param length:读的长度
     * @param done:上一层异步回调的closure
     * @param retriedTimes:已经重试了几次
     */
    int ReadChunkSnapshot(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkID chunkId,
                  uint64_t sn,
                  off_t offset,
                  size_t length,
                  Closure *done,
                  uint16_t retriedTimes = 0);

    /**
     * 删除Chunk快照文件
     * @param logicPoolId:逻辑池id
     * @param copysetId:复制组id
     * @param chunkId:Chunk文件id
     * @param sn:文件版本号
     * @param done:上一层异步回调的closure
     * @param retriedTimes:已经重试了几次
     */
    int DeleteChunkSnapshot(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkID chunkId,
                  uint64_t sn,
                  Closure *done,
                  uint16_t retriedTimes = 0);

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
                  Closure *done,
                  uint16_t retriedTimes = 0);

 private:
    // 元数据缓存
    MetaCache            *metaCache_;
    // 所有ChunkServer的链接管理者
    RequestSenderManager *senderManager_;
    // 配置
    IOSenderOption_t iosenderopt_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_COPYSETCLIENT_H
