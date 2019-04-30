/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CLIENT_COPYSET_CLIENT_H_
#define SRC_CLIENT_COPYSET_CLIENT_H_

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
             IOSenderOption_t ioSenderOpt);

    /**
     * 返回依赖的Meta Cache
     */
    MetaCache *GetMetaCache() { return metaCache_; }

    /**
     * 读Chunk
     * @param idinfo为chunk相关的id信息
     * @param sn:文件版本号
     * @param offset:读的偏移
     * @param length:读的长度
     * @param appliedindex:需要读到>=appliedIndex的数据
     * @param done:上一层异步回调的closure
     * @param retriedTimes:已经重试了几次
     */
    int ReadChunk(ChunkIDInfo idinfo,
                  uint64_t sn,
                  off_t offset,
                  size_t length,
                  uint64_t appliedindex,
                  google::protobuf::Closure *done,
                  uint16_t retriedTimes = 0);

    /**
    * 写Chunk
    * @param idinfo为chunk相关的id信息
    * @param sn:文件版本号
    * @param buf:要写入的数据
     *@param offset:写的偏移
    * @param length:写的长度
    * @param done:上一层异步回调的closure
    * @param retriedTimes:已经重试了几次
    */
    int WriteChunk(ChunkIDInfo idinfo,
                  uint64_t sn,
                  const char *buf,
                  off_t offset,
                  size_t length,
                  Closure *done,
                  uint16_t retriedTimes = 0);

    /**
     * 读Chunk快照文件
     * @param idinfo为chunk相关的id信息
     * @param sn:文件版本号
     * @param offset:读的偏移
     * @param length:读的长度
     * @param done:上一层异步回调的closure
     * @param retriedTimes:已经重试了几次
     */
    int ReadChunkSnapshot(ChunkIDInfo idinfo,
                  uint64_t sn,
                  off_t offset,
                  size_t length,
                  Closure *done,
                  uint16_t retriedTimes = 0);

    /**
     * 删除此次转储时产生的或者历史遗留的快照
     * 如果转储过程中没有产生快照，则修改chunk的correctedSn
     * @param idinfo为chunk相关的id信息
     * @param correctedSn:需要修正的版本号
     * @param done:上一层异步回调的closure
     * @param retriedTimes:已经重试了几次
     */
    int DeleteChunkSnapshotOrCorrectSn(ChunkIDInfo idinfo,
                  uint64_t correctedSn,
                  Closure *done,
                  uint16_t retriedTimes = 0);

    /**
     * 获取chunk文件的信息
     * @param idinfo为chunk相关的id信息
     * @param done:上一层异步回调的closure
     * @param retriedTimes:已经重试了几次
     */
    int GetChunkInfo(ChunkIDInfo idinfo,
                  Closure *done,
                  uint16_t retriedTimes = 0);

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
                  const std::string &location,
                  uint64_t sn,
                  uint64_t correntSn,
                  uint64_t chunkSize,
                  Closure *done,
                  uint16_t retriedTimes = 0);

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
                  uint64_t offset,
                  uint64_t len,
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

#endif  // SRC_CLIENT_COPYSET_CLIENT_H_
