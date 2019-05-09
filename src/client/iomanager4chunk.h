/*
 * Project: curve
 * File Created: Wednesday, 26th December 2018 3:47:53 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_IOMANAGER4CHUNK_H_
#define SRC_CLIENT_IOMANAGER4CHUNK_H_

#include <atomic>
#include <mutex>    // NOLINT
#include <string>
#include <condition_variable>   // NOLINT

#include "src/client/metacache.h"
#include "src/client/iomanager.h"
#include "src/client/client_common.h"
#include "src/client/request_scheduler.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {
class MetaCache;
class IOManager4Chunk : public IOManager {
 public:
    IOManager4Chunk();
    ~IOManager4Chunk() = default;
    bool Initialize(IOOption_t  ioOpt);

   /**
    * 读取seq版本号的快照数据
    * @param:chunkidinfo 目标chunk
    * @param: seq是快照版本号
    * @param: offset是快照内的offset
    * @param: len是要读取的长度
    * @param: buf是读取缓冲区
    * @return：成功返回真实读取长度，失败为-1
    */
    int ReadSnapChunk(const ChunkIDInfo &chunkidinfo,
                     uint64_t seq,
                     uint64_t offset,
                     uint64_t len,
                     char *buf);
   /**
    * 删除此次转储时产生的或者历史遗留的快照
    * 如果转储过程中没有产生快照，则修改chunk的correctedSn
    * @param:chunkidinfo 目标chunk
    * @param: correctedSeq是需要修正的版本号
    */
    int DeleteSnapChunkOrCorrectSn(const ChunkIDInfo &chunkidinfo,
                                   uint64_t correctedSeq);
   /**
    * 获取chunk的版本信息，chunkInfo是出参
    * @param:chunkidinfo 目标chunk
    * @param: chunkInfo是快照的详细信息
    */
    int GetChunkInfo(const ChunkIDInfo &chunkidinfo,
                     ChunkInfoDetail *chunkInfo);

   /**
    * @brief lazy 创建clone chunk
    * @detail
    *  - location的格式定义为 A@B的形式。
    *  - 如果源数据在s3上，则location格式为uri@s3，uri为实际chunk对象的地址；
    *  - 如果源数据在curvefs上，则location格式为/filename/chunkindex@cs
    *
    * @param:location 数据源的url
    * @param:chunkidinfo 目标chunk
    * @param:sn chunk的序列号
    * @param:chunkSize chunk的大小
    * @param:correntSn CreateCloneChunk时候用于修改chunk的correctedSn
    *
    * @return 成功返回0， 否则-1
    */
    int CreateCloneChunk(const std::string &location,
                                const ChunkIDInfo &chunkidinfo,
                                uint64_t sn,
                                uint64_t correntSn,
                                uint64_t chunkSize);

   /**
    * @brief 实际恢复chunk数据
    *
    * @param:chunkidinfo chunkidinfo
    * @param:offset 偏移
    * @param:len 长度
    *
    * @return 成功返回0， 否则-1
    */
    int RecoverChunk(const ChunkIDInfo &chunkidinfo,
                              uint64_t offset,
                              uint64_t len);

    /**
     * 因为curve client底层都是异步IO，每个IO会分配一个IOtracker跟踪IO
     * 当这个IO做完之后，底层需要告知当前io manager来释放这个IOTracker，
     * HandleAsyncIOResponse负责释放IOTracker
     * @param: 是异步返回的io
     */
    void HandleAsyncIOResponse(IOTracker* iotracker) override;
   /**
    * 析构，回收资源
    */
    void UnInitialize();

   /**
    * 获取metacache，测试代码使用
    */
    MetaCache* GetMetaCache() {return &mc_;}
   /**
    * 设置scahuler，测试代码使用
    */
    void SetRequestScheduler(RequestScheduler* scheduler) {
      scheduler_ = scheduler;
    }

 private:
    // 每个IOManager都有其IO配置，保存在iooption里
    IOOption_t ioopt_;

    // metacache存储当前snapshot client元数据信息
    MetaCache  mc_;

    // IO最后由schedule模块向chunkserver端分发，scheduler由IOManager创建和释放
    RequestScheduler* scheduler_;
};

}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_IOMANAGER4CHUNK_H_
