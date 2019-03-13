/*
 * Project: curve
 * File Created: Wednesday, 26th December 2018 3:47:53 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_IOCONTEXT_MANAGER4CHUNK_H
#define CURVE_IOCONTEXT_MANAGER4CHUNK_H

#include <atomic>
#include <mutex>    // NOLINT
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
    * @param: lpid逻辑池id
    * @param: cpid是copysetid
    * @param: chunkID对应chunkid
    * @param: seq是快照版本号
    * @param: offset是快照内的offset
    * @param: len是要读取的长度
    * @param: buf是读取缓冲区
    * @return：成功返回真实读取长度，失败为-1
    */
    int ReadSnapChunk(LogicPoolID lpid,
                     CopysetID cpid,
                     ChunkID chunkID,
                     uint64_t seq,
                     uint64_t offset,
                     uint64_t len,
                     void *buf);
   /**
    * 删除seq版本号的快照数据
    * @param: lpid逻辑池id
    * @param: cpid是copysetid
    * @param: chunkID对应chunkid
    * @param: seq是快照版本号
    */
    int DeleteSnapChunk(LogicPoolID lpid,
                     CopysetID cpid,
                     ChunkID chunkId,
                     uint64_t seq);
   /**
    * 获取chunk的版本信息，chunkInfo是出参
    * @param: lpid逻辑池id
    * @param: cpid是copysetid
    * @param: chunkID对应chunkid
    * @param: chunkInfo是快照的详细信息
    */
    int GetChunkInfo(LogicPoolID lpid,
                     CopysetID cpid,
                     ChunkID chunkId,
                     ChunkInfoDetail *chunkInfo);

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
#endif  // !CURVE_IOCONTEXT_MANAGER_H
