/*
 * Project: curve
 * File Created: Wednesday, 5th September 2018 8:04:38 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_CHUNKSERVER_CHUNKSERVER_DATASTORE_H
#define CURVE_CHUNKSERVER_CHUNKSERVER_DATASTORE_H

#include <glog/logging.h>
#include <string>
#include <vector>
#include <unordered_map>

#include "include/curve_compiler_specific.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/datastore/chunkserver_chunkfile.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "src/fs/local_filesystem.h"

namespace curve {
namespace chunkserver {
using curve::fs::LocalFileSystem;
using CSChunkFilePtr = std::shared_ptr<CSChunkFile>;

/**
 * DataStore的配置参数
 * baseDir:DataStore管理的目录路径
 * chunkSize:DataStore中chunk文件或快照文件的大小
 * pageSize:最小读写单元的大小
 */
struct DataStoreOptions {
    std::string                         baseDir;
    ChunkSizeType                       chunkSize;
    PageSizeType                        pageSize;
};

// 为chunkid到chunkfile的映射，使用读写锁对map的操作进行保护
class CSMetaCache {
 public:
    CSMetaCache() {}
    virtual ~CSMetaCache() {}

    CSChunkFilePtr Get(ChunkID id) {
        ReadLockGuard readGuard(rwLock_);
        if (chunkMap_.find(id) == chunkMap_.end()) {
            return nullptr;
        }
        return chunkMap_[id];
    }

    CSChunkFilePtr Set(ChunkID id, CSChunkFilePtr chunkFile) {
        WriteLockGuard writeGuard(rwLock_);
        // 当两个写请求并发去创建chunk文件时，返回先Set的chunkFile
        if (chunkMap_[id] == nullptr)
            chunkMap_[id] = chunkFile;
        return chunkMap_[id];
    }

    void Remove(ChunkID id) {
        WriteLockGuard writeGuard(rwLock_);
        chunkMap_.erase(id);
    }

    void Clear() {
        WriteLockGuard writeGuard(rwLock_);
        chunkMap_.clear();
    }

 private:
    RWLock                                         rwLock_;
    std::unordered_map<ChunkID, CSChunkFilePtr>    chunkMap_;
};

class CSDataStore {
 public:
    CSDataStore(std::shared_ptr<LocalFileSystem> lfs,
                std::shared_ptr<ChunkfilePool> ChunkfilePool,
                const DataStoreOptions& options);
    virtual ~CSDataStore();
    /**
     * copyset初始化时调用
     * 初始化时遍历当前copyset目录下的所有文件，读取metapage加载到metacache
     * @return：成功返回true，失败返回false
     */
    virtual bool Initialize();
    /**
     * 删除当前chunk文件
     * @param id：要删除的chunk的id
     * @param sn：用于记录trace，实际逻辑处理用不到，表示当前用户文件的版本号
     * @return：返回错误码
     */
    virtual CSErrorCode DeleteChunk(ChunkID id, SequenceNum sn);
    /**
     * 删除指定的快照文件
     * @param id：要删除的快照的chunk id
     * @param snapshotSn：要删除快照的版本号
     * @return：返回错误码
     */
    virtual CSErrorCode DeleteSnapshotChunk(ChunkID id,
                                               SequenceNum snapshotSn);
    /**
     * 读当前chunk的内容
     * @param id：要读取的chunk id
     * @param sn：用于记录trace，实际逻辑处理用不到，表示当前用户文件的版本号
     * @param buf：读取到的数据内容
     * @param offset：请求读取的数据在chunk中的逻辑偏移
     * @param length：请求读取的数据长度
     * @return：返回错误码
     */
    virtual CSErrorCode ReadChunk(ChunkID id,
                                     SequenceNum sn,
                                     char * buf,
                                     off_t offset,
                                     size_t length);
    /**
     * 读指定版本的数据，可能读当前chunk文件，也有可能读快照文件
     * @param id：要读取的chunk id
     * @param sn：要读取得chunk的版本号
     * @param buf：读取到的数据内容
     * @param offset：请求读取的数据在chunk中的逻辑偏移
     * @param length：请求读取的数据长度
     * @return：返回错误码
     */
    virtual CSErrorCode ReadSnapshotChunk(ChunkID id,
                                             SequenceNum sn,
                                             char * buf,
                                             off_t offset,
                                             size_t length);
    /**
     * 写数据
     * @param id：要写入的chunk id
     * @param sn：当前写请求发出时用户文件的版本号
     * @param buf：要写入的数据内容
     * @param offset：请求写入的偏移地址
     * @param length：请求写入的数据长度
     * @param cost：实际产生的IO次数，用于QOS控制
     * @return：返回错误码
     */
    virtual CSErrorCode WriteChunk(ChunkID id,
                                   SequenceNum sn,
                                   const char * buf,
                                   off_t offset,
                                   size_t length,
                                   uint32_t* cost);
    /**
     * 获取chunk的版本信息
     * @param id：请求获取的chunk的id
     * @param sns：chunk的所有版本号
     * @return：返回错误码
     */
    virtual CSErrorCode GetChunkInfo(ChunkID id,
                                     vector<SequenceNum>* sns);

 private:
    inline CSErrorCode loadChunkFile(ChunkID id);

 private:
    // 每个chunk的大小
    ChunkSizeType chunkSize_;
    // page大小，为最小原子读写单元
    PageSizeType pageSize_;
    // datastore的管理目录
    std::string baseDir_;
    // 为chunkid->chunkfile的映射
    CSMetaCache metaCache_;
    // chunkfile池，依赖该池子创建回收chunk文件或快照文件
    std::shared_ptr<ChunkfilePool>          chunkfilePool_;
    // 本地文件系统
    std::shared_ptr<LocalFileSystem>        lfs_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // !CURVE_CHUNKSERVER_CHUNKSERVER_DATASTORE_H
