/*
 * Project: curve
 * File Created: Thursday, 6th September 2018 10:49:30 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_CHUNKFILE_H_
#define SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_CHUNKFILE_H_

#include <glog/logging.h>
#include <string>
#include <vector>
#include <set>
#include <atomic>
#include <functional>

#include "include/curve_compiler_specific.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/crc32.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/chunkserver/datastore/chunkserver_snapshot.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::common::RWLock;
using curve::common::WriteLockGuard;
using curve::common::ReadLockGuard;
using curve::common::BitRange;

class ChunkfilePool;
class CSSnapshot;

/**
 * Chunkfile Metapage Format
 * version: 1 byte
 * sn: 8 bytes
 * correctedSn: 8 bytes
 * crc: 4 bytes
 * padding: 4075 bytes
 */
struct ChunkFileMetaPage {
    // 文件格式的版本
    uint8_t version;
    // chunk文件的版本号
    SequenceNum sn;
    // chunk的修正版本号，与sn
    SequenceNum correctedSn;
    // 表示数据源的位置信息，如果不是CloneChunk则为空
    string location;
    // 表示当前Chunk中page的状态，如果不是CloneChunk则为nullptr
    std::shared_ptr<Bitmap> bitmap;

    ChunkFileMetaPage() : version(FORMAT_VERSION)
                        , sn(0)
                        , correctedSn(0)
                        , location("")
                        , bitmap(nullptr) {}
    ChunkFileMetaPage(const ChunkFileMetaPage& metaPage);
    ChunkFileMetaPage& operator = (const ChunkFileMetaPage& metaPage);

    void encode(char* buf);
    CSErrorCode decode(const char* buf);
};

struct ChunkOptions {
    // chunk的id，将作为chunk的文件名
    ChunkID         id;
    // chunk的版本号
    SequenceNum     sn;
    // chunk的修正版本号
    SequenceNum     correctedSn;
    // chunk所在的目录
    std::string     baseDir;
    // 如果要创建CloneChunk，需要指定该参数，表示数据源位置信息
    std::string     location;
    // chunk的大小
    ChunkSizeType   chunkSize;
    // page的大小，bitmap中每个bit表示1个page，metapage大小也是1个page
    PageSizeType    pageSize;

    ChunkOptions() : id(0)
                   , sn(0)
                   , correctedSn(0)
                   , baseDir("")
                   , location("")
                   , chunkSize(0)
                   , pageSize(0) {}
};

class CSChunkFile {
 public:
    CSChunkFile(std::shared_ptr<LocalFileSystem> lfs,
                std::shared_ptr<ChunkfilePool> ChunkfilePool,
                const ChunkOptions& options);
    virtual ~CSChunkFile();

    /**
      * Datastore初始化时发现存在Chunk文件会调用此接口初始化Chunk文件
      * 正常情况不存在并发，与其他操作互斥，加写锁
      * @createFile：true表示创建新文件，false则不创建文件
      * @return 返回错误码
      */
    CSErrorCode Open(bool createFile);
    /**
     * Datastore初始化发现快照文件时调用
     * 函数内部加载快找文件的metapage到内存
     * 正常情况下不存在并发，与其他操作互斥，加写锁
     * @param sn：要加载的快照文件版本号
     * @return：返回错误码
     */
    CSErrorCode LoadSnapshot(SequenceNum sn);
    /**
     * 写chunk文件
     * Write接口为raft apply时调用，Write之间不存多并发，
     * 但可能与其他Read、Delete等操作存在并发，加写锁
     * @param sn: 当前写请求的文件版本号
     * @param buf: 请求写入的数据
     * @param offset: 请求写入的偏移位置
     * @param length: 请求写入的数据长度
     * @param cost: 此次请求实际产生的IO次数，用于QOS控制
     * @return: 返回错误码
     */
    CSErrorCode Write(SequenceNum sn,
                      const char * buf,
                      off_t offset,
                      size_t length,
                      uint32_t* cost);
    /**
     * 将拷贝的数据写入Chunk中
     * 只会写入未写过的区域，不会覆盖已经写过的区域
     * 可能存在并发，加写锁
     * @param buf: 请求Paste的数据
     * @param offset: 请求Paste的数据起始偏移
     * @param length: 请求Paste的数据长度
     * @return: 返回错误码
     */
    CSErrorCode Paste(const char * buf, off_t offset, size_t length);
    /**
     * 读chunk文件
     * 可能存在并发，加读锁
     * @param buf: 读到的数据
     * @param offset: 请求读取的数据起始偏移
     * @param length: 请求读取的数据长度
     * @return: 返回错误码
     */
    CSErrorCode Read(char * buf, off_t offset, size_t length);
    /**
     * 读指定版本的chunk
     * 可能存在并发，加读锁
     * @param sn: 指定的chunk的数据
     * @param buf: 读到的快照数据
     * @param offset: 请求读取的快照数据起始偏移
     * @param length: 请求读取的快照数据长度
     * @return: 返回错误码
     */
    CSErrorCode ReadSpecifiedChunk(SequenceNum sn,
                                   char * buf,
                                   off_t offset,
                                   size_t length);
    /**
     * 删除chunk文件
     * 正常不存在并发，与其他操作互斥，加写锁
     * @return: 返回错误码
     */
    CSErrorCode Delete();
    /**
     * 删除chunk文件的快照
     * 正常不存在并发，与其他操作互斥，加写锁
     * @param snapshotSn:表示发出请求时快照文件的版本号
     * @return: 返回错误码
     */
    CSErrorCode DeleteSnapshot(SequenceNum snapshotSn);
    /**
     * 调用fsync将snapshot文件在pagecache中的数据刷盘
     */
    void GetInfo(CSChunkInfo* info);

 private:
    /**
     * 判断是否需要创建新的快照
     * @param sn:写请求的版本号
     * @return: true 表示要创建快照；false 表示不需要创建快照
     */
    bool needCreateSnapshot(SequenceNum sn);
    /**
     *  判断是否要做copy on write
     * @param sn:写请求的版本号
     * @return: true 表示要cow；false 表示不需要cow
     */
    bool needCow(SequenceNum sn);
    /**
     * 将metapage持久化
     * @param metaPage:需要持久化到磁盘的metapage,
     *                 如果成功持久化，会更改当前内存的metapage
     *                 如果失败，则不会更改
     */
    CSErrorCode updateMetaPage(ChunkFileMetaPage* metaPage);
    /**
     * 将metapage加载到内存
     */
    CSErrorCode loadMetaPage();
    /**
     * 将指定区域中未拷贝过的数据从chunk文件拷贝到快照文件
     * @param offset: 写入数据区域的起始偏移
     * @param length: 写入数据区域的长度
     * @return: 返回错误码
     */
    CSErrorCode copy2Snapshot(off_t offset, size_t length);
    /**
     * 更新clone chunk的bitmap
     * 如果所有的page都已写过，则将clone chunk转成普通chunk
     */
    CSErrorCode flush();

    inline string path() {
        return baseDir_ + "/" +
                    FileNameOperator::GenerateChunkFileName(chunkId_);
    }

    inline uint32_t fileSize() {
        return pageSize_ + size_;
    }

    inline int readMetaPage(char* buf) {
        return lfs_->Read(fd_, buf, 0, pageSize_);
    }

    inline int writeMetaPage(const char* buf) {
        return lfs_->Write(fd_, buf, 0, pageSize_);
    }

    inline int readData(char* buf, off_t offset, size_t length) {
        return lfs_->Read(fd_, buf, offset + pageSize_, length);
    }

    inline int writeData(const char* buf, off_t offset, size_t length) {
        int rc = lfs_->Write(fd_, buf, offset + pageSize_, length);
        if (rc < 0) {
            return rc;
        }
        // 如果是clone chunk，需要判断是否需要更改bitmap并更新metapage
        if (isCloneChunk()) {
            uint32_t beginIndex = offset / pageSize_;
            uint32_t endIndex = (offset + length - 1) / pageSize_;
            for (uint32_t i = beginIndex; i <= endIndex; ++i) {
                // 记录dirty page
                if (!metaPage_.bitmap->Test(i)) {
                    dirtyPages_.insert(i);
                }
            }
        }
        return rc;
    }

    inline bool isCloneChunk() {
        return !metaPage_.location.empty();
    }

 private:
    // chunk文件的资源描述符
    int fd_;
    // chunk的逻辑大小，不包含metapage
    ChunkSizeType size_;
    // 最小原子读写单元
    PageSizeType pageSize_;
    // chunk id
    ChunkID chunkId_;
    // chunk所在目录
    std::string baseDir_;
    // chunk的metapage
    ChunkFileMetaPage metaPage_;
    // 被写过但还未更新到metapage中的page索引
    std::set<uint32_t> dirtyPages_;
    // 读写锁
    RWLock rwLock_;
    // 快照文件指针
    CSSnapshot* snapshot_;
    // 依赖chunkfilepool创建删除文件
    std::shared_ptr<ChunkfilePool> chunkfilePool_;
    // 依赖本地文件系统操作文件
    std::shared_ptr<LocalFileSystem> lfs_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_CHUNKFILE_H_
