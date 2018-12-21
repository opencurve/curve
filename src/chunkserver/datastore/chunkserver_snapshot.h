/*
 * Project: curve
 * Created Date: Wednesday November 28th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef CURVEFS_CHUNKSERVER_CHUNKSERVER_SNAPSHOT_H
#define CURVEFS_CHUNKSERVER_CHUNKSERVER_SNAPSHOT_H

#include <glog/logging.h>
#include <string>
#include <set>

#include "src/common/bitmap.h"
#include "src/common/crc32.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/chunkserver/datastore/chunkserver_chunkfile.h"
#include "src/chunkserver/datastore/define.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"

namespace curve {
namespace chunkserver {

using curve::common::Bitmap;
using curve::fs::LocalFileSystem;

class ChunkfilePool;
class CSChunkFile;
struct ChunkOptions;

/**
 * Snapshot Metapage Format
 * version: 1 byte
 * damaged: 1 bytes
 * sn: 8 bytes
 * bits: 4 bytes
 * bitmap: (bits + 8 - 1) / 8 bytes
 * crc: 4 bytes
 * padding: (4096 - 18 - (bits + 8 - 1) / 8) bytes
 */
struct SnapshotMetaPage {
    // 文件格式版本号
    uint8_t version;
    // 表示当前快照是否已损坏
    bool damaged;
    // 快照版本号
    SequenceNum sn;
    // 表示当前快照page状态的为图标
    std::shared_ptr<Bitmap> bitmap;

    SnapshotMetaPage() : version(FORMAT_VERSION)
                       , damaged(false)
                       , bitmap(nullptr) {}
    SnapshotMetaPage(const SnapshotMetaPage& metaPage);
    SnapshotMetaPage& operator = (const SnapshotMetaPage& metaPage);

    void encode(char* buf);
    CSErrorCode decode(const char* buf);
};

class CSSnapshot {
 public:
    CSSnapshot(std::shared_ptr<LocalFileSystem> lfs,
               std::shared_ptr<ChunkfilePool> ChunkfilePool,
               const ChunkOptions& options);
    virtual ~CSSnapshot();
    /**
     * open快照文件，启动加载快照文件或者新建快照文件时调用
     * @param createFile：true表示创建新文件，false则不创建文件
     * @return: 返回错误码
     */
    CSErrorCode Open(bool createFile);
    /**
     * 将数据写入快照文件，数据写完后不立即更新bitmap，
     * 需要通过调用Flush来更新
     * @param buf: 请求写入的数据
     * @param offset: 请求写入的其实偏移
     * @param length: 请求写入的数据长度
     * @return: 返回错误码
     */
    CSErrorCode Write(const char * buf, off_t offset, size_t length);
    /**
     * 读快照数据，根据bitmap来判断是否要从chunk文件中读数据
     * @param buf: 读到的快照数据
     * @param offset: 请求读取的起始偏移
     * @param length: 请求读取的数据长度
     * @return: 返回错误码
     */
    CSErrorCode Read(char * buf, off_t offset, size_t length);
    /**
     * 删除快照文件
     * @return: 返回错误码
     */
    CSErrorCode Delete();
    /**
     * 将快照的metapage写到pagecache中，并将快照加到sync队列
     * @return: 成功返回0，失败返回错误码，错误码为负数
     */
    CSErrorCode Flush();
    /**
     * 获取快照版本号
     * @return: 返回快照版本号
     */
    SequenceNum GetSn();
    /**
     * 获取快照文件中指定块的状态
     * @param pageIndex: 表示指定的块在快照中的偏移，每个块大小为4KB
     * @return: 如果块被写过返回true，块没被写过返回false
     */
    bool IsPageWritten(uint32_t pageIndex);
    /**
     * 判断当前快照文件是否已经损坏
     * @return: 损坏返回true，未损坏返回false
     */
    bool IsDamaged();
    /**
     * 标坏当前快照，并将damaged标记，写到pagecache
     * 如果需要刷盘，调用Sync接口
     * @return: 返回错误码
     */
    CSErrorCode SetDamaged();

 private:
    /**
     * 将metapage持久化
     * @param metaPage:需要持久化到磁盘的metapage,
     *                 如果成功持久化，会更改当前内存的metapage
     *                 如果失败，则不会更改
     */
    CSErrorCode updateMetaPage(SnapshotMetaPage* metaPage);
    /**
     * 将metapage加载到内存
     */
    CSErrorCode loadMetaPage();

    inline string path() {
        return baseDir_ + "/" +
               FileNameOperator::GenerateSnapshotName(chunkId_, metaPage_.sn);
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
        return lfs_->Write(fd_, buf, offset + pageSize_, length);
    }

 private:
    // 快照文件资源描述符
    int fd_;
    // 快照所属chunk的id
    ChunkID chunkId_;
    // 快照文件逻辑大小，不包括metapage
    ChunkSizeType size_;
    // 最小原子读写单元,同时也是metapage的大小
    PageSizeType pageSize_;
    // 快照文件所在目录
    std::string baseDir_;
    // 快照文件的metapage
    SnapshotMetaPage metaPage_;
    // 被写过但还未更新到metapage中的page索引
    std::set<uint32_t> dirtyPages_;
    // 依赖本地文件系统操作文件
    std::shared_ptr<LocalFileSystem> lfs_;
    // 依赖chunkfilepool创建删除文件
    std::shared_ptr<ChunkfilePool> chunkfilePool_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVEFS_CHUNKSERVER_CHUNKSERVER_SNAPSHOT_H
