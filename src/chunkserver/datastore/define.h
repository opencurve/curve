/*
 * Project: curve
 * Created Date: Tuesday December 4th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_DATASTORE_DEFINE_H_
#define SRC_CHUNKSERVER_DATASTORE_DEFINE_H_

#include <string>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/bitmap.h"

namespace curve {
namespace chunkserver {

using curve::common::Bitmap;

const uint8_t FORMAT_VERSION = 1;

// define error code
enum CSErrorCode {
    // 成功
    Success = 0,
    // 内部错误，一般为系统调用时出错
    InternalError = 1,
    // 版本不兼容
    IncompatibleError = 2,
    // crc校验失败
    CrcCheckError = 3,
    // 文件格式不正确，例如文件长度不正确
    FileFormatError = 4,
    // 快照冲突，存在多个快照文件
    SnapshotConflictError = 5,
    // 版本落后的请求，如果是日志恢复时抛的错误属于正常情况
    BackwardRequestError = 6,
    // 删除chunk时如果存在快照文件时抛出，不允许删除存在快照的chunk
    SnapshotExistError = 7,
    // 请求读写的chunk不存在
    ChunkNotExistError = 8,
    // 请求读写的区域超过了文件的大小
    OutOfRangeError = 9,
    // 参数错误
    InvalidArgError = 10,
    // 创建chunk时存在冲突的chunk
    ChunkConflictError = 11,
};

// Chunk的详细信息
struct CSChunkInfo {
    // chunk的id
    ChunkID  chunkId;
    // page的大小
    uint32_t pageSize;
    // chunk的大小
    uint32_t chunkSize;
    // chunk文件的版本号
    SequenceNum curSn;
    // chunk快照的版本号，如果快照不存在，则为0
    SequenceNum snapSn;
    // chunk的修正版本号
    SequenceNum correctedSn;
    // 表示chunk是否为CloneChunk
    bool isClone;
    // 如果是CloneChunk，表示数据源的位置；否则为空
    std::string location;
    // 如果是CloneChunk表示当前Chunk的page的状态，否则为nullptr
    std::shared_ptr<Bitmap> bitmap;
    CSChunkInfo() : chunkId(0)
                  , pageSize(4096)
                  , chunkSize(16 * 4096 * 4096)
                  , curSn(0)
                  , snapSn(0)
                  , correctedSn(0)
                  , isClone(false)
                  , location("")
                  , bitmap(nullptr) {}
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_DEFINE_H_
