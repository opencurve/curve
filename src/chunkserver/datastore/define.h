/*
 * Project: curve
 * Created Date: Tuesday December 4th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_DATASTORE_DEFINE_H_
#define SRC_CHUNKSERVER_DATASTORE_DEFINE_H_

namespace curve {
namespace chunkserver {

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
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_DEFINE_H_
