/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOT_DEFINE_H_
#define CURVE_SRC_SNAPSHOT_SNAPSHOT_DEFINE_H_

#include <string>

namespace curve {
namespace snapshotserver {

typedef std::string UUID;

// 转储chunk分片大小
const uint64_t kChunkSplitSize = 1024u * 1024u;
// CheckSnapShotStatus调用间隔
const uint32_t kCheckSnapshotStatusIntervalMs = 1000u;

// 错误码：执行成功
const int kErrCodeSnapshotServerSuccess = 0;
// 错误码: 内部错误
const int kErrCodeSnapshotInternalError = -1;
// 错误码：转储服务器初始化失败
const int kErrCodeSnapshotServerInitFail = -2;
// 错误码：转储服务器启动失败
const int kErrCodeSnapshotServerStartFail = -3;
// 错误码：服务已停止
const int kErrCodeSnapshotServiceIsStop = -4;
// 错误码：删除任务已存在
const int kErrCodeSnapshotDeleteTaskExist = -5;
// 错误码：用户不匹配
const int kErrCodeSnapshotUserNotMatch = -6;
// 错误码：目标文件名不匹配
const int kErrCodeSnapshotFileNameNotMatch = -7;
// 错误码：chunk大小未按chunk分片大小对齐
const int kErrCodeSnapshotChunkSizeNotAligned = -8;
// 错误码： 不能删除未完成的快照
const int kErrCodeSnapshotCannotDeleteUnfinished = -9;
// 错误码: 不能对存在异常快照的文件打快照
const int kErrCodeSnapshotCannotCreateWhenError = -10;
// 错误码：取消的快照已完成
const int kErrCodeCannotCancelFinished = -11;

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_DEFINE_H_
