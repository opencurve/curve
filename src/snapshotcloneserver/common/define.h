/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_DEFINE_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_DEFINE_H_

#include <string>

namespace curve {
namespace snapshotcloneserver {

typedef std::string UUID;
using TaskIdType = UUID;

enum class CloneTaskType {
    kClone = 0,
    kRecover
};

// 转储chunk分片大小
const uint64_t kChunkSplitSize = 1024u * 1024u;
// CheckSnapShotStatus调用间隔
const uint32_t kCheckSnapshotStatusIntervalMs = 1000u;
// CloneTaskManager 后台线程扫描间隔
const uint32_t kCloneTaskManagerScanIntervalMs = 1000;
// 快照后台线程扫描等待队列和工作队列的扫描周期(单位：ms)
const uint32_t kSnapshotTaskManagerScanIntervalMs = 1000;

// 快照工作线程数
const int kSnapsnotPoolThreadNum = 8;
// 克隆恢复工作线程数
const int kClonePoolThreadNum = 8;

// clone chunk分片大小
const uint64_t kCloneChunkSplitSize = 1024u * 1024u;

// 未初始序列号
const uint64_t kUnInitializeSeqNum = 0;
// 初始序列号
const uint64_t kInitializeSeqNum = 1;

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
// 错误码：非法的用户
const int kErrCodeInvalidUser = -12;
// 错误码：文件不存在
const int kErrCodeFileNotExist = -13;
// 错误码：任务已存在
const int kErrCodeTaskExist = -14;
// 错误码：不能从未完成或存在错误的快照克隆
const int kErrCodeInvalidSnapshot = -15;
// 错误码：不能删除正在克隆的快照
const int kErrCodeSnapshotCannotDeleteCloning = -16;
// 错误码：文件状态异常
const int kErrCodeFileStatusInvalid = -17;
// 错误码：非法请求
const int kErrCodeInvalidRequest = -18;

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_DEFINE_H_
