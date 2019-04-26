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

// 未初始序列号
const uint64_t kUnInitializeSeqNum = 0;
// 初始序列号
const uint64_t kInitializeSeqNum = 1;

// 错误码：执行成功
const int kErrCodeSuccess = 0;
// 错误码: 内部错误
const int kErrCodeInternalError = -1;
// 错误码：服务器初始化失败
const int kErrCodeServerInitFail = -2;
// 错误码：服务器启动失败
const int kErrCodeServerStartFail = -3;
// 错误码：服务已停止
const int kErrCodeServiceIsStop = -4;
// 错误码：非法请求
const int kErrCodeInvalidRequest = -5;
// 错误码：任务已存在
const int kErrCodeTaskExist = -6;
// 错误码：非法的用户
const int kErrCodeInvalidUser = -7;
// 错误码：文件不存在
const int kErrCodeFileNotExist = -8;
// 错误码：文件状态异常
const int kErrCodeFileStatusInvalid = -9;
// 错误码：chunk大小未按chunk分片大小对齐
const int kErrCodeChunkSizeNotAligned = -10;
// 错误码：文件名不匹配
const int kErrCodeFileNameNotMatch = -11;
// 错误码： 不能删除未完成的快照
const int kErrCodeSnapshotCannotDeleteUnfinished = -12;
// 错误码: 不能对存在异常快照的文件打快照
const int kErrCodeSnapshotCannotCreateWhenError = -13;
// 错误码：取消的快照已完成
const int kErrCodeCannotCancelFinished = -14;
// 错误码：不能从未完成或存在错误的快照克隆
const int kErrCodeInvalidSnapshot = -15;
// 错误码：不能删除正在克隆的快照
const int kErrCodeSnapshotCannotDeleteCloning = -16;
// 错误码：不能清理非错误的克隆
const int kErrCodeCannotCleanCloneNotError = -17;
// 错误码：快照到达上限
const int kErrCodeSnapshotCountReachLimit = -18;

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_DEFINE_H_
