/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_DEFINE_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_DEFINE_H_

#include <string>
#include <map>

namespace curve {
namespace snapshotcloneserver {

// snapshotcloneservice字符串常量定义
extern const char* kServiceName;
// action
extern const char* kCreateSnapshotAction;
extern const char* kDeleteSnapshotAction;
extern const char* kCancelSnapshotAction;
extern const char* kGetFileSnapshotInfoAction;
extern const char* kCloneAction;
extern const char* kRecoverAction;
extern const char* kGetCloneTasksAction;
extern const char* kCleanCloneTaskAction;
extern const char* kFlattenAction;

// param
extern const char* kActionStr;
extern const char* kVersionStr;
extern const char* kUserStr;
extern const char* kFileStr;
extern const char* kNameStr;
extern const char* kUUIDStr;
extern const char* kLimitStr;
extern const char* kOffsetStr;
extern const char* kSourceStr;
extern const char* kDestinationStr;
extern const char* kLazyStr;

// json key
extern const char* kCodeStr;
extern const char* kMessageStr;
extern const char* kRequestIdStr;
extern const char* kTotalCountStr;
extern const char* kSnapshotsStr;
extern const char* kTaskInfosStr;


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
// 错误码: 不能对存在异常快照的文件打快照，或不能对存在错误的目标文件克隆/恢复
const int kErrCodeSnapshotCannotCreateWhenError = -13;
// 错误码：取消的快照已完成
const int kErrCodeCannotCancelFinished = -14;
// 错误码：不能从未完成或存在错误的快照克隆
const int kErrCodeInvalidSnapshot = -15;
// 错误码：不能删除正在克隆的快照
const int kErrCodeSnapshotCannotDeleteCloning = -16;
// 错误码：不能清理未完成的克隆
const int kErrCodeCannotCleanCloneUnfinished = -17;
// 错误码：快照到达上限
const int kErrCodeSnapshotCountReachLimit = -18;
// 错误码：文件已存在
const int kErrCodeFileExist = -19;
// 错误码：克隆任务已满
const int kErrCodeTaskIsFull = -20;
// 错误码：不支持
const int kErrCodeNotSupport = -21;

extern std::map<int, std::string> code2Msg;

std::string BuildErrorMessage(
    int errCode,
    const std::string &requestId,
    const std::string &uuid = "");


// clone progress
constexpr uint32_t kProgressCloneStart = 0;
constexpr uint32_t kProgressCloneError = kProgressCloneStart;
constexpr uint32_t kProgressCreateCloneFile = 1;
constexpr uint32_t kProgressCreateCloneMeta = 2;
constexpr uint32_t kProgressMetaInstalled = 5;
constexpr uint32_t kProgressRecoverChunkBegin = kProgressMetaInstalled;
constexpr uint32_t kProgressRecoverChunkEnd = 95;
constexpr uint32_t kProgressCloneComplete = 100;



}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_DEFINE_H_
