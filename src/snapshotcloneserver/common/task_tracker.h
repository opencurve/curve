/*
 * Project: curve
 * Created Date: Thu Sep 12 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_TASK_TRACKER_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_TASK_TRACKER_H_

#include <vector>
#include <memory>
#include <list>

#include "src/common/concurrent/concurrent.h"
#include "src/snapshotcloneserver/common/define.h"

using ::curve::common::Mutex;
using ::curve::common::ConditionVariable;

namespace curve {
namespace snapshotcloneserver {

// forward declaration
class TrackerTask;

struct RecoverChunkContext;

// 并发任务跟踪模块
class TaskTracker : public std::enable_shared_from_this<TaskTracker> {
 public:
    TaskTracker()
    : concurrent_(0),
      lastErr_(kErrCodeSuccess) {}

    /**
     * @brief 增加一个追踪任务
     */
    void AddOneTrace();

    /**
     * @brief 获取任务数量
     *
     * @return 任务数量
     */
    uint32_t GetTaskNum() const {
        return concurrent_;
    }

    /**
     * @brief 处理任务返回值
     *
     * @param retCode 返回值
     */
    void HandleResponse(int retCode);

    /**
     * @brief 等待追踪的所有任务完成
     */
    void Wait();

    /**
     * @brief 等待完成任意数量任务
     *
     * @param num 完成任务数量
     */
    void WaitSome(uint32_t num);

    /**
     * @brief 获取最后一个错误
     *
     * @return 错误码
     */
    int GetResult() {
        return lastErr_;
    }

 private:
    // 等待的条件变量
    ConditionVariable cv_;
    Mutex cv_m;
    // 并发数量
    std::atomic<uint32_t> concurrent_;
    // 错误码
    int lastErr_;
};

template <typename CTX>
class ContextTaskTracker : public TaskTracker {
 public:
     void PushResultContext(const CTX &ctx);
     std::list<CTX> PopResultContexts();

 private:
     Mutex ctxMutex_;
     std::list<CTX> contexts_;
};

template <typename CTX>
void ContextTaskTracker<CTX>::PushResultContext(const CTX &ctx) {
    std::unique_lock<Mutex> lk(ctxMutex_);
    contexts_.push_back(ctx);
}

template <typename CTX>
std::list<CTX>
    ContextTaskTracker<CTX>::PopResultContexts() {
    std::unique_lock<Mutex> lk(ctxMutex_);
    std::list<CTX> ret;
    ret.swap(contexts_);
    return ret;
}

using RecoverChunkContextPtr = std::shared_ptr<RecoverChunkContext>;
using RecoverChunkTaskTracker = ContextTaskTracker<RecoverChunkContextPtr>;

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_TASK_TRACKER_H_
