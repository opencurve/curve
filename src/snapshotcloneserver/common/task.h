/*
 * Project: curve
 * Created Date: Thu Mar 21 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_TASK_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_TASK_H_

#include <functional>
#include <memory>
#include "src/snapshotcloneserver/common/define.h"

namespace curve {
namespace snapshotcloneserver {

class Task {
 public:
    explicit Task(const TaskIdType &taskId)
        : taskId_(taskId) {}

    virtual ~Task() {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&&) = default;
    Task& operator=(Task&&) = default;

    /**
     * @brief 获取快照任务执行体闭包
     *
     * @return 快照任务执行体
     */
    virtual std::function<void()> clousre() {
        return [this] () {
            Run();
        };
    }

    /**
     * @brief 获取快照任务id
     *
     * @return 快照任务id
     */
    TaskIdType GetTaskId() const {
        return taskId_;
    }

    /**
     * @brief 快照执行函数接口
     */
    virtual void Run() = 0;

 private:
    // 快照id
    TaskIdType taskId_;
};


}  // namespace snapshotcloneserver
}  // namespace curve


#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_TASK_H_
