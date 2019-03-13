/*
 * Project: curve
 * Created Date: Monday March 11th 2019
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_CLONE_MANAGER_H_
#define SRC_CHUNKSERVER_CLONE_MANAGER_H_

#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <thread>  // NOLINT
#include <mutex>   // NOLINT
#include <memory>
#include <vector>
#include <string>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/chunkserver/clone_task.h"
#include "src/chunkserver/clone_core.h"

namespace curve {
namespace chunkserver {

using curve::common::TaskThreadPool;

class ReadChunkRequest;

struct CloneOptions {
    // 核心逻辑处理类
    std::shared_ptr<CloneCore> core;
    // 最大线程数
    uint32_t threadNum;
    // 最大队列深度
    uint32_t queueCapacity;
    // 任务状态检查的周期,单位ms
    uint32_t checkPeriod;
    CloneOptions() : core(nullptr)
                   , threadNum(10)
                   , queueCapacity(100)
                   , checkPeriod(5000) {}
};

class CloneManager {
 public:
    CloneManager();
    virtual ~CloneManager();

    /**
     * 初始化
     *
     * @param options[in]:初始化参数
     * @return 错误码
     */
    virtual int Init(const CloneOptions& options);

    /**
     * 启动所有线程
     *
     * @return 成功返回0，失败返回-1
     */
    virtual int Run();

    /**
     * 停止所有线程
     *
     * @return 成功返回0，失败返回-1
     */
    virtual int Fini();

    /**
     * 生成克隆任务
     * @param request[in]:请求信息
     * @return:返回生成的克隆任务，如果生成失败，返回nullptr
     */
    virtual std::shared_ptr<CloneTask> GenerateCloneTask(
        std::shared_ptr<ReadChunkRequest> request,
        ::google::protobuf::Closure* done);

    /**
     * 发布克隆任务，产生克隆任务放到线程池中处理
     * @param task[in]:克隆任务
     * @return  成功返回true，失败返回false
     */
    virtual bool IssueCloneTask(std::shared_ptr<CloneTask> cloneTask);

 private:
    // 克隆任务管理相关的选项，调Init的时候初始化
    CloneOptions options_;
    // 处理克隆任务的异步线程池
    std::shared_ptr<TaskThreadPool> tp_;
    // 当前线程池是否处于工作状态
    std::atomic<bool> isRunning_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_MANAGER_H_
