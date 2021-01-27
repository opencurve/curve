/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * File Created: Saturday, 23rd February 2019 1:41:23 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_LEASE_EXECUTOR_H_
#define SRC_CLIENT_LEASE_EXECUTOR_H_

#include <brpc/periodic_task.h>

#include <memory>
#include <string>

#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/iomanager4file.h"
#include "src/client/mds_client.h"

namespace curve {
namespace client {

class RefreshSessionTask;

/**
 * lease refresh结果，session如果不存在就不需要再续约
 * 如果session存在但是lease续约失败,继续续约
 * 续约成功了FInfo_t中才会有对应的文件信息
 */
struct LeaseRefreshResult {
    enum class Status {
        OK,
        FAILED,
        NOT_EXIST
    };
    Status status;
    FInfo_t finfo;
};

/**
 * 每个vdisk对应的fileinstance都会与mds保持心跳
 * 心跳通过LeaseExecutor实现，LeaseExecutor定期
 * 去mds续约，同时将mds端当前file最新的版本信息带回来
 * 然后检查版本信息是否变更，如果变更就需要通知iomanager
 * 更新版本。如果续约失败，就需要将用户新发下来的io直接错误返回
 */
class LeaseExecutor {
 public:
    /**
     * 构造函数
     * @param: leaseopt为当前lease续约的option配置
     * @param: mdsclient是与mds续约的client
     * @param: iomanager会在续约失败或者版本变更的时候进行io调度
     */
    LeaseExecutor(const LeaseOption_t& leaseOpt,
                 UserInfo_t userinfo,
                 MDSClient* mdscllent,
                 IOManager4File* iomanager);

    ~LeaseExecutor();

    /**
     * LeaseExecutor需要finfo保存filename
     * LeaseSession_t是当前leaeexcutor的执行配置
     * @param: fi为当前需要续约的文件版本信息
     * @param: lease为续约的lease信息
     * @return: 成功返回true，否则返回false
     */
    bool Start(const FInfo_t& fi, const LeaseSession_t&  lease);

    /**
     * 获取当前lease的sessionid信息，外围close文件的时候需要用到
     */
    std::string GetLeaseSessionID();

    /**
     * 停止续约
     */
    void Stop();

    /**
     * 当前lease如果续约失败则通知iomanagerdisable io
     */
    bool LeaseValid();

    /**
     * 测试使用，主动失效增加刷新失败
     */
    void InvalidLease() {
        for (int i = 0; i <= leaseoption_.mdsRefreshTimesPerLease; i++) {
            IncremRefreshFailed();
        }
    }

    /**
     * @brief 续约任务执行者
     * @return 是否继续执行refresh session任务
     */
    bool RefreshLease();

    /**
     * @brief 测试使用，重置refresh session task
     */
    void ResetRefreshSessionTask();

 private:
    /**
     *  一个lease期间会续约rfreshTimesPerLease次，每次续约失败就递增
     * 当连续续约rfreshTimesPerLease次失败的时候，则disable IO
     */
    void IncremRefreshFailed();

    /**
     * @brief Updating local file information consistent with MDS record.
     *        Currently, only seqnum and file status are updated
     * @param fileInfo Latest file information returned by refresh session RPC
     */
    void CheckNeedUpdateFileInfo(const FInfo& fileInfo);

 private:
    // 与mds进行lease续约的文件名
    std::string             fullFileName_;

    // 用于续约的client
    MDSClient*              mdsclient_;

    // 用于发起refression的user信息
    UserInfo_t              userinfo_;

    // IO管理者，当文件需要更新版本信息或者disable io的时候调用其接口
    IOManager4File*         iomanager_;

    // 当前lease执行的配置信息
    LeaseOption_t           leaseoption_;

    // mds端传过来的lease信息，包含当前文件的lease时长，及sessionid
    LeaseSession_t          leasesession_;

    // 记录当前lease是否可用
    std::atomic<bool>       isleaseAvaliable_;

    // 记录当前连续续约失败的次数
    std::atomic<uint64_t>   failedrefreshcount_;

    // refresh session定时任务，会间隔固定时间执行一次
    std::unique_ptr<RefreshSessionTask> task_;
};

// RefreshSessin定期任务
// 利用brpc::PeriodicTaskManager进行管理
// 定时器触发时调用OnTriggeringTask，根据返回值决定是否继续定时触发
// 如果不再继续触发，调用OnDestroyingTask进行清理操作
class RefreshSessionTask : public brpc::PeriodicTask {
 public:
    using Task = std::function<bool(void)>;

    RefreshSessionTask(LeaseExecutor* leaseExecutor,
                       uint64_t intervalUs)
        : leaseExecutor_(leaseExecutor),
          refreshIntervalUs_(intervalUs),
          stopped_(false),
          stopMtx_(),
          terminated_(false),
          terminatedMtx_(),
          terminatedCv_() {}

    RefreshSessionTask(const RefreshSessionTask& other)
        : leaseExecutor_(other.leaseExecutor_),
          refreshIntervalUs_(other.refreshIntervalUs_),
          stopped_(false),
          stopMtx_(),
          terminated_(false),
          terminatedMtx_(),
          terminatedCv_() {}

    virtual ~RefreshSessionTask() = default;

    /**
     * @brief 定时器超时后执行当前函数
     * @param next_abstime 任务下次执行的绝对时间
     * @return true 继续定期执行当前任务
     *         false 停止执行当前任务
     */
    bool OnTriggeringTask(timespec* next_abstime) override {
        std::lock_guard<std::mutex> lk(stopMtx_);
        if (stopped_) {
            return false;
        }

        *next_abstime = butil::microseconds_from_now(refreshIntervalUs_);
        return leaseExecutor_->RefreshLease();
    }

    /**
     * @brief 停止再次执行当前任务
     */
    void Stop() {
        std::lock_guard<std::mutex> lk(stopMtx_);
        stopped_ = true;
    }

    /**
     * @brief 任务停止后调用
     */
    void OnDestroyingTask() override {
        std::unique_lock<std::mutex> lk(terminatedMtx_);
        terminated_ = true;
        terminatedCv_.notify_one();
    }

    /**
     * @brief 等待任务退出
     */
    void WaitTaskExit() {
        std::unique_lock<std::mutex> lk(terminatedMtx_);
        terminatedCv_.wait(lk, [this]() { return terminated_ == true; });
    }

    /**
     * @brief 获取refresh session时间间隔(us)
     * @return refresh session任务时间间隔(us)
     */
    uint64_t RefreshIntervalUs() const {
        return refreshIntervalUs_;
    }

 private:
    LeaseExecutor* leaseExecutor_;
    uint64_t refreshIntervalUs_;

    bool stopped_;
    std::mutex stopMtx_;

    bool terminated_;
    std::mutex terminatedMtx_;
    std::condition_variable terminatedCv_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_LEASE_EXECUTOR_H_
