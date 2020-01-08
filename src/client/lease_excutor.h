/*
 * Project: curve
 * File Created: Saturday, 23rd February 2019 1:41:23 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_LEASE_EXCUTOR_H_
#define SRC_CLIENT_LEASE_EXCUTOR_H_

#include <string>

#include "src/client/mds_client.h"
#include "src/client/client_config.h"
#include "src/client/client_common.h"
#include "src/client/iomanager4file.h"
#include "src/client/timertask_worker.h"

namespace curve {
namespace client {
/**
 * lease refresh结果，session如果不存在就不需要再续约
 * 如果session存在但是lease续约失败,继续血月
 * 续约成功了FInfo_t中才会有对应的文件信息
 */
struct leaseRefreshResult {
    enum Status {
        OK,
        FAILED,
        NOT_EXIST
    };
    Status status;
    FInfo_t finfo;
};

/**
 * 每个vdisk对应的fileinstance都会与mds保持心跳
 * 心跳通过leaseexcutor实现，leaseexcutor定期
 * 去mds续约，同时将mds端当前file最新的版本信息带回来
 * 然后检查版本信息是否变更，如果变更就需要通知iomanager
 * 更新版本。如果续约失败，就需要将用户新发下来的io直接错误返回
 */
class LeaseExcutor {
 public:
    /**
     * 构造函数
     * @param: leaseopt为当前lease续约的option配置
     * @param: mdsclient是与mds续约的client
     * @param: iomanager会在续约失败或者版本变更的时候进行io调度
     */
    LeaseExcutor(const LeaseOption_t& leaseOpt,
                 UserInfo_t userinfo,
                 MDSClient* mdscllent,
                 IOManager4File* iomanager);
    ~LeaseExcutor() = default;

    /**
     * leaseexcutor需要finfo保存filename和更新seqnum
     * LeaseSession_t是当前leaeexcutor的执行配置
     * @param: fi为当前需要续约的文件版本信息
     * @param: lease为续约的lease信息
     * @return: 成功返回true，否则返回false
     */
    bool Start(FInfo_t fi, LeaseSession_t  lease);
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

    // 测试使用
    TimerTask* GetTimerTask() const {
        return refreshTask_;
    }

    // 测试使用
    void SetTimerTask(TimerTask* task) {
        timerTaskWorker_.AddTimerTask(refreshTask_);
        LOG(INFO) << "add timer task "
              << refreshTask_->GetTimerID()
              << " for lease refresh!";
        isleaseAvaliable_.store(true);
    }

 private:
    /**
     * 续约任务执行者
     */
    void RefreshLease();

    /**
     *  一个lease期间会续约rfreshTimesPerLease次，每次续约失败就递增
     * 当连续续约rfreshTimesPerLease次失败的时候，则disable IO
     */
    void IncremRefreshFailed();

    /**
     * 每次续约会携带新的文件信息，该函数检查当前文件是否需要更新版本信息
     * @param: newversion是lease续约后从mds端携带回来的版本号
     */
    void CheckNeedUpdateVersion(uint64_t newversion);

 private:
    // 当前文件的基本信息，leaseexcutor会用到seqnum、finame
    FInfo_t                 finfo_;

    // 用于续约的client
    MDSClient*              mdsclient_;

    // 用于发起refression的user信息
    UserInfo_t              userinfo_;

    // IO管理者，当文件需要更新版本信息或者disable io的时候调用其接口
    IOManager4File*         iomanager_;

    // 定时器执行的续约任务，隔固定时间执行一次
    TimerTask*              refreshTask_;

    // 当前lease执行的配置信息
    LeaseOption_t           leaseoption_;

    // mds端传过来的lease信息，包含当前文件的lease时长，及sessionid
    LeaseSession_t          leasesession_;

    // 执行定时任务的定时器
    TimerTaskWorker         timerTaskWorker_;

    // 记录当前lease是否可用
    std::atomic<bool>       isleaseAvaliable_;

    // 记录当前连续续约失败的次数
    std::atomic<uint64_t>   failedrefreshcount_;
};
}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_LEASE_EXCUTOR_H_
