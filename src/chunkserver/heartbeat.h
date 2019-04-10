/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/12/20  Wenyu Zhou   Initial version
 */

#ifndef SRC_CHUNKSERVER_HEARTBEAT_H_
#define SRC_CHUNKSERVER_HEARTBEAT_H_

#include <braft/node_manager.h>
#include <braft/node.h>                  // NodeImpl

#include <map>
#include <vector>
#include <string>
#include <atomic>
#include <thread>  //NOLINT

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "proto/heartbeat.pb.h"

namespace curve {
namespace chunkserver {

using HeartbeatRequest  = curve::mds::heartbeat::ChunkServerHeartbeatRequest;
using HeartbeatResponse = curve::mds::heartbeat::ChunkServerHeartbeatResponse;
using ConfigChangeInfo  = curve::mds::heartbeat::ConfigChangeInfo;
using CopysetConf       = curve::mds::heartbeat::CopysetConf;
using CandidateError    = curve::mds::heartbeat::CandidateError;
using TaskStatus        = butil::Status;

class HeartbeatTask;
class CopysetInfo;

using CopysetNodePtr            = std::shared_ptr<CopysetNode>;
using CopysetInfoPtr            = std::shared_ptr<CopysetInfo>;
using HeartbeatTaskPtr          = std::shared_ptr<HeartbeatTask>;

/**
 * 心跳子系统选项
 */
struct HeartbeatOptions {
    ChunkServerID           chunkserverId;
    std::string             chunkserverToken;
    std::string             dataUri;
    std::string             mdsIp;
    std::string             ip;
    uint16_t                mdsPort;
    uint16_t                port;
    uint32_t                interval;
    uint32_t                timeout;
    CopysetNodeManager*     copysetNodeManager;

    std::shared_ptr<LocalFileSystem> fs;
};

/**
 * 心跳任务类型
 */
enum TASK_TYPE {
    TASK_TYPE_ADD_PEER,
    TASK_TYPE_TRANSFER_LEADER,
    TASK_TYPE_REMOVE_PEER,
    TASK_TYPE_CLEAN_PEER,
    TASK_TYPE_NONE,
};

/**
 * 心跳任务处理类
 */
class HeartbeatTask {
 public:
    HeartbeatTask() {}
    ~HeartbeatTask() {}

    /**
     * @brief 初始化心跳任务
     * @param[in] type 任务类型
     * @param[in] peerId 心跳任务的目标成员
     */
    void NewTask(TASK_TYPE type, const PeerId& peerId);

    /**
     * @brief 获取心跳任务所针对的目标成员
     * @return 目标成员的ID
     */
    PeerId& GetPeerId();

    /**
     * @brief 获取心跳任务的类型
     * @return 任务类型
     */
    TASK_TYPE GetType();

    /**
     * @brief 设置心跳任务的状态
     * @param[in] status 目标状态
     */
    void SetStatus(const TaskStatus& status);

    /**
     * @brief 获取心跳任务的状态
     * @return 任务状态
     */
    TaskStatus& GetStatus();

 private:
    /*
     * 任务类型
     */
    TASK_TYPE                       type_;

    /*
     * 任务的目标对象成员ID
     */
    PeerId                          peerId_;

    /*
     * 任务状态
     */
    TaskStatus                      status_;
};

/**
 * 复制组信息类,封装心跳系统所用到的各种复制组信息和接口
 */
class CopysetInfo : public std::enable_shared_from_this<CopysetInfo> {
 public:
    CopysetInfo(LogicPoolID poolId, CopysetID copysetId);
    ~CopysetInfo();

    /**
     * @brief 初始化心跳任务
     * @param[in] type 任务类型
     * @param[in] peerId 心跳任务的目标成员
     */
    void NewTask(TASK_TYPE type, const PeerId& peerId);

    /**
     * @brief 结束心跳任务
     * @param[in] status 任务将被设置的状态
     */
    void FinishTask(const TaskStatus& status);

    /**
     * @brief 判断复制组是否为Leader
     * @return 返回是否为Leader
     */
    bool IsLeader();

    /**
     * @brief 获取复制组的Leader
     * @return 复制组Leader的ID
     */
    int GetLeader(PeerId* leader);

    /**
     * @brief 获取复制组的Raft实现实例
     * @param[out] node 出参,复制组实例的引用
     * @return 0:成功，非0失败
     */
    int GetNode(scoped_refptr<braft::NodeImpl>* node);

    /**
     * @brief 获取复制组的成员列表
     * @param[out] peers 出参,复制组成员列表
     * @return 0:成功，非0失败
     */
    int ListPeers(std::vector<PeerId>* peers);

    /**
     * @brief 切换复制组的Leader
     * @param[in] peerId 目标Leader的成员ID
     * @return 心跳任务的引用
     */
    TaskStatus TransferLeader(const PeerId& peerId);

    /**
     * @brief 复制组添加新成员
     * @param[in] peerId 新成员的ID
     * @return 心跳任务的引用
     */
    TaskStatus AddPeer(const PeerId& peerId);

    /**
     * @brief 复制组删除成员
     * @param[in] peerId 将要删除成员的ID
     * @return 心跳任务的引用
     */
    TaskStatus RemovePeer(const PeerId& peerId);

    /**
     * @brief 清理复制组实例及持久化数据
     * @return 心跳任务的引用
     */
    TaskStatus CleanPeer();

    /**
     * @brief 获取复制组ID
     * @return 复制组ID
     */
    CopysetID GetCopysetId();

    /**
     * @brief 获取复制组所在的逻辑池的ID
     * @return 复制组所在的逻辑池的ID
     */
    LogicPoolID GetLogicPoolId();

    /**
     * @brief 获取复制组的组ID
     * @return 心复制组的组ID
     */
    GroupNid GetGroupId();

    /**
     * @brief 获取复制组活跃任务的状态
     * @return 任务状态
     */
    TaskStatus& GetStatus();

    /**
     * @brief 获取复制组活跃任务的目标成员
     * @return 复制组活跃任务的目标成员
     */
    PeerId& GetPeerInTask();

    /**
     * @brief 获取复制组的运行实例
     * @return 复制组实例的引用
     */
    CopysetNodePtr GetCopysetNode();

    /**
     * @brief 设置复制组的运行实例
     * @param[in] copyset 复制组实例的引用
     */
    void SetCopysetNode(CopysetNodePtr copyset);

    /**
     * @brief 判断复制组的任务是否结束
     */
    bool IsTaskOngoing();

    /**
     * @brief 判断复制组是否有正在进行的任务
     */
    bool HasTask();

    /**
     * @brief 清理复制组的心跳任务信息
     */
    void ReleaseTask();

    /**
     * @brief 更新复制组信息的心跳周期，根据跟全局最新心跳周期比较可判断复制组
     * 是否活跃, 以清理过时的复制组信息
     * @param[in] term 将要给复制组信息设置的目标心跳周期
     */
    void UpdateTerm(uint64_t term);

    /**
     * @brief 获取复制组信息的心跳周期
     * @return 心跳周期
     */
    uint64_t GetTerm();

    /**
     * @brief 获取复制组实例的epoch
     * @return 复制组实例的epoch
     */
    uint64_t GetEpoch();

    /**
     * @brief 设置复制组管理器
     * @param[in] man 复制组管理器的引用
     */
    static void SetCopysetNodeManager(CopysetNodeManager* man);

 private:
    /*
     * Copyset逻辑池ID
     */
    LogicPoolID             poolId_;

    /*
     * Copyset ID
     */
    CopysetID               copysetId_;

    /*
     * CopysetInfo对应的Copyset实例引用
     */
    CopysetNodePtr          copyset_;

    /*
     * CopysetInfo当前的心跳周期号，每个心跳一个term号，跟raft的term没有关系，
     * 用来区别活跃与非活跃Copyset跟踪项
     */
    uint64_t                term_;

    /*
     * Copyset是否有任务在进行
     */
    std::atomic<bool>       taskOngoing_;

    /*
     * Copyset是否存在任务
     */
    std::atomic<bool>       newTask_;

    /*
     * Copyset对应的任务实例引用
     */
    HeartbeatTaskPtr        task_;

    /*
     * Copyset管理模块引用
     */
    static CopysetNodeManager*  copysetNodeManager_;
};

/**
 * 心跳子系统处理模块
 */
class Heartbeat {
 public:
    Heartbeat() {}
    ~Heartbeat() {}

    /**
     * @brief 初始化心跳子系统
     * @param[in] options 心跳子系统选项
     * @return 0:成功，非0失败
     */
    int Init(const HeartbeatOptions& options);

    /**
     * @brief 清理心跳子系统
     * @return 0:成功，非0失败
     */
    int Fini();

    /**
     * @brief 启动心跳子系统
     * @return 0:成功，非0失败
     */
    int Run();

    /**
     * @brief 停止心跳子系统
     * @return 0:成功，非0失败
     */
    int Stop();

 private:
    /*
     * 心跳工作线程
     */
    static void HeartbeatWorker(Heartbeat *heartbeat);

    /*
     * 更新Copyset当前状态信息到Copyset信息表
     */
    int UpdateCopysetInfo(const std::vector<CopysetNodePtr>& copysets);

    /*
     * 清理Copyset信息表中非活跃的Copyset信息项
     */
    void CleanAgingCopysetInfo();

    /*
     * 获取指定的Copyset信息项目
     */
    CopysetInfoPtr GetCopysetInfo(LogicPoolID poolId, CopysetID copysetId);

    /*
     * 获取指定的Copyset信息项目
     */
    CopysetInfoPtr GetCopysetInfo(CopysetNodePtr copyset);

    /*
     * 获取Chunkserver存储空间信息
     */
    int GetFileSystemSpaces(size_t* capacity, size_t* free);

    /*
     * 构建心跳消息的Copyset信息项
     */
    int BuildCopysetInfo(curve::mds::heartbeat::CopysetInfo* info,
                         CopysetNodePtr copyset);

    /*
     * 构建心跳请求
     */
    int BuildRequest(HeartbeatRequest* request);

    /*
     * 发送心跳消息
     */
    int SendHeartbeat(const HeartbeatRequest& request,
                      HeartbeatResponse* response);

    /*
     * 执行心跳任务
     */
    int ExecTask(const HeartbeatResponse& response);

    /*
     * 等待下一个心跳时间点
     */
    void WaitForNextHeartbeat();

    /*
     * 输出心跳请求信息
     */
    void DumpHeartbeatRequest(const HeartbeatRequest& request);

    /*
     * 输出心跳回应信息
     */
    void DumpHeartbeatResponse(const HeartbeatResponse& response);

    /*
     * 心跳线程
     */
    std::unique_ptr<std::thread>    hbThread_;

    /*
     * 心跳选项
     */
    HeartbeatOptions                options_;

    /*
     * 控制心跳模块运行或停止
     */
    std::atomic<bool>               toStop_;

    /*
     * 心跳模块的当前心跳周期
     */
    uint64_t                        term_;

    /*
     * 当前的复制组Leader角色数量
     */
    int32_t                         leaders_;

    /*
     * MDS的地址
     */
    butil::EndPoint                 mdsEp_;

    /*
     * ChunkServer本身的地址
     */
    butil::EndPoint                 csEp_;

    /*
     * ChunkServer数据目录
     */
    std::string                     dataDirPath_;

    /*
     * Copyset信息列表
     */
    std::map<GroupNid, CopysetInfoPtr>  copysets_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_HEARTBEAT_H_

