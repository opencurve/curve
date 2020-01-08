/*
 * Project: curve
 * Created Date: 2019-12-05
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef TEST_CHUNKSERVER_HEARTBEAT_TEST_COMMON_H_
#define TEST_CHUNKSERVER_HEARTBEAT_TEST_COMMON_H_

#include <braft/node_manager.h>
#include <braft/node.h>

#include <string>
#include <vector>
#include <atomic>
#include <thread>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "proto/heartbeat.pb.h"
#include "src/common/configuration.h"
#include "src/chunkserver/heartbeat.h"
#include "src/chunkserver/cli.h"
#include "src/chunkserver/uri_paser.h"
#include "test/client/fake/fakeMDS.h"

namespace curve {
namespace chunkserver {
class HeartbeatTestCommon {
 public:
    explicit HeartbeatTestCommon(const std::string &filename) {
        hbtestCommon_ = this;
        handlerReady_.store(false, std::memory_order_release);

        mds_ = new FakeMDS(filename);
        mds_->SetChunkServerHeartbeatCallback(HeartbeatCallback);
        mds_->Initialize();
        mds_->StartService();
    }

    std::atomic<bool>& GetReady() {
        return handlerReady_;
    }

    std::mutex& GetMutex() {
        return hbMtx_;
    }

    std::condition_variable& GetCV() {
        return hbCV_;
    }

    void UnInitializeMds() {
        mds_->UnInitialize();
        delete mds_;
    }

    /**
     * CleanPeer 清空peer上指定copyset数据
     *
     * @param[in] poolId 逻辑池id
     * @param[in] copysetId copyset id
     * @param[in] peer chunkserver ip
     */
    void CleanPeer(
        LogicPoolID poolId, CopysetID copysetId, const std::string& peer);

    /**
     * CreateCopysetPeers 在指定chunkserverlist上创建指定配置的copyset
     *
     * @param[in] poolId 逻辑池id
     * @param[in] copysetId copyset id
     * @param[in] cslist 待创建copyset的chunkserver列表
     * @param[in] conf 使用该配置作为初始配置创建copyset
     */
    void CreateCopysetPeers(LogicPoolID poolId, CopysetID copysetId,
        const std::vector<std::string> &cslist, const std::string& conf);

    /**
     * WaitCopysetReady 等待指定copyset选出leader
     *
     * @param[in] poolId 逻辑池id
     * @param[in] copysetId copyset id
     * @param[in] conf 指定copyset复制组成员
     */
    void WaitCopysetReady(
        LogicPoolID poolId, CopysetID copysetId, const std::string& conf);

    /**
     * TransferLeaderSync 触发transferleader并等待完成
     *
     * @param[in] poolId 逻辑池id
     * @param[in] copysetId copyset id
     * @param[in] conf 指定copyset复制组成员
     * @param[in] newLeader 目标leader
     */
    void TransferLeaderSync(LogicPoolID poolId, CopysetID copysetId,
        const std::string& conf, const std::string& newLeader);

    /**
     * WailForConfigChangeOk 指定时间内(timeLimitMs),chunkserver是否上报了
     *                       符合预期的copyset信息
     *
     * @param[in] conf mds需要下发给指定copyset的变更命令
     * @param[in] expectedInfo 变更之后期望复制组配置
     * @param[in] timeLimitMs 等待时间
     *
     * @return false-指定时间内copyset配置未能达到预期， true-达到预期
     */
    bool WailForConfigChangeOk(
        const ::curve::mds::heartbeat::CopySetConf &conf,
        ::curve::mds::heartbeat::CopySetInfo expectedInfo,
        int timeLimitMs);

    /**
     * SameCopySetInfo 比较两个copysetInfo是否一致
     *
     * @param[in] orig 待比较的copysetInfo
     * @param[in] expect 期望copysetInfo
     *
     * @return true-一致 false-不一致
     */
    bool SameCopySetInfo(
        const ::curve::mds::heartbeat::CopySetInfo &orig,
        const ::curve::mds::heartbeat::CopySetInfo &expect);

    /**
     * ReleaseHeartbeat heartbeat中的会掉设置为nullptr
     */
    void ReleaseHeartbeat();

    /**
     * SetHeartbeatInfo 把mds接受到的cntl等信息复制到成员变量
     */
    void SetHeartbeatInfo(
        ::google::protobuf::RpcController* cntl,
        const HeartbeatRequest* request,
        HeartbeatResponse* response,
        ::google::protobuf::Closure* done);

    /**
     * GetHeartbeat 把当前成员中的cntl等变量设置到rpc中
     */
    void GetHeartbeat(
        ::google::protobuf::RpcController** cntl,
        const HeartbeatRequest** request,
        HeartbeatResponse** response,
        ::google::protobuf::Closure** done);

    /**
     * HeartbeatCallback heartbeat回掉
     */
    static void HeartbeatCallback(
        ::google::protobuf::RpcController* controller,
        const HeartbeatRequest* request,
        HeartbeatResponse* response,
        ::google::protobuf::Closure* done);

 private:
    FakeMDS* mds_;

    mutable std::mutex hbMtx_;
    std::condition_variable hbCV_;
    std::atomic<bool> handlerReady_;

    ::google::protobuf::RpcController* cntl_;
    const HeartbeatRequest* req_;
    HeartbeatResponse* resp_;
    ::google::protobuf::Closure* done_;

    static HeartbeatTestCommon* hbtestCommon_;
};

int RemovePeersData(bool rmChunkServerMeta = false);

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_HEARTBEAT_TEST_COMMON_H_
