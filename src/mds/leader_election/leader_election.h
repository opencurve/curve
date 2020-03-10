/*
 * Project: curve
 * Created Date: Tue June 25th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_LEADER_ELECTION_LEADER_ELECTION_H_
#define SRC_MDS_LEADER_ELECTION_LEADER_ELECTION_H_

#include <fiu.h>
#include <memory>
#include <string>

#include "src/mds/kvstorageclient/etcd_client.h"

namespace curve {
namespace mds {
struct LeaderElectionOptions {
    // etcd客户端
    std::shared_ptr<EtcdClientImp> etcdCli;

    // 带ttl的session，ttl超时时间内
    uint32_t sessionInterSec;

    // 竞选leader的超时时间
    uint32_t electionTimeoutMs;

    // leader名称，建议使用ip+port以示区分
    std::string leaderUniqueName;

    // 需要竞选的key
    std::string campaginPrefix;
};

class LeaderElection {
 public:
    explicit LeaderElection(LeaderElectionOptions opt) {
        opt_ = opt;
    }

    /**
     * @brief CampaginLeader 竞选leader
     *
     * @return 0表示竞选成功 -1表示竞选失败
     */
    int CampaginLeader();

    /**
     * @brief StartObserverLeader 启动leader节点监测线程
     */
    void StartObserverLeader();

    /**
     * @brief LeaderResign leader主动卸任leader，卸任成功后其他节点可以竞选leader
     */
    int LeaderResign();

    /**
     * @brief 返回leader name
     */
    const std::string& GetLeaderName() {
        return opt_.leaderUniqueName;
    }

 public:
    /**
     * @brief ObserveLeader 监测在etcd中创建的leader节点，正常情况下一直block，
     *        退出表示leader change或者从client端角度看etcd异常，进程退出
     */
    int ObserveLeader();

 private:
    // option
    LeaderElectionOptions opt_;

    // 竞选leader之后记录在objectManager中的id号
    uint64_t leaderOid_;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_LEADER_ELECTION_LEADER_ELECTION_H_

