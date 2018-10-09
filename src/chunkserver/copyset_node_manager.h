/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_COPYSET_NODE_MANAGER_H
#define CURVE_CHUNKSERVER_COPYSET_NODE_MANAGER_H

#include <mutex>    //NOLINT
#include <vector>
#include <memory>
#include <unordered_map>

#include "src/chunkserver/copyset_node.h"
#include "src/common/uncopyable.h"

namespace curve {
namespace chunkserver {

class OpRequest;

// copyset 管理接口
class CopysetNodeManager : public curve::common::Uncopyable {
 public:
    CopysetNodeManager() {}
    ~CopysetNodeManager() {}

    // 单例，仅仅在 c++11 下正确
    static CopysetNodeManager &GetInstance() {
        static CopysetNodeManager instance;
        return instance;
    }

    int Init(const CopysetNodeOptions &copysetNodeOptions);

    int Run();

    int Fini();

    // 创建 copyset node，两种情况需要创建 copyset node
    //  1. 集群初始化，创建 copyset
    //  2. add peer
    bool CreateCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const Configuration &conf);  // 有 RPC service
    // 仅有接口，没有 RPC service
    bool DeleteCopysetNode(const LogicPoolID &logicPoolId, const CopysetID &copysetId);

    std::shared_ptr<CopysetNode> GetCopysetNode(const LogicPoolID &logicPoolId,
                                                const CopysetID &copysetId) const;

    void GetAllCopysetNodes(std::vector<std::shared_ptr<CopysetNode>> *nodes) const;

    void ScheduleRequest(std::shared_ptr<OpRequest> request);

    bool IsExist(const LogicPoolID &logicPoolId,
                 const CopysetID &copysetId);

    // 添加 RPC service
    int AddService(brpc::Server *server,
                          const butil::EndPoint &listenAddress);

    const CopysetNodeOptions &GetCopysetNodeOptions() const {
        return copysetNodeOptions_;
    }

 private:
    using CopysetNodeMap = std::unordered_map<GroupId, std::shared_ptr<CopysetNode>>;

    // FixMe: 互斥锁仅用于快速实现，后期替换成 read/write lock，
    // 当前运行在 bthread 空间，应该改用 btread 提供的 lock，避免阻塞整个线程
    mutable std::mutex rwLock_;  // 读写锁
    CopysetNodeMap copysetNodeMap_;

    CopysetNodeOptions copysetNodeOptions_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_COPYSET_NODE_MANAGER_H
