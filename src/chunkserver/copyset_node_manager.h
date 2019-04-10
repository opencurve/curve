/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_COPYSET_NODE_MANAGER_H_
#define SRC_CHUNKSERVER_COPYSET_NODE_MANAGER_H_

#include <mutex>    //NOLINT
#include <vector>
#include <memory>
#include <unordered_map>

#include "src/chunkserver/copyset_node.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/uncopyable.h"

namespace curve {
namespace chunkserver {

using curve::common::RWLock;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

class ChunkOpRequest;

/**
 * Copyset Node的管理者
 */
class CopysetNodeManager : public curve::common::Uncopyable {
 public:
    using CopysetNodePtr = std::shared_ptr<CopysetNode>;

    // 单例，仅仅在 c++11或者更高版本下正确
    static CopysetNodeManager &GetInstance() {
        static CopysetNodeManager instance;
        return instance;
    }

    int Init(const CopysetNodeOptions &copysetNodeOptions);
    int Run();
    int Fini();

    /**
     * 创建copyset node，两种情况需要创建copyset node
     *  1.集群初始化，创建copyset
     *  2.恢复的时候add peer
     */
    bool CreateCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const Configuration &conf);

    /**
     * 删除copyset node内存实例(停止copyset, 销毁copyset内存实例并从copyset
     * manager的copyset表中清除copyset表项,并不影响盘上的copyset持久化数据)
     * @param logicPoolId:逻辑池id
     * @param copysetId:复制组id
     * @return true 成功，false失败
     */
    bool DeleteCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId);

    /**
     * 彻底删除copyset node内存数据(停止copyset, 销毁copyset内存实例并从
     * copyset manager的copyset表中清除copyset表项,并将copyset持久化数据从盘
     * 上彻底删除)
     * @param logicPoolId:逻辑池id
     * @param copysetId:复制组id
     * @return true 成功，false失败
     */
    bool PurgeCopysetNodeData(const LogicPoolID &logicPoolId,
                              const CopysetID &copysetId);

    /**
     * 判断指定的copyset是否存在
     * @param logicPoolId:逻辑池子id
     * @param copysetId:复制组id
     * @return true存在，false不存在
     */
    bool IsExist(const LogicPoolID &logicPoolId, const CopysetID &copysetId);

    /**
     * 获取指定的copyset
     * @param logicPoolId:逻辑池子id
     * @param copysetId:复制组id
     * @return nullptr则为没查询到
     */
    CopysetNodePtr GetCopysetNode(const LogicPoolID &logicPoolId,
                                  const CopysetID &copysetId) const;

    /**
     * 查询所有的copysets
     * @param nodes:出参，返回所有的copyset
     */
    void GetAllCopysetNodes(std::vector<CopysetNodePtr> *nodes) const;

    /**
     * 添加RPC service
     * TODO(wudemiao): 目前仅仅用于测试，后期完善了会删除掉
     * @param server:rpc Server
     * @param listenAddress:监听的地址
     * @return 0成功，-1失败
     */
    int AddService(brpc::Server *server,
                   const butil::EndPoint &listenAddress);

    const CopysetNodeOptions &GetCopysetNodeOptions() const {
        return copysetNodeOptions_;
    }

 private:
    using CopysetNodeMap = std::unordered_map<GroupId,
                                              std::shared_ptr<CopysetNode>>;
    // 保护复制组 map的读写锁
    mutable RWLock rwLock_;
    // 复制组map
    CopysetNodeMap copysetNodeMap_;
    // 复制组配置选项
    CopysetNodeOptions copysetNodeOptions_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_COPYSET_NODE_MANAGER_H_
