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
 * Created Date: 18-8-23
 * Author: wudemiao
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
#include "src/common/concurrent/task_thread_pool.h"

namespace curve {
namespace chunkserver {

using curve::common::RWLock;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::TaskThreadPool;

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
     * @brief 加载目录下的所有copyset
     *
     * @return 0表示加载成功，非0表示加载失败
     */
    int ReloadCopysets();

    /**
     * 创建copyset node，两种情况需要创建copyset node
     * TODO(wudemiao): 后期替换之后删除掉
     *  1.集群初始化，创建copyset
     *  2.恢复的时候add peer
     */
    bool CreateCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const Configuration &conf);

    /**
     * 都是创建copyset，目前两个同时存在，后期仅仅保留一个
     */
    bool CreateCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const std::vector<Peer> peers);

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
     * @brief Delete broken copyset
     * @param[in] poolId logical pool id
     * @param[in] copysetId copyset id
     * @return true if delete success, else return false
     */ 
    bool DeleteBrokenCopyset(const LogicPoolID& poolId,
                             const CopysetID& copysetId);

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
    virtual CopysetNodePtr GetCopysetNode(const LogicPoolID &logicPoolId,
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

    virtual const CopysetNodeOptions &GetCopysetNodeOptions() const {
        return copysetNodeOptions_;
    }

    /**
     * @brief: Only for test
     */
    void SetCopysetNodeOptions(
        const CopysetNodeOptions& copysetNodeOptions) {
        copysetNodeOptions_ = copysetNodeOptions;
    }

    /**
     * 加载copyset，包括新建一个copyset或者重启一个copyset
     * @param logicPoolId: 逻辑池id
     * @param copysetId: copyset id
     * @param needCheckLoadFinished: 是否需要判断copyset加载完成
     */
    void LoadCopyset(const LogicPoolID &logicPoolId,
                     const CopysetID &copysetId,
                     bool needCheckLoadFinished);
    /**
     * 检测指定的copyset状态，直到copyset加载完成或出现异常
     * @param node: 指定的copyset node
     * @return true表示加载成功，false表示检测过程中出现异常
     */
    bool CheckCopysetUntilLoadFinished(std::shared_ptr<CopysetNode> node);

    /**
     * 获取copysetNodeManager加载copyset的状态
     * @return false-copyset未加载完成 true-copyset已加载完成
     */
    virtual bool LoadFinished();

 protected:
    CopysetNodeManager()
        : copysetLoader_(nullptr)
        , running_(false)
        , loadFinished_(false) {}

 private:
    /**
     * 如果指定copyset不存在，则将copyset插入到map当中（线程安全）
     * @param logicPoolId:逻辑池id
     * @param copysetId:复制组id
     * @param node:要插入的copysetnode
     * @return copyset不存在，则插入到map并返回true；
     *         copyset如果存在，则返回false
     */
    bool InsertCopysetNodeIfNotExist(const LogicPoolID &logicPoolId,
                                     const CopysetID &copysetId,
                                     std::shared_ptr<CopysetNode> node);

    /**
     * 创建一个新的copyset或加载一个已存在的copyset（非线程安全）
     * @param logicPoolId:逻辑池id
     * @param copysetId:复制组id
     * @param conf:此copyset的配置成员
     * @return 创建或加载成功返回copysetnode，否则返回nullptr
     */
    std::shared_ptr<CopysetNode> CreateCopysetNodeUnlocked(
        const LogicPoolID &logicPoolId,
        const CopysetID &copysetId,
        const Configuration &conf);

 private:
    using CopysetNodeMap = std::unordered_map<GroupId,
                                              std::shared_ptr<CopysetNode>>;
    // 保护复制组 map的读写锁
    mutable RWLock rwLock_;
    // 复制组map
    CopysetNodeMap copysetNodeMap_;
    // 复制组配置选项
    CopysetNodeOptions copysetNodeOptions_;
    // 控制copyset并发启动的数量
    std::shared_ptr<TaskThreadPool<>> copysetLoader_;
    // 表示copyset node manager当前是否正在运行
    Atomic<bool> running_;
    // 表示copyset node manager当前是否已经完成加载
    Atomic<bool> loadFinished_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_COPYSET_NODE_MANAGER_H_
