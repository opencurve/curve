/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_COPYSET_NODE_H_
#define SRC_CHUNKSERVER_COPYSET_NODE_H_

#include <butil/memory/ref_counted.h>

#include <string>
#include <vector>
#include <climits>
#include <memory>

#include "src/chunkserver/concurrent_apply.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/conf_epoch_file.h"
#include "src/chunkserver/config_info.h"
#include "src/chunkserver/chunkserver_metrics.h"
#include "proto/heartbeat.pb.h"
#include "proto/chunk.pb.h"
#include "proto/common.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::curve::mds::heartbeat::ConfigChangeType;
using ::curve::common::Peer;

class CopysetNodeManager;

extern const char *kCurveConfEpochFilename;

const char RAFT_DATA_DIR[] = "data";
const char RAFT_META_DIR[] = "raft_meta";
const char RAFT_SNAP_DIR[] = "raft_snapshot";
const char RAFT_LOG_DIR[]  = "log";

/**
 * 一个Copyset Node就是一个复制组的副本
 */
class CopysetNode : public braft::StateMachine,
                    public std::enable_shared_from_this<CopysetNode> {
 public:
    // for ut mock
    CopysetNode() = default;

    CopysetNode(const LogicPoolID &logicPoolId,
                const CopysetID &copysetId,
                const Configuration &initConf);

    virtual ~CopysetNode();

    /**
     * 初始化copyset node配置
     * @param options
     * @return 0，成功，-1失败
     */
    virtual int Init(const CopysetNodeOptions &options);

    /**
     * Raft Node init，使得Raft Node运行起来
     * @return
     */
    virtual int Run();

    /**
     * 关闭copyset node
     */
    virtual void Fini();

    /**
     * 返回复制组的逻辑池ID
     * @return
     */
    LogicPoolID GetLogicPoolId() const;

    /**
     * 返回复制组的复制组ID
     * @return
     */
    CopysetID GetCopysetId() const;

    /**
     * 返回复制组数据目录
     * @return
     */
    std::string GetCopysetDir() const;

    /**
     * 返回当前副本是否在leader任期
     * @return
     */
    virtual bool IsLeaderTerm() const;

    /**
     * 返回当前的任期
     * @return 当前的任期
     */
    virtual uint64_t LeaderTerm() const;

    /**
     * 返回leader id
     * @return
     */
    virtual PeerId GetLeaderId() const;

    /**
     * 返回是否是复制组Leader
     * @return
     */
    virtual bool IsLeader() const;

    /**
     * @brief 切换复制组的Leader
     * @param[in] peerId 目标Leader的成员ID
     * @return 心跳任务的引用
     */
    butil::Status TransferLeader(const Peer& peer);

    /**
     * @brief 复制组添加新成员
     * @param[in] peerId 新成员的ID
     * @return 心跳任务的引用
     */
    butil::Status AddPeer(const Peer& peer);

    /**
     * @brief 复制组删除成员
     * @param[in] peerId 将要删除成员的ID
     * @return 心跳任务的引用
     */
    butil::Status RemovePeer(const Peer& peer);

    /**
     * 返回copyset的配置版本
     * @return
     */
    virtual uint64_t GetConfEpoch() const;

    /**
     * 更新applied index，只有比它大的才更新
     * @param index
     */
    virtual void UpdateAppliedIndex(uint64_t index);

    /**
     * 返回当前最新的applied index
     * @return
     */
    virtual uint64_t GetAppliedIndex() const;

    /**
     * @brief: 查询配置变更的状态，TODO(wudemiao): 后面修改braft再补充单测
     * @param type[out]: 配置变更类型
     * @param oldConf[out]: 老的配置
     * @param alterPeer[out]: 变更的peer
     * @return 0查询成功，-1查询异常失败
     */
    virtual int GetConfChange(ConfigChangeType *type,
                              Configuration *oldConf,
                              Peer *alterPeer);

    /**
     * @brief: 获取copyset node的状态值，用于比较多个副本的数据一致性
     * @param hash[out]: copyset node状态值
     * @return 0成功，-1失败
     */
    virtual int GetHash(std::string *hash);

    /**
     * @brief: 获取copyset node的status，实际调用的raft node的get_status接口
     * @param status[out]: copyset node status
     */
    virtual void GetStatus(NodeStatus *status);

    /**
     * 返回data store指针
     * @return
     */
    virtual std::shared_ptr<CSDataStore> GetDataStore() const;

    /**
     * 返回ConcurrentApplyModule
     */
    virtual ConcurrentApplyModule* GetConcurrentApplyModule() const;

    /**
     * 向copyset node propose一个op request
     * @param task
     */
    virtual void Propose(const braft::Task &task);

    /**
     * 获取复制组成员
     * @param peers:返回的成员列表(输出参数)
     * @return
     */
    void ListPeers(std::vector<Peer>* peers);

    /**
     * 下面的接口都是继承StateMachine实现的接口
     */
 public:
    /**
     * op log apply的时候回调函数
     * @param iter:可以batch的访问已经commit的log entries
     */
    void on_apply(::braft::Iterator &iter) override;

    /**
     * 复制关闭的时候调用此回调
     */
    void on_shutdown() override;

    /**
     * raft snapshot相关的接口,仅仅保存raft snapshot meta
     * 和snapshot文件的list，这里并没有拷贝实际的数据，因为
     * 在块存储场景所有操作是幂等，所以，并不真实的拷贝数据
     */
    void on_snapshot_save(::braft::SnapshotWriter *writer,
                          ::braft::Closure *done) override;

    /**
     *  load日志有两种情况：
     *  1. Follower节点Install snapshot追赶leader，这个时候
     *  snapshot目录下面有chunk数据和snapshot数据
     *  2. 节点重启，会执行snapshot load，然后回放日志，这个时
     *  候snapshot目录下面没有数据，什么都不用做
     *  TODO(wudemiao): install snapshot的时候会存在空间
     *  double的可能性，考虑如下场景，follower落后，然后通过从
     *  leader install snapshot恢复数据，其首先会从leader将
     *  所有数据下载过来，然后在调用snapshot load加载快照，这个
     *  期间空间占用了就double了；后期需要通过控制单盘参与install
     *  snapshot的数量
     */
    int on_snapshot_load(::braft::SnapshotReader *reader) override;

    /**
     * new leader在apply noop之后会调用此接口，表示此 leader可
     * 以提供read/write服务了。
     * @param term:当前leader任期
     */
    void on_leader_start(int64_t term) override;

    /**
     * leader step down的时候调用
     * @param status:复制组的状态
     */
    void on_leader_stop(const butil::Status &status) override;

    /**
     * 复制组发生错误的时候调用
     * @param e:具体的 error
     */
    void on_error(const ::braft::Error &e) override;

    /**
     * 配置变更日志entry apply的时候会调用此函数，目前会利用此接口
     * 更新配置epoch值
     * @param conf:当前复制组最新的配置
     */
    void on_configuration_committed(const ::braft::Configuration &conf) override;   //NOLINT

    /**
     * 当follower停止following主的时候调用
     * @param ctx:可以获取stop following的原因
     */
    void on_stop_following(const ::braft::LeaderChangeContext &ctx) override;

    /**
     * Follower或者Candidate发现新的leader后调用
     * @param ctx:leader变更上下，可以获取new leader和start following的原因
     */
    void on_start_following(const ::braft::LeaderChangeContext &ctx) override;

    /**
     * 用于测试注入mock依赖
     */
 public:
    void SetCSDateStore(std::shared_ptr<CSDataStore> datastore);

    void SetLocalFileSystem(std::shared_ptr<LocalFileSystem> fs);

    void SetConfEpochFile(std::unique_ptr<ConfEpochFile> epochFile);

    void SetCopysetNode(std::shared_ptr<Node> node);

    void SetSnapshotFileSystem(scoped_refptr<FileSystemAdaptor>* fs);

    /**
     * better for test
     */
 public:
    /**
     * 从文件中解析copyset配置版本信息
     * @param filePath:文件路径
     * @return 0: successs, -1 failed
     */
    int LoadConfEpoch(const std::string &filePath);

    /**
     * 保存copyset配置版本信息到文件中
     * @param filePath:文件路径
     * @return 0 成功，-1 failed
     */
    int SaveConfEpoch(const std::string &filePath);

 private:
    // 逻辑池 id
    LogicPoolID logicPoolId_;
    // 复制组 id
    CopysetID copysetId_;
    // 复制组的配置
    Configuration       conf_;
    // 复制组的配置操作锁
    mutable std::mutex  confLock_;
    // 复制组的配置版本
    std::atomic<uint64_t> epoch_;
    // 复制组副本的peer id
    PeerId peerId_;
    // braft Node的配置参数
    NodeOptions nodeOptions_;
    // CopysetNode对应的braft Node
    std::shared_ptr<Node> raftNode_;
    // chunk file的绝对目录
    std::string chunkDataApath_;
    // chunk file的相对目录
    std::string chunkDataRpath_;
    // copyset绝对路径
    std::string copysetDirPath_;
    // 文件系统适配器
    std::shared_ptr<LocalFileSystem> fs_;
    // Chunk持久化操作接口
    std::shared_ptr<CSDataStore> dataStore_;
    // 并发模块
    ConcurrentApplyModule *concurrentapply_;
    // 配置版本持久化工具接口
    std::unique_ptr<ConfEpochFile> epochFile_;
    // 复制组的apply index
    std::atomic<uint64_t> appliedIndex_;
    // 复制组当前任期，如果<=0表明不是leader
    std::atomic<int64_t> leaderTerm_;
    // 复制组数据回收站目录
    std::string recyclerUri_;
    // 复制组的metric信息
    CopysetMetricPtr metric_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_COPYSET_NODE_H_
