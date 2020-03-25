/*
 * Project: curve
 * Created Date: 19-08-15
 * Author: lixiaocui
 * Copyright (c) 2019 netease
 */

#ifndef TEST_INTEGRATION_CLUSTER_COMMON_CLUSTER_H_
#define TEST_INTEGRATION_CLUSTER_COMMON_CLUSTER_H_

#include <string>
#include <map>
#include <vector>
#include <memory>
#include "src/mds/dao/mdsRepo.h"
#include "src/client/mds_client.h"
#include "src/client/config_info.h"
#include "test/util/config_generator.h"

using ::curve::mds::MdsRepo;
using ::curve::client::MDSClient;

namespace curve {
class CurveCluster {
 public:
    /**
     * CurveCluster 构造函数
     *
     * @param[in] netWorkSegment 网桥的网络地址，默认为"192.168.200."
     * @param[in] nsPrefix 网络命名空间的前缀，默认为"integ_"
     */
    CurveCluster(const std::string &netWorkSegment = "192.168.200.",
        const std::string &nsPrefix = "integ_") :
        networkSegment_(netWorkSegment), nsPrefix_(nsPrefix) {}

    /**
     * InitDB 初始化一个MdsRepo， 方便对数据库进行操作
     *
     * @param[in] mdsTable 数据库名称,建议按照模块来命名，避免冲突
     * @param[in] user 测试环境一般设置为"root"
     * @param[in] url 测试环境一般设置为"localhost"
     * @param[in] password 测试环境一般设置为"qwer"
     * @param[in] poolSize 测试环境一般设置为16
     */
    void InitDB(const std::string &mdsTable, const std::string &user = "root",
        const std::string &url = "localhost",
        const std::string &password = "qwer", int poolSize = 16);

    /**
     * InitMdsClient 初始化mdsclient， 用于和mds交互
     *
     * @param op 参数设置
     */
    void InitMdsClient(const MetaServerOption_t& op);

    /**
     * BuildNetWork 如果需要是用不同的ip来起chunkserver, 需要在测试用例的SetUp中先
     *              调用该函数
     */
    void BuildNetWork();

    /**
     * StopCluster 停止该集群中所有的进程
     */
    void StopCluster();

    /**
     * @brief 生成各模块配置文件
     *
     * @tparam T 任一ConfigGenerator
     * @param configPath 配置文件路径
     * @param options 修改的配置项
     */
    template <class T>
    void PrepareConfig(
        const std::string &configPath,
        const std::vector<std::string> &options) {
        T gentor(configPath);
        gentor.SetConfigOptions(options);
        gentor.Generate();
    }

    /**
     * StartSingleMDS 启动一个mds
     *                如果需要不同ip的chunkserver,ipPort请设置为192.168.200.1:XXXX
     *
     * @param[in] id mdsId
     * @param[in] ipPort 指定mds的ipPort
     * @param expectAssert是否期望使用assert判断
     * @param[in] mdsConf mds启动参数项, 示例：
     *   const std::vector<std::string> mdsConf{
            {" --graceful_quit_on_sigterm"},
            {" --confPath=./test/integration/cluster_common/mds.basic.conf"},
        };
     * @pram[in] expectLeader 是否希望成为leader
     */
    void StartSingleMDS(int id, const std::string &ipPort,
                        int dummyPort,
                        const std::vector<std::string> &mdsConf,
                        bool expectLeader, bool expectAssert = true,
                        bool* startsuccess = nullptr);

    /**
     * StopMDS 停止指定id的mds
     */
    void StopMDS(int id);

    /**
     * StopAllMDS 停止所有mds
     */
    void StopAllMDS();

    /**
     * StarrSingleEtcd 启动一个etcd节点
     *
     * @param clientIpPort
     * @param peerIpPort
     * @param etcdConf etcd启动项参数, 建议按照模块指定name,防止并发运行时冲突
     *      std::vector<std::string>{" --name basic_test_start_stop_module1"}
     */
    void StartSingleEtcd(int id, const std::string &clientIpPort,
        const std::string &peerIpPort,
        const std::vector<std::string> &etcdConf);

    /**
     * WaitForEtcdClusterAvalible 在一定时间内等待etcd集群leader选举成功，处于可用状态
     */
    bool WaitForEtcdClusterAvalible(int waitSec = 20);

    /**
     * StopEtcd 停止指定id的etcd节点
     */
    void StopEtcd(int id);

    /**
     * StopAllEtcd 停止所有etcd节点
     */
    void StopAllEtcd();

    /**
     * StartSingleChunkServer 启动一个chunkserver节点
     *
     * @param[in] id
     * @param[in] ipPort
     * @param[in] chunkserverConf chunkserver启动项，示例:
     *  const std::vector<std::string> chunkserverConf1{
            {" --graceful_quit_on_sigterm"},
            {" -chunkServerStoreUri=local://./basic1/"},
            {" -chunkServerMetaUri=local://./basic1/chunkserver.dat"},
            {" -copySetUri=local://./basic1/copysets"},
            {" -recycleUri=local://./basic1/recycler"},
            {" -chunkFilePoolDir=./basic1/chunkfilepool/"},
            {" -chunkFilePoolMetaPath=./basic1/chunkfilepool.meta"},
            {" -conf=./test/integration/cluster_common/chunkserver.basic.conf"},
            {" -raft_sync_segments=true"},
        };
        建议文件名也按模块的缩写来，文件名不能太长，否则注册到数据库会失败
     */
    void StartSingleChunkServer(int id, const std::string &ipPort,
        const std::vector<std::string> &chunkserverConf);

    /**
     * StartSingleChunkServer 在网络命名空间内启动一个指定id的chunkserver
     *                        无需指定ipPort
     *
     * @param id
     * @param chunkserverConf, 同StartSingleChunkServer的示例
     */
    void StartSingleChunkServerInBackground(
        int id, const std::vector<std::string> &chunkserverConf);

    /**
     * StopChunkServer 停掉指定id的chunkserver进程
     */
    void StopChunkServer(int id);

    /**
     * StopAllChunkServer 停止所有chunkserver
     */
    void StopAllChunkServer();

    /**
     * PreparePhysicalPool 创建物理池
     *
     * @param[in] id 给指定id的mds发送命令
     * @param[in] clusterMap 拓扑信息，示例：
     * ./test/integration/cluster_common/cluster_common_topo_1.txt (不同ip)
     * ./test/integration/cluster_common/cluster_common_topo_2.txt
     *  (相同ip, 一定要加上port加以区分,
     *      chunkserver也必须和clusterMap中server的ipPort相同)
     */
    void PreparePhysicalPool(int mdsId, const std::string &clusterMap);

    void PrepareLogicalPool(int mdsId, const std::string &clusterMap,
        int copysetNum, const std::string &physicalPoolName);

    /**
     * MDSIpPort 获取指定id的mds地址
     */
    std::string MDSIpPort(int id);

    /**
     * EtcdClientIpPort 获取指定id的etcd client地址
     */
    std::string EtcdClientIpPort(int id);

    /**
     * EtcdPeersIpPort 获取指定id的etcd peers地址
     */
    std::string EtcdPeersIpPort(int id);

    /**
     * ChunkServerIpPort 获取指定id的chunkserver地址
     */
    std::string ChunkServerIpPort(int id);

    /**
     * HangMDS hang住指定mds进程
     */
    void HangMDS(int id);

    /**
     * RecoverHangMDS 恢复hang住的mds进程
     */
    void RecoverHangMDS(int id, bool expectOK = true);

    /**
     * HangEtcd hang住指定etcd进程
     */
    void HangEtcd(int id);

    /**
     * RecoverHangEtcd 恢复hang住的mds进程
     */
    void RecoverHangEtcd(int id);

    /**
     * HangChunkServer hang住指定chunkserver进程
     */
    void HangChunkServer(int id);

    /**
     * RecoverHangChunkServer 恢复hang住的chunkserver进程
     */
    void RecoverHangChunkServer(int id);

    /**
     * CurrentServiceMDS 获取当前正在提供服务的mds
     *
     * @param[out] curId 当前正在服务的mds编号
     *
     * @return true表示有正在服务的mds, false表示没有正在服务的mds
     */
    bool CurrentServiceMDS(int *curId);

    /**
     * CreateFile 在curve中创建文件
     *
     * @param[in] success 是否希望create成功
     * @param[in] user 用户
     * @param[in] pwd 密码
     * @param[in] fileName 文件名
     * @param[in] fileSize 文件大小
     * @param[in] normalFile 是否为normal file
     */
    void CreateFile(bool success, const std::string &user,
        const std::string &pwd, const std::string &fileName,
        uint64_t fileSize = 0, bool normalFile = true);

 private:
    /**
     * ProbePort 探测指定ipPort是否处于监听状态
     *
     * @param[in] ipPort 指定的ipPort值
     * @param[in] timeoutMs 探测的超时时间，单位是ms
     * @param[in] expectOpen 是否希望是监听状态
     *
     * @return 0表示指定时间内的探测符合预期. -1表示指定时间内的探测不符合预期
     */
    int ProbePort(
        const std::string &ipPort, int64_t timeoutMs, bool expectOpen);

    /**
     * ChunkServerIpPortInBackground
     *      在需要不同ip的chunkserver的情况下，用于生成chunkserver ipPort
     */
    std::string ChunkServerIpPortInBackground(int id);

    /**
     * HangProcess hang住一个进程
     *
     * @param pid 进程id
     */
    void HangProcess(pid_t pid);

    /**
     * RecoverHangProcess 恢复hang住的进程
     *
     * @param pid 进程id
     * @param expected是否期望使用ASSERT判断
     */
    void RecoverHangProcess(pid_t pid, bool expected);

 private:
    // 网络号
    std::string networkSegment_;

    // 网络命名空间前缀
    std::string nsPrefix_;

    // mds的id对应的进程号
    std::map<int, pid_t> mdsPidMap_;

    // mds的id对应的ipport
    std::map<int, std::string> mdsIpPort_;

    // etcd的id对应的进程号
    std::map<int, pid_t> etcdPidMap_;

    // etcd的id对应的client ipport
    std::map<int, std::string> etcdClientIpPort_;

    // etcd的id对应的peer ipport
    std::map<int, std::string> etcdPeersIpPort_;

    // chunkserver的id对应的进程号
    std::map<int, pid_t> chunkserverPidMap_;

    // chunkserver的id对应的ipport
    std::map<int, std::string> chunkserverIpPort_;

    // mdsClient
    std::shared_ptr<MDSClient> mdsClient_;

 public:
    // mysql数据库
    MdsRepo *mdsRepo_;
};
}  // namespace curve

#endif  // TEST_INTEGRATION_CLUSTER_COMMON_CLUSTER_H_

