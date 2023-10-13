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
 * Created Date: 19-08-15
 * Author: lixiaocui
 */

#ifndef TEST_INTEGRATION_CLUSTER_COMMON_CLUSTER_H_
#define TEST_INTEGRATION_CLUSTER_COMMON_CLUSTER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "src/client/config_info.h"
#include "src/client/mds_client.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store_etcd.h"
#include "test/util/config_generator.h"

using ::curve::client::MDSClient;
using ::curve::snapshotcloneserver::SnapshotCloneMetaStoreEtcd;

namespace curve {

#define RETURN_IF_NOT_ZERO(x)                           \
    do {                                                \
        int ret = (x);                                  \
        if (ret != 0) {                                 \
            LOG(ERROR) << __FILE__ << ":" << __LINE__   \
                       << "-> get non-ZERO, return -1"; \
            return ret;                                 \
        }                                               \
    } while (0)

#define RETURN_IF_FALSE(x)                            \
    do {                                              \
        bool ret = (x);                               \
        if (!ret) {                                   \
            LOG(ERROR) << __FILE__ << ":" << __LINE__ \
                       << "-> get FALSE, return -1";  \
            return -1;                                \
        }                                             \
    } while (0)

class CurveCluster {
 public:
    /**
     * CurveCluster constructor
     *
     * @param[in] netWorkSegment The network address of the bridge, which
     * defaults to "192.168.200."
     * @param[in] nsPrefix The prefix of the network namespace, which defaults
     * to "integ_"
     */
    CurveCluster(const std::string& netWorkSegment = "192.168.200.",
                 const std::string& nsPrefix = "integ_")
        : networkSegment_(netWorkSegment), nsPrefix_(nsPrefix) {}

    /**
     * InitMdsClient initializes mdsclient for interaction with mds
     *
     * @param op parameter setting
     * @return 0. Success; Non 0. Failure
     */
    int InitMdsClient(const curve::client::MetaServerOption& op);

    /**
     * @brief Initialize metastore
     *
     * @param[in] etcdEndpoints etcd client's IP port
     *
     * @return returns an error code
     */
    int InitSnapshotCloneMetaStoreEtcd(const std::string& etcdEndpoints);

    /**
     * If BuildNet needs to use a different IP to start the chunkserver,
     * This function needs to be called first in the SetUp of the test case
     * @return 0. Success; Non 0. Failure
     */
    int BuildNetWork();

    /**
     * StopCluster stops all processes in the cluster
     * @return 0.Success; -1.Failure
     */
    int StopCluster();

    /**
     * @brief Generate configuration files for each module
     *
     * @tparam T any ConfigGenerator
     * @param configPath Configuration file path
     * @param options Configuration items modified
     */
    template <class T>
    void PrepareConfig(const std::string& configPath,
                       const std::vector<std::string>& options) {
        T gentor(configPath);
        gentor.SetConfigOptions(options);
        gentor.Generate();
    }

    /**
     * StartSingleMDS starts an mds
     * If need chunkservers with different IPs, please set the ipPort to
     * 192.168.200.1:XXXX
     *
     * @param[in] id mdsId
     * @param[in] ipPort specifies the ipPort of the mds
     * @param[in] mdsConf mds startup parameter item, example:
     *   const std::vector<std::string> mdsConf{
            {"--graceful_quit_on_sigterm"},
            {"--confPath=./test/integration/cluster_common/mds.basic.conf"},
        };
     * @param[in] expectLeader is the expected leader expected
     * @return success returns pid; Failure returns -1
     */
    int StartSingleMDS(int id, const std::string& ipPort, int dummyPort,
                       const std::vector<std::string>& mdsConf,
                       bool expectLeader);

    /**
     * StopMDS stops the specified id's mds
     * @return 0.Success; -1.Failure
     */
    int StopMDS(int id);

    /**
     * StopAllMDS stops all mds
     * @return 0.Success; -1.Failure
     */
    int StopAllMDS();

    /**
     * @brief Start a snapshotcloneserver
     *
     * @param id The ID of snapshotclone server
     * @param ipPort IP Port
     * @param snapshot clone Conf parameter item
     * @return success returns pid; Failure returns -1
     */
    int StartSnapshotCloneServer(
        int id, const std::string& ipPort,
        const std::vector<std::string>& snapshotcloneConf);

    /**
     * @brief Stop the snapshotcloneserver for the specified Id
     *
     * @param id The ID of the snapshotcloneserver
     * @param force Use kill -9 when it is true
     * @return returns 0 for success, -1 for failure
     */
    int StopSnapshotCloneServer(int id, bool force = false);

    /**
     * @brief: Restart the snapshotcloneserver with the specified Id
     *
     * @param id The ID of the snapshotcloneserver
     * @param force Use kill -9 when it is true
     * @return success returns pid; Failure returns -1
     */
    int RestartSnapshotCloneServer(int id, bool force = false);

    /**
     * @brief Stop all snapshotcloneserver
     * @return returns 0 for success, -1 for failure
     */
    int StopAllSnapshotCloneServer();

    /**
     * StartSingleEtcd starts an etcd node
     *
     * @param clientIpPort
     * @param peerIpPort
     * @param etcdConf etcd startup parameter, it is recommended to specify the
     * name according to the module to prevent concurrent runtime conflicts
     *      std::vector<std::string>{"--name basic_test_start_stop_module1"}
     * @return success returns pid; Failure returns -1
     */
    int StartSingleEtcd(int id, const std::string& clientIpPort,
                        const std::string& peerIpPort,
                        const std::vector<std::string>& etcdConf);

    /**
     * WaitForEtcdClusterAvalible
     * Wait for the ETCD cluster leader election to be successful and available
     * for a certain period of time
     */
    bool WaitForEtcdClusterAvalible(int waitSec = 20);

    /**
     * StopEtcd stops the etcd node with the specified id
     * @return 0.Success; -1.Failure
     */
    int StopEtcd(int id);

    /**
     * StopAllEtcd stops all etcd nodes
     * @return 0.Success; -1.Failure
     */
    int StopAllEtcd();

    /**
     * @brief Format FilePool
     *
     * @param filePooldir FilePool directory
     * @param filePoolmetapath FilePool metadata directory
     * @param filesystemPath file system directory
     * @param size FilePool size (GB)
     * @return returns 0 for success, -1 for failure
     */
    int FormatFilePool(const std::string& filePooldir,
                       const std::string& filePoolmetapath,
                       const std::string& filesystemPath, uint32_t size);

    /**
     * StartSingleChunkServer starts a chunkserver node
     *
     * @param[in] id
     * @param[in] ipPort
     * @param[in] chunkserverConf chunkserver startup item, example:
     *  const std::vector<std::string> chunkserverConf1{
            {"--graceful_quit_on_sigterm"},
            {"-chunkServerStoreUri=local://./basic1/"},
            {"-chunkServerMetaUri=local://./basic1/chunkserver.dat"},
            {"-copySetUri=local://./basic1/copysets"},
            {"-recycleUri=local://./basic1/recycler"},
            {"-chunkFilePoolDir=./basic1/chunkfilepool/"},
            {"-chunkFilePoolMetaPath=./basic1/chunkfilepool.meta"},
            {"-conf=./test/integration/cluster_common/chunkserver.basic.conf"},
            {"-raft_sync_segments=true"},
        };
        It is recommended to also use the abbreviation of the module for the
     file name. The file name should not be too long, otherwise registering to
     the database will fail
     * @return success returns pid; Failure returns -1
     */
    int StartSingleChunkServer(int id, const std::string& ipPort,
                               const std::vector<std::string>& chunkserverConf);

    /**
     * StartSingleChunkServer Starts a chunkserver with the specified id in the
     * network namespace No need to specify ipPort
     *
     * @param id
     * @param chunkserverConf, same as the example of StartSingleChunkServer
     * @return success returns pid; Failure returns -1
     */
    int StartSingleChunkServerInBackground(
        int id, const std::vector<std::string>& chunkserverConf);

    /**
     * StopChunkServer stops the chunkserver process with the specified id
     * @return 0.Success; -1.Failure
     */
    int StopChunkServer(int id);

    /**
     * StopAllChunkServer Stop all chunkserver
     * @return 0.Success; -1.Failure
     */
    int StopAllChunkServer();

    /**
     * PreparePhysicalPool Create Physical Pool
     *
     * @param[in] id Send command to the specified mds with id
     * @param[in] clusterMap topology information, example:
     * ./test/integration/cluster_common/cluster_common_topo_1.txt (different
     * IPs)
     * ./test/integration/cluster_common/cluster_common_topo_2.txt
     *  (The same IP address must be distinguished by adding a port,
     *      The chunkserver must also be the same as the ipPort of the server in
     * the clusterMap)
     * @return 0.Success; -1.Failure
     */
    int PreparePhysicalPool(int mdsId, const std::string& clusterMap);

    /**
     * @return 0.Success; -1.Failure
     */
    int PrepareLogicalPool(int mdsId, const std::string& clusterMap);

    /**
     * MDSIpPort retrieves the mds address of the specified id
     */
    std::string MDSIpPort(int id);

    /**
     * EtcdClientIpPort retrieves the etcd client address for the specified id
     */
    std::string EtcdClientIpPort(int id);

    /**
     * EtcdPeersIpPort retrieves the etcd Peers address of the specified id
     */
    std::string EtcdPeersIpPort(int id);

    /**
     * ChunkServerIpPort retrieves the chunkserver address for the specified id
     */
    std::string ChunkServerIpPort(int id);

    /**
     * HangMDS hang resides in the specified mds process
     * @return 0.Success; -1.Failure
     */
    int HangMDS(int id);

    /**
     * RecoverHangMDS restores the mds process where hang resides
     * @return 0.Success; -1.Failure
     */
    int RecoverHangMDS(int id);

    /**
     * HangEtcd hang lives in the specified etcd process
     * @return 0.Success; -1.Failure
     */
    int HangEtcd(int id);

    /**
     * RecoverHangEtcd recovers the mds process where hang resides
     * @return 0.Success; -1.Failure
     */
    int RecoverHangEtcd(int id);

    /**
     * HangChunkServer hang resides in the specified chunkserver process
     * @return 0.Success; -1.Failure
     */
    int HangChunkServer(int id);

    /**
     * RecoverHangChunkServer Restores the chunkserver process where hang
     * resides
     * @return 0.Success; -1.Failure
     */
    int RecoverHangChunkServer(int id);

    /**
     * CurrentServiceMDS obtains the mds that are currently providing services
     *
     * @param[out] curId is currently serving the mds number
     *
     * @return true indicates that there are serving mds, while false indicates
     * that there are no serving mds
     */
    bool CurrentServiceMDS(int* curId);

    /**
     * CreateFile creates a file in Curve.
     *
     * @param[in] user User
     * @param[in] pwd Password
     * @param[in] fileName File name
     * @param[in] fileSize File size
     * @param[in] normalFile Whether it is a normal file
     * @return 0. Success; -1. Failure
     */
    int CreateFile(const std::string& user, const std::string& pwd,
                   const std::string& fileName, uint64_t fileSize = 0,
                   bool normalFile = true, const std::string& poolset = "");

 private:
    /**
     * ProbePort checks if the specified ipPort is in a listening state.
     *
     * @param[in] ipPort The specified ipPort value.
     * @param[in] timeoutMs The timeout for probing in milliseconds.
     * @param[in] expectOpen Whether it is expected to be in a listening state.
     *
     * @return 0 indicates that the probing meets the expected condition within
     * the specified time. -1 indicates that the probing does not meet the
     * expected condition within the specified time.
     */
    int ProbePort(const std::string& ipPort, int64_t timeoutMs,
                  bool expectOpen);

    /**
     * ChunkServerIpPortInBackground
     *      Used to generate chunkserver ipPort when chunkservers with different
     * IPs are required
     */
    std::string ChunkServerIpPortInBackground(int id);

    /**
     * HangProcess hang
     *
     * @param pid process id
     * @return 0.Success; -1.Failure
     */
    int HangProcess(pid_t pid);

    /**
     * RecoverHangProcess
     *
     * @param pid process id
     * @return 0.Success; -1.Failure
     */
    int RecoverHangProcess(pid_t pid);

 private:
    // Network number
    std::string networkSegment_;

    // Network namespace prefix
    std::string nsPrefix_;

    // The process number corresponding to the ID of the mds
    std::map<int, pid_t> mdsPidMap_;

    // The ipport corresponding to the ID of the mds
    std::map<int, std::string> mdsIpPort_;

    // The pid corresponding to the snapshotcloneserver id
    std::map<int, pid_t> snapPidMap_;

    // The ipPort corresponding to the snapshotcloneserver ID
    std::map<int, std::string> snapIpPort_;

    // Conf corresponding to snapshotcloneserver id
    std::map<int, std::vector<std::string>> snapConf_;

    // The process number corresponding to the id of ETCD
    std::map<int, pid_t> etcdPidMap_;

    // The client ipport corresponding to the id of ETCD
    std::map<int, std::string> etcdClientIpPort_;

    // Peer ipport corresponding to the id of ETCD
    std::map<int, std::string> etcdPeersIpPort_;

    // The process number corresponding to the id of chunkserver
    std::map<int, pid_t> chunkserverPidMap_;

    // The IP port corresponding to the ID of the chunkserver
    std::map<int, std::string> chunkserverIpPort_;

    // mdsClient
    std::shared_ptr<MDSClient> mdsClient_;

 public:
    // SnapshotCloneMetaStore for filling data during testing
    std::shared_ptr<SnapshotCloneMetaStoreEtcd> metaStore_;
};
}  // namespace curve

#endif  // TEST_INTEGRATION_CLUSTER_COMMON_CLUSTER_H_
