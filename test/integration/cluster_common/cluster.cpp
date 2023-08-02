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

#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <errno.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <string>
#include <thread>  //NOLINT
#include <chrono>  //NOLINT
#include <memory>
#include "test/integration/cluster_common/cluster.h"
#include "src/common/string_util.h"
#include "src/common/timeutility.h"
#include "src/client/client_common.h"
#include "src/kvstorageclient/etcd_client.h"

using ::curve::client::UserInfo_t;
using ::curve::kvstorage::EtcdClientImp;
using ::curve::snapshotcloneserver::SnapshotCloneCodec;

namespace curve {

using ::curve::client::CreateFileContext;

int CurveCluster::InitMdsClient(const curve::client::MetaServerOption &op) {
    mdsClient_ = std::make_shared<MDSClient>();
    return mdsClient_->Initialize(op);
}

int CurveCluster::InitSnapshotCloneMetaStoreEtcd(
    const std::string &etcdEndpoints) {
    EtcdConf conf;
    conf.Endpoints = new char[etcdEndpoints.size()];
    std::memcpy(conf.Endpoints, etcdEndpoints.c_str(), etcdEndpoints.size());
    conf.len = etcdEndpoints.size();
    conf.DialTimeout = 5000;
    int timeout = 5000;
    int retryTimes = 3;
    auto etcdClient = std::make_shared<EtcdClientImp>();
    auto res = etcdClient->Init(conf, timeout, retryTimes);
    if (res != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "init etcd client err! ";
        return -1;
    }
    auto codec = std::make_shared<SnapshotCloneCodec>();

    metaStore_ = std::make_shared<SnapshotCloneMetaStoreEtcd>(etcdClient,
        codec);
    if (metaStore_->Init() < 0) {
        LOG(ERROR) << "metaStore init fail.";
        return -1;
    }
    return 0;
}

int CurveCluster::BuildNetWork() {
    std::string cmd_dir("./test/integration/cluster_common/build.sh");
    return system(cmd_dir.c_str());
}

int CurveCluster::StopCluster() {
    LOG(INFO) << "stop cluster begin...";

    int ret = 0;
    if (StopAllMDS() < 0)
        ret = -1;

    if (StopAllChunkServer() < 0)
        ret = -1;

    if (StopAllSnapshotCloneServer() < 0)
        ret = -1;

    if (StopAllEtcd() < 0)
        ret = -1;

    if (!ret)
        LOG(INFO) << "success stop cluster";
    else
        LOG(ERROR) << "Error occurred to stop cluster";
    return ret;
}

int CurveCluster::StartSingleMDS(int id, const std::string &ipPort,
                                 int dummyPort,
                                 const std::vector<std::string> &mdsConf,
                                 bool expectLeader) {
    LOG(INFO) << "start mds " << ipPort << " begin...";
    pid_t pid = ::fork();
    if (0 > pid) {
        LOG(ERROR) << "start mds " << ipPort << " fork failed";
        return -1;
    } else if (0 == pid) {
        // 在子进程中起一个mds
        // ./bazel-bin/src/mds/main/curvemds
        std::string cmd_dir =
            std::string("./bazel-bin/src/mds/main/curvemds --mdsAddr=") +
            ipPort + " --dummyPort=" + std::to_string(dummyPort);
        for (auto &item : mdsConf) {
            cmd_dir += item;
        }

        /**
         *  重要提示！！！！
         *  fork后，子进程尽量不要用LOG()打印，可能死锁！！！
         */
        execl("/bin/sh", "sh", "-c", cmd_dir.c_str(), NULL);
        exit(0);
    }

    if (ProbePort(ipPort, 20000, expectLeader) < 0) {
        LOG(ERROR) << "start mds " << ipPort << " faild, pid=" << pid;
        return -1;
    }

    LOG(INFO) << "start mds " << ipPort << " success";
    mdsPidMap_[id] = pid;
    mdsIpPort_[id] = ipPort;

    for (auto it : mdsPidMap_) {
        LOG(INFO) << "mds pid = " << it.second;
    }
    return pid;
}

int CurveCluster::StopMDS(int id) {
    LOG(INFO) << "stop mds " << mdsIpPort_[id] << " begin...";
    if (mdsPidMap_.find(id) != mdsPidMap_.end()) {
        if (kill(mdsPidMap_[id], SIGKILL) < 0) {
            LOG(ERROR) << "kill mds: " << strerror(errno);
            RETURN_IF_NOT_ZERO(-1);
        }
        int waitStatus;
        waitpid(mdsPidMap_[id], &waitStatus, 0);
        RETURN_IF_NOT_ZERO(ProbePort(MDSIpPort(id), 20000, false));
        mdsPidMap_.erase(id);
    }

    ::sleep(2);
    LOG(INFO) << "stop mds " << MDSIpPort(id) << " success";
    return 0;
}

int CurveCluster::StopAllMDS() {
    LOG(INFO) << "stop all mds begin...";

    int ret = 0;
    for (auto it = mdsPidMap_.begin(); it != mdsPidMap_.end();
         it = mdsPidMap_.erase(it)) {
        LOG(INFO) << "begin to stop mds " << it->first << " " << it->second
                  << ", port: " << mdsIpPort_[it->first];
        if (kill(it->second, SIGKILL) < 0) {
            LOG(ERROR) << "kill mds: " << strerror(errno);
            ret = -1;
            continue;
        }
        int waitStatus;
        waitpid(it->second, &waitStatus, 0);
        if (ProbePort(mdsIpPort_[it->first], 20000, false) < 0) {
            ret = -1;
            LOG(ERROR) << "failed to stop mds " << it->first << " "
                       << it->second << ", port: " << mdsIpPort_[it->first];
        } else {
            LOG(INFO) << "success stop mds " << it->first << " " << it->second;
        }
    }

    ::sleep(2);
    if (ret < 0)
        LOG(ERROR) << "Error occured to stop all mds";
    else
        LOG(INFO) << "success stop all mds";
    return ret;
}

int CurveCluster::StartSnapshotCloneServer(
    int id, const std::string &ipPort,
    const std::vector<std::string> &snapshotcloneConf) {
    LOG(INFO) << "start snapshotcloneserver " << ipPort << " begin ...";
    pid_t pid = ::fork();
    if (0 > pid) {
        LOG(ERROR) << "start snapshotcloneserver " << ipPort << " fork failed";
        return -1;
    } else if (0 == pid) {
        // 在子进程中起一个snapshotcloneserver
        std::string cmd_dir = std::string(
                                  "./bazel-bin/src/snapshotcloneserver/"
                                  "snapshotcloneserver --addr=") +  // NOLINT
                              ipPort;
        for (auto &item : snapshotcloneConf) {
            cmd_dir += item;
        }
        /**
         *  重要提示！！！！
         *  fork后，子进程尽量不要用LOG()打印，可能死锁！！！
         */
        execl("/bin/sh", "sh", "-c", cmd_dir.c_str(), NULL);
        exit(0);
    }

    if (ProbePort(ipPort, 20000, true) < 0) {
        LOG(ERROR) << "start snapshotcloneserver " << ipPort
                   << " faild, pid=" << pid;
        return -1;
    }
    LOG(INFO) << "start snapshotcloneserver " << ipPort << " (pid=" << pid
              << ") success.";
    snapPidMap_[id] = pid;
    snapIpPort_[id] = ipPort;
    snapConf_[id] = snapshotcloneConf;
    return pid;
}

int CurveCluster::StopSnapshotCloneServer(int id, bool force) {
    LOG(INFO) << "stop snapshotcloneserver " << snapIpPort_[id] << " begin...";
    if (snapPidMap_.find(id) != snapPidMap_.end()) {
        int res = 0;
        if (force) {
            res = kill(snapPidMap_[id], SIGKILL);
        } else {
            res = kill(snapPidMap_[id], SIGKILL);
        }
        if (res < 0)
            LOG(ERROR) << "failed to kill snapshotcloneserver: "
                       << strerror(errno);
        RETURN_IF_NOT_ZERO(res);
        RETURN_IF_NOT_ZERO(ProbePort(snapIpPort_[id], 5000, false));
        int waitStatus;
        waitpid(snapPidMap_[id], &waitStatus, 0);
        snapPidMap_.erase(id);
    }
    LOG(INFO) << "stop snapshotcloneserver " << snapIpPort_[id] << " success.";
    return 0;
}

int CurveCluster::RestartSnapshotCloneServer(int id, bool force) {
    LOG(INFO) << "restart snapshotcloneserver " << snapIpPort_[id]
              << " begin...";
    pid_t pid = -1;
    if (snapPidMap_.find(id) != snapPidMap_.end()) {
        int res = 0;
        if (force) {
            res = kill(snapPidMap_[id], SIGKILL);
        } else {
            res = kill(snapPidMap_[id], SIGKILL);
        }
        if (res < 0)
            LOG(ERROR) << "failed to kill snapshotcloneserver: "
                       << strerror(errno);
        RETURN_IF_NOT_ZERO(res);
        RETURN_IF_NOT_ZERO(ProbePort(snapIpPort_[id], 20000, false));
        int waitStatus;
        waitpid(snapPidMap_[id], &waitStatus, 0);
        std::string ipPort = snapIpPort_[id];
        auto conf = snapConf_[id];
        snapPidMap_.erase(id);
        pid = StartSnapshotCloneServer(id, ipPort, conf);
    }

    LOG(INFO) << "restart snapshotcloneserver " << snapIpPort_[id] << " done.";
    return pid;
}

int CurveCluster::StopAllSnapshotCloneServer() {
    LOG(INFO) << "stop all snapshotclone server begin ...";

    int ret = 0;
    auto tempMap = snapPidMap_;
    for (auto pair : tempMap) {
        if (StopSnapshotCloneServer(pair.first) < 0)
            ret = -1;
    }

    // 等待进程完全退出
    ::sleep(2);
    LOG(INFO) << "stop all snapshotcloneservver end.";
    return ret;
}

int CurveCluster::StartSingleEtcd(int id, const std::string &clientIpPort,
                                  const std::string &peerIpPort,
                                  const std::vector<std::string> &etcdConf) {
    LOG(INFO) << "start etcd " << clientIpPort << " begin...";

    pid_t pid = ::fork();
    if (0 > pid) {
        LOG(ERROR) << "start etcd " << id << " fork failed";
        return -1;
    } else if (0 == pid) {
        // 在子进程中起一个etcd
        // ip netns exec integ_etcd1 etcd
        std::string cmd_dir =
            std::string(" etcd --listen-peer-urls http://") + peerIpPort +
            std::string(" --initial-advertise-peer-urls http://") + peerIpPort +
            std::string(" --listen-client-urls http://") + clientIpPort +
            std::string(" --advertise-client-urls http://") + clientIpPort +
            std::string(" --pre-vote") +
            std::string(" --election-timeout 3000") +
            std::string(" --heartbeat-interval 300");
        for (auto &item : etcdConf) {
            cmd_dir += item;
        }

        /**
         *  重要提示！！！！
         *  fork后，子进程尽量不要用LOG()打印，可能死锁！！！
         */
        execl("/bin/sh", "sh", "-c", cmd_dir.c_str(), NULL);
        exit(0);
    }

    if (ProbePort(clientIpPort, 20000, true) < 0) {
        LOG(ERROR) << "start etcd " << clientIpPort << " faild, pid=" << pid;
        return -1;
    }
    LOG(INFO) << "start etcd " << clientIpPort << " success";
    etcdPidMap_[id] = pid;
    etcdClientIpPort_[id] = clientIpPort;
    etcdPeersIpPort_[id] = peerIpPort;
    return pid;
}

bool CurveCluster::WaitForEtcdClusterAvalible(int waitSec) {
    std::string endpoint;
    if (etcdClientIpPort_.empty()) {
        LOG(INFO) << "no initialized etcd cluster";
        return false;
    } else {
        int i = 0;
        for (auto &item : etcdClientIpPort_) {
            i++;
            if (i == etcdClientIpPort_.size()) {
                endpoint += "http://" + item.second;
            } else {
                endpoint += "http://" + item.second + ",";
            }
        }
    }

    int res;
    std::string cmd = std::string("etcdctl --endpoints=") + endpoint +
                      std::string(" --command-timeout=1s put test test");
    LOG(INFO) << "exec command: " << cmd;
    uint64_t start = ::curve::common::TimeUtility::GetTimeofDaySec();
    while (::curve::common::TimeUtility::GetTimeofDaySec() - start < waitSec) {
        res = system(cmd.c_str());
        if (res != 0) {
            ::sleep(1);
            continue;
        } else {
            break;
        }
    }

    LOG(INFO) << cmd << " result: " << res;
    return res == 0;
}

int CurveCluster::StopEtcd(int id) {
    LOG(INFO) << "stop etcd " << etcdClientIpPort_[id] << " begin...";

    if (etcdPidMap_.find(id) != etcdPidMap_.end()) {
        if (kill(etcdPidMap_[id], SIGKILL) < 0) {
            LOG(ERROR) << "kill etcd: " << strerror(errno);
            RETURN_IF_NOT_ZERO(-1);
        }
        int waitStatus;
        waitpid(etcdPidMap_[id], &waitStatus, 0);
        RETURN_IF_NOT_ZERO(ProbePort(etcdClientIpPort_[id], 20000, false));
        RETURN_IF_NOT_ZERO(ProbePort(etcdPeersIpPort_[id], 20000, false));
        LOG(INFO) << "stop etcd " << etcdClientIpPort_[id] << ", "
                  << etcdPidMap_[id] << " success";
        etcdPidMap_.erase(id);
    } else {
        LOG(INFO) << "etcd " << id << " not exist";
    }

    ::sleep(2);
    return 0;
}

int CurveCluster::StopAllEtcd() {
    LOG(INFO) << "stop all etcd begin...";

    int ret = 0;
    int waitStatus;
    for (auto it = etcdPidMap_.begin(); it != etcdPidMap_.end();
         it = etcdPidMap_.erase(it)) {
        LOG(INFO) << "begin to stop etcd" << it->first << " " << it->second
                  << ", " << etcdClientIpPort_[it->first];
        if (kill(it->second, SIGKILL) < 0) {
            LOG(ERROR) << "kill etcd: " << strerror(errno);
            ret = -1;
            continue;
        }
        waitpid(it->second, &waitStatus, 0);
        if (ProbePort(etcdClientIpPort_[it->first], 20000, false) < 0) {
            ret = -1;
            LOG(ERROR) << "faild to stop etcd" << it->first << " "
                       << it->second;
        } else {
            LOG(INFO) << "success stop etcd" << it->first << " " << it->second;
        }
    }

    ::sleep(2);
    if (ret < 0)
        LOG(ERROR) << "Error occured to stop all etcd";
    else
        LOG(INFO) << "success stop all etcd";
    return ret;
}

int CurveCluster::FormatFilePool(const std::string &filePooldir,
                                 const std::string &filePoolmetapath,
                                 const std::string &filesystempath,
                                 uint32_t size) {
    LOG(INFO) << "FormatFilePool begin...";

    std::string cmd = std::string("./bazel-bin/src/tools/curve_format") +
                      " -filePoolDir=" + filePooldir +
                      " -filePoolMetaPath=" + filePoolmetapath +
                      " -fileSystemPath=" + filesystempath +
                      " -allocateByPercent=false -preAllocateNum=" +
                      std::to_string(size * 300) +
                      " -needWriteZero=false";

    RETURN_IF_NOT_ZERO(system(cmd.c_str()));

    LOG(INFO) << "FormatFilePool end.";
    return 0;
}

int CurveCluster::StartSingleChunkServer(
    int id, const std::string &ipPort,
    const std::vector<std::string> &chunkserverConf) {
    LOG(INFO) << "start chunkserver " << id << ", " << ipPort << " begin...";
    std::vector<std::string> split;
    ::curve::common::SplitString(ipPort, ":", &split);
    if (2 != split.size()) {
        LOG(ERROR) << "invalid chunkserver ipPort: " << ipPort;
        return -1;
    }

    pid_t pid = ::fork();
    if (0 > pid) {
        LOG(ERROR) << "start chunkserver " << id << " fork failed";
        return -1;
    } else if (0 == pid) {
        // 在子进程中起一个chunkserver
        std::string cmd_dir =
            std::string("./bazel-bin/src/chunkserver/chunkserver ") +
            std::string(" -chunkServerIp=") + split[0] +
            std::string(" -chunkServerPort=") + split[1];
        for (auto &item : chunkserverConf) {
            cmd_dir += item;
        }

        /**
         *  重要提示！！！！
         *  fork后，子进程尽量不要用LOG()打印，可能死锁！！！
         */
        execl("/bin/sh", "sh", "-c", cmd_dir.c_str(), NULL);
        exit(0);
    }

    if (ProbePort(ipPort, 20000, true) < 0) {
        LOG(ERROR) << "start chunkserver " << ipPort << " failed, pid=" << pid;
        return -1;
    }
    LOG(INFO) << "start chunkserver " << ipPort << " success";
    chunkserverPidMap_[id] = pid;
    chunkserverIpPort_[id] = ipPort;
    return pid;
}

int CurveCluster::StartSingleChunkServerInBackground(
    int id, const std::vector<std::string> &chunkserverConf) {
    std::vector<std::string> ipPort;
    ::curve::common::SplitString(ChunkServerIpPortInBackground(id), ":",
                                 &ipPort);
    if (2 != ipPort.size()) {
        LOG(ERROR) << "invalid chunkserver ipPort: "
                   << ChunkServerIpPortInBackground(id);
        return -1;
    }

    LOG(INFO) << "start chunkserver " << id << ", " << ipPort[0] << ipPort[1]
              << " in background begin...";
    pid_t pid = ::fork();
    if (0 > pid) {
        LOG(ERROR) << "start chunkserver " << id << " fork failed";
        return -1;
    } else if (0 == pid) {
        // 在子进程中起一个chunkserver
        std::string cmd_dir =
            std::string("ip netns exec ") + nsPrefix_ + std::string("cs") +
            std::to_string(id) +
            std::string(" ./bazel-bin/src/chunkserver/chunkserver ") +
            std::string(" -chunkServerIp=") + ipPort[0] +
            std::string(" -chunkServerPort=") + ipPort[1];
        for (auto &item : chunkserverConf) {
            cmd_dir += item;
        }
        /**
         *  重要提示！！！！
         *  fork后，子进程尽量不要用LOG()打印，可能死锁！！！
         */
        execl("/bin/sh", "sh", "-c", cmd_dir.c_str(), NULL);
        exit(0);
    }

    if (ProbePort(ChunkServerIpPortInBackground(id), 20000, true) < 0) {
        LOG(ERROR) << "start chunkserver " << ChunkServerIpPortInBackground(id)
                   << " failed, pid=" << pid;
        return -1;
    }
    LOG(INFO) << "start chunkserver " << id << " in background success";
    chunkserverPidMap_[id] = pid;
    chunkserverIpPort_[id] = ChunkServerIpPortInBackground(id);
    return pid;
}

int CurveCluster::StopChunkServer(int id) {
    LOG(INFO) << "stop chunkserver " << chunkserverIpPort_[id] << " begin...";

    if (chunkserverPidMap_.find(id) != chunkserverPidMap_.end()) {
        std::string killCmd = "kill " + std::to_string(chunkserverPidMap_[id]);
        LOG(INFO) << "exec cmd: " << killCmd << " to stop chunkserver " << id;
        if (system(killCmd.c_str()) < 0) {
            LOG(ERROR) << "kill chunkserver: " << strerror(errno);
            RETURN_IF_NOT_ZERO(-1);
        }
        int waitStatus;
        waitpid(chunkserverPidMap_[id], &waitStatus, 0);
        RETURN_IF_NOT_ZERO(ProbePort(chunkserverIpPort_[id], 20000, false));
        chunkserverPidMap_.erase(id);
        LOG(INFO) << "stop chunkserver " << chunkserverIpPort_[id]
                  << " success";
    } else {
        LOG(INFO) << "chunkserver " << id << " not exist";
    }

    ::sleep(2);
    return 0;
}

int CurveCluster::StopAllChunkServer() {
    LOG(INFO) << "sttop all chunkserver begin...";

    int ret = 0;
    for (auto it = chunkserverPidMap_.begin(); it != chunkserverPidMap_.end();
         it = chunkserverPidMap_.erase(it)) {
        LOG(INFO) << "begin to stop chunkserver" << it->first << " "
                  << it->second << ", " << chunkserverIpPort_[it->first];
        if (kill(it->second, SIGKILL) < 0) {
            LOG(ERROR) << "kill chunkserver: " << strerror(errno);
            ret = -1;
            continue;
        }
        int waitStatus;
        waitpid(it->second, &waitStatus, 0);
        if (ProbePort(chunkserverIpPort_[it->first], 20000, false) < 0) {
            ret = -1;
            LOG(ERROR) << "failed to stop chunkserver" << it->first << " "
                       << it->second;
        } else {
            LOG(INFO) << "success stop chunkserver" << it->first << " "
                      << it->second;
        }
    }

    ::sleep(2);
    if (ret < 0)
        LOG(ERROR) << "Error occured to stop all chunkserver";
    else
        LOG(INFO) << "success stop all chunkserver";
    return ret;
}

std::string CurveCluster::MDSIpPort(int id) {
    if (mdsIpPort_.find(id) == mdsIpPort_.end()) {
        return "";
    }
    return mdsIpPort_[id];
}

std::string CurveCluster::EtcdClientIpPort(int id) {
    if (etcdClientIpPort_.find(id) == etcdClientIpPort_.end()) {
        return "";
    }
    return etcdClientIpPort_[id];
}

std::string CurveCluster::EtcdPeersIpPort(int id) {
    if (etcdPeersIpPort_.find(id) == etcdPeersIpPort_.end()) {
        return "";
    }
    return etcdPeersIpPort_[id];
}

std::string CurveCluster::ChunkServerIpPort(int id) {
    if (chunkserverIpPort_.find(id) == chunkserverIpPort_.end()) {
        return "";
    }
    return chunkserverIpPort_[id];
}

int CurveCluster::HangMDS(int id) {
    RETURN_IF_FALSE(mdsPidMap_.find(id) != mdsPidMap_.end());
    return HangProcess(mdsPidMap_[id]);
}

int CurveCluster::RecoverHangMDS(int id) {
    RETURN_IF_FALSE(mdsPidMap_.find(id) != mdsPidMap_.end());
    return RecoverHangProcess(mdsPidMap_[id]);
}

int CurveCluster::HangEtcd(int id) {
    RETURN_IF_FALSE(etcdPidMap_.find(id) != etcdPidMap_.end());
    return HangProcess(etcdPidMap_[id]);
}

int CurveCluster::RecoverHangEtcd(int id) {
    RETURN_IF_FALSE(etcdPidMap_.find(id) != etcdPidMap_.end());
    return RecoverHangProcess(etcdPidMap_[id]);
}

int CurveCluster::HangChunkServer(int id) {
    RETURN_IF_FALSE(chunkserverPidMap_.find(id) != chunkserverPidMap_.end());
    return HangProcess(chunkserverPidMap_[id]);
}

int CurveCluster::RecoverHangChunkServer(int id) {
    RETURN_IF_FALSE(chunkserverPidMap_.find(id) != chunkserverPidMap_.end());
    return RecoverHangProcess(chunkserverPidMap_[id]);
}

int CurveCluster::HangProcess(pid_t pid) {
    LOG(INFO) << "hang pid: " << pid << " begin...";
    if (kill(pid, SIGSTOP) < 0) {
        LOG(ERROR) << "hang pid: " << strerror(errno);
        RETURN_IF_NOT_ZERO(-1);
    }
    int waitStatus;
    waitpid(pid, &waitStatus, WUNTRACED);
    LOG(INFO) << "success hang pid: " << pid;
    return 0;
}

int CurveCluster::RecoverHangProcess(pid_t pid) {
    LOG(INFO) << "recover hang pid: " << pid << " begin...";
    if (kill(pid, SIGCONT) < 0) {
        LOG(ERROR) << "recover pid: " << strerror(errno);
        RETURN_IF_NOT_ZERO(-1);
    }
    int waitStatus;
    waitpid(pid, &waitStatus, WCONTINUED);
    LOG(INFO) << "success recover hang pid: " << pid;
    return 0;
}

std::string CurveCluster::ChunkServerIpPortInBackground(int id) {
    return networkSegment_ + std::to_string(40 + id) + std::string(":3500");
}

int CurveCluster::PreparePhysicalPool(int mdsId,
                                      const std::string &clusterMap) {
    LOG(INFO) << "create physicalpool begin...";

    std::string createPPCmd = std::string("./bazel-bin/tools/curvefsTool") +
                              std::string(" -cluster_map=") + clusterMap +
                              std::string(" -mds_addr=") + MDSIpPort(mdsId) +
                              std::string(" -op=create_physicalpool") +
                              std::string(" -stderrthreshold=0") +
                              std::string(" -rpcTimeOutMs=20000") +
                              std::string(" -minloglevel=0");

    LOG(INFO) << "exec cmd: " << createPPCmd;
    RETURN_IF_NOT_ZERO(system(createPPCmd.c_str()));

    LOG(INFO) << "success create physicalpool";
    return 0;
}

int CurveCluster::PrepareLogicalPool(int mdsId, const std::string &clusterMap) {
    LOG(INFO) << "create logicalpool begin...";

    std::string createLPCmd =
        std::string("./bazel-bin/tools/curvefsTool") +
        std::string(" -cluster_map=") + clusterMap +
        std::string(" -mds_addr=") + MDSIpPort(mdsId) +
        std::string(" -op=create_logicalpool") +
        std::string(" -stderrthreshold=0 -minloglevel=0");

    LOG(INFO) << "exec cmd: " << createLPCmd;
    RETURN_IF_NOT_ZERO(system(createLPCmd.c_str()));

    LOG(INFO) << "success create logicalpool";
    return 0;
}

bool CurveCluster::CurrentServiceMDS(int *curId) {
    for (auto mdsId : mdsPidMap_) {
        if (0 == ProbePort(mdsIpPort_[mdsId.first], 20000, true)) {
            *curId = mdsId.first;
            LOG(INFO) << "mds" << mdsId.first << ": " << mdsIpPort_[mdsId.first]
                      << "is in service";
            return true;
        }
    }

    LOG(INFO) << "no mds in service";
    return false;
}

int CurveCluster::CreateFile(const std::string &user, const std::string &pwd,
                             const std::string &fileName, uint64_t fileSize,
                             bool normalFile, const std::string& poolset) {
    LOG(INFO) << "create file: " << fileName << ", size: " << fileSize
              << " begin...";
    UserInfo_t info(user, pwd);
    CreateFileContext context;
    context.pagefile = true;
    context.name = fileName;
    context.user = info;
    context.length = fileSize;
    context.poolset = poolset;

    RETURN_IF_NOT_ZERO(
        mdsClient_->CreateFile(context));
    LOG(INFO) << "success create file";
    return 0;
}

int CurveCluster::ProbePort(const std::string &ipPort, int64_t timeoutMs,
                            bool expectOpen) {
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == socket_fd) {
        LOG(ERROR) << "create socket fail";
        return -1;
    }

    std::vector<std::string> res;
    ::curve::common::SplitString(ipPort, ":", &res);
    if (res.size() != 2) {
        return -1;
    }
    std::string ip = res[0];
    uint64_t port;
    if (false == ::curve::common::StringToUll(res[1], &port)) {
        LOG(ERROR) << "split " << ipPort << " fail";
        return -1;
    }

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(res[0].c_str());

    uint64_t start = ::curve::common::TimeUtility::GetTimeofDayMs();
    while (::curve::common::TimeUtility::GetTimeofDayMs() - start < timeoutMs) {
        int connectRes =
            connect(socket_fd, (struct sockaddr *)&addr, sizeof(addr));
        if (expectOpen && connectRes == 0) {
            LOG(INFO) << "probe " << ipPort << " success.";
            close(socket_fd);
            return 0;
        }

        if (!expectOpen && connectRes == -1) {
            LOG(INFO) << "probe " << ipPort << " fail.";
            close(socket_fd);
            return 0;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs / 10));
    }

    close(socket_fd);
    LOG(INFO) << "probe " << ipPort << " fail within " << timeoutMs << " ms";
    return -1;
}
}  // namespace curve
