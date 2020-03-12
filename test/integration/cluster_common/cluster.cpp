/*
 * Project: curve
 * Created Date: 19-08-15
 * Author: lixiaocui
 * Copyright (c) 2019 netease
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

using ::curve::client::UserInfo_t;

namespace curve {

#define RETURN_IF_NOT_ZERO(x)                                                  \
    do {                                                                       \
        int ret = x;                                                           \
        if (ret != 0) {                                                        \
            LOG(ERROR) << #x << " is not ZERO";                                \
            return ret;                                                        \
        }                                                                      \
    } while (0)

#define RETURN_IF_FALSE(x)                                                     \
    do {                                                                       \
        bool ret = x;                                                          \
        if (!ret) {                                                            \
            LOG(ERROR) << #x << " is FALSE";                                   \
            return -1;                                                         \
        }                                                                      \
    } while (0)

int CurveCluster::InitDB(const std::string &mdsTable, const std::string &user,
                         const std::string &url, const std::string &password,
                         int poolSize) {
    mdsRepo_ = new MdsRepo();
    return mdsRepo_->connectDB(mdsTable, user, url, password, poolSize);
}

int CurveCluster::InitMdsClient(const MetaServerOption_t &op) {
    mdsClient_ = std::make_shared<MDSClient>();
    return mdsClient_->Initialize(op);
}

int CurveCluster::BuildNetWork() {
    std::string cmd_dir("./test/integration/cluster_common/build.sh");
    return system(cmd_dir.c_str());
}

int CurveCluster::StopCluster() {
    LOG(INFO) << "stop cluster begin...";

    int waitStatus;
    for (auto it = mdsPidMap_.begin(); it != mdsPidMap_.end();) {
        LOG(INFO) << "begin to stop mds" << it->first << " " << it->second
                  << ", port: " << mdsIpPort_[it->first];
        kill(it->second, SIGTERM);
        LOG(INFO) << "kill mds: " << strerror(errno);
        waitpid(it->second, &waitStatus, 0);
        RETURN_IF_NOT_ZERO(ProbePort(mdsIpPort_[it->first], 20000, false));
        LOG(INFO) << "success stop mds" << it->first << " " << it->second;
        it = mdsPidMap_.erase(it);
    }

    for (auto it = etcdPidMap_.begin(); it != etcdPidMap_.end();) {
        LOG(INFO) << "begin to stop etcd" << it->first << " " << it->second
                  << ", port: " << etcdClientIpPort_[it->first];
        kill(it->second, SIGTERM);
        LOG(INFO) << "kill etcd: " << strerror(errno);
        waitpid(it->second, &waitStatus, 0);
        RETURN_IF_NOT_ZERO(
            ProbePort(etcdClientIpPort_[it->first], 20000, false));
        LOG(INFO) << "success stop etcd" << it->first << " " << it->second;
        it = etcdPidMap_.erase(it);
    }

    for (auto it = chunkserverPidMap_.begin();
         it != chunkserverPidMap_.end();) {
        LOG(INFO) << "begin to stop chunkserver" << it->first << " "
                  << it->second << "， port: " << chunkserverIpPort_[it->first];
        kill(it->second, SIGTERM);
        LOG(INFO) << "kill chunkserver: " << strerror(errno);
        waitpid(it->second, &waitStatus, 0);
        RETURN_IF_NOT_ZERO(
            ProbePort(chunkserverIpPort_[it->first], 20000, false));
        LOG(INFO) << "success stop chunkserver" << it->first << " "
                  << it->second;
        it = chunkserverPidMap_.erase(it);
    }

    // 等待进程完全退出
    ::sleep(2);
    LOG(INFO) << "success stop cluster";
    return 0;
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
        execl("/bin/sh", "sh", "-c", cmd_dir.c_str(), NULL);
        // 使用error级别，帮助收集日志
        LOG(ERROR) << "start single mds: " << strerror(errno);
        exit(0);
    }

    RETURN_IF_NOT_ZERO(ProbePort(ipPort, 20000, expectLeader));
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
        kill(mdsPidMap_[id], SIGTERM);
        LOG(INFO) << "kill mds: " << strerror(errno);
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

    for (auto it = mdsPidMap_.begin(); it != mdsPidMap_.end();) {
        LOG(INFO) << "begin to stop mds" << it->first << " " << it->second
                  << ", port: " << mdsIpPort_[it->first];
        kill(it->second, SIGTERM);
        LOG(INFO) << "kill mds: " << strerror(errno);
        int waitStatus;
        waitpid(it->second, &waitStatus, 0);
        RETURN_IF_NOT_ZERO(ProbePort(mdsIpPort_[it->first], 20000, false));
        LOG(INFO) << "success stop mds" << it->first << " " << it->second;
        it = mdsPidMap_.erase(it);
    }

    ::sleep(2);
    LOG(INFO) << "success stop all mds";
    return 0;
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

        LOG(INFO) << "start exec cmd: " << cmd_dir;
        execl("/bin/sh", "sh", "-c", cmd_dir.c_str(), NULL);
        LOG(ERROR) << "start single etcd: " << strerror(errno);
        exit(0);
    }

    RETURN_IF_NOT_ZERO(ProbePort(clientIpPort, 20000, true));
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
        kill(etcdPidMap_[id], SIGTERM);
        LOG(INFO) << "kill etcd: " << strerror(errno);
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

    int waitStatus;
    for (auto it = etcdPidMap_.begin(); it != etcdPidMap_.end();) {
        LOG(INFO) << "begin to stop etcd" << it->first << " " << it->second
                  << ", " << etcdClientIpPort_[it->first];
        kill(it->second, SIGTERM);
        LOG(INFO) << "kill etcd: " << strerror(errno);
        waitpid(it->second, &waitStatus, 0);
        RETURN_IF_NOT_ZERO(
            ProbePort(etcdClientIpPort_[it->first], 20000, false));
        LOG(INFO) << "success stop etcd" << it->first << " " << it->second;
        it = etcdPidMap_.erase(it);
    }

    ::sleep(2);
    LOG(INFO) << "success stop all etcd";
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
        LOG(INFO) << "start exec cmd: " << cmd_dir;
        execl("/bin/sh", "sh", "-c", cmd_dir.c_str(), NULL);
        LOG(ERROR) << "start single chunkserver: " << strerror(errno);
        exit(0);
    }

    RETURN_IF_NOT_ZERO(ProbePort(ipPort, 20000, true));
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
        LOG(INFO) << "start exec cmd: " << cmd_dir;
        execl("/bin/sh", "sh", "-c", cmd_dir.c_str(), NULL);
        LOG(ERROR) << "start single chunkserver in background: "
                   << strerror(errno);
        exit(0);
    }

    RETURN_IF_NOT_ZERO(
        ProbePort(ChunkServerIpPortInBackground(id), 20000, true));
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
        system(killCmd.c_str());
        LOG(INFO) << "kill chunkserver: " << strerror(errno);
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

    for (auto it = chunkserverPidMap_.begin();
         it != chunkserverPidMap_.end();) {
        LOG(INFO) << "begin to stop chunkserver" << it->first << " "
                  << it->second << ", " << chunkserverIpPort_[it->first];
        kill(it->second, SIGTERM);
        LOG(INFO) << "kill chunkserver: " << strerror(errno);
        int waitStatus;
        waitpid(it->second, &waitStatus, 0);
        RETURN_IF_NOT_ZERO(
            ProbePort(chunkserverIpPort_[it->first], 20000, false));
        LOG(INFO) << "success stop chunkserver" << it->first << " "
                  << it->second;
        it = chunkserverPidMap_.erase(it);
    }

    ::sleep(2);
    LOG(INFO) << "success stop all chunkserver";
    return 0;
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
    kill(pid, SIGSTOP);
    LOG(INFO) << "hung pid: " << strerror(errno);
    int waitStatus;
    waitpid(pid, &waitStatus, WUNTRACED);
    LOG(INFO) << "success hang pid: " << pid;
    return 0;
}

int CurveCluster::RecoverHangProcess(pid_t pid) {
    LOG(INFO) << "recover hang pid: " << pid << " begin...";
    kill(pid, SIGCONT);
    LOG(INFO) << "recover pid: " << strerror(errno);
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
                              std::string(" -minloglevel=0") +
                              std::string(" -rpcTimeOutMs=10000");

    LOG(INFO) << "exec cmd: " << createPPCmd;
    RETURN_IF_NOT_ZERO(system(createPPCmd.c_str()));

    LOG(INFO) << "success create physicalpool";
    return 0;
}

int CurveCluster::PrepareLogicalPool(int mdsId, const std::string &clusterMap,
                                     int copysetNum,
                                     const std::string &physicalPoolName) {
    LOG(INFO) << "create logicalpool begin...";

    std::string createLPCmd =
        std::string("./bazel-bin/tools/curvefsTool") +
        std::string(" -cluster_map=") + clusterMap +
        std::string(" -mds_addr=") + MDSIpPort(mdsId) +
        std::string(" -copyset_num=") + std::to_string(copysetNum) +
        std::string(" -op=create_logicalpool") +
        std::string(" -physicalpool_name=") + physicalPoolName +
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
                             bool normalFile) {
    LOG(INFO) << "create file: " << fileName << ", size: " << fileSize
              << " begin...";
    UserInfo_t info(user, pwd);
    RETURN_IF_NOT_ZERO(
        mdsClient_->CreateFile(fileName, info, fileSize, normalFile));
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

    bool satisfy = false;
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
