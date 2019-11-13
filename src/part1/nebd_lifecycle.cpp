/*
 * Project: nebd
 * File Created: 2019-10-08
 * Author: hzchenwei7
 * Copyright (c) 2018 NetEase
 */

#include "src/part1/nebd_lifecycle.h"

#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <dirent.h>
#include <libgen.h>
#include <json/json.h>
#include <butil/logging.h>
#include <butil/endpoint.h>
#include "src/common/configuration.h"
#include "src/part1/libnebd_file.h"

namespace nebd {
namespace client {

int LifeCycleManager::Start(common::Configuration *conf) {
    LOG(INFO) << "start LifeCycleManager begin.";

    int ret = LoadConf(conf);
    if (ret != 0) {
        LOG(ERROR) << "LifeCycleManager LoadConf fail";
        return -1;
    }

    // 根据本进程pid，查询qemu的uuid
    pidPart1_ = getpid();
    LOG(INFO) << "qemu pid = " << pidPart1_;

    char uuid[kUuidLength + 1] = {0};
    ret = getUuid(pidPart1_, lifeCycleOptions_.qemuProcName.c_str(), uuid);
    if (ret != 0) {
        LOG(ERROR) << "get qemu uuid fail";
        return -1;
    }

    qemuUUID_ = uuid;
    LOG(INFO) << "get qemu uuid = " << qemuUUID_;

    // open文件锁
    int fd = open(lifeCycleOptions_.lockFile.c_str(), O_RDONLY | O_CREAT, 0644);
    if (fd < 0) {
        LOG(ERROR) << "open lock file fail, file = "
                   << lifeCycleOptions_.lockFile;
        return -1;
    }

    // 检查part2是否运行
    bool isPart2Alive = false;

    uint32_t retryCount = 0;
    do {
        // 加文件锁
        bool boolRet = LockFileWithRetry(fd,
                                    lifeCycleOptions_.fileLockRetryTimes,
                                    lifeCycleOptions_.fileLockRetryIntervalUs);

        if (!boolRet) {
            LOG(ERROR) << "lock file with retry fail, file = "
                       << lifeCycleOptions_.lockFile;
            close(fd);
            return -1;
        }

        ret = CheckProcAlive(uuid, lifeCycleOptions_.part2ProcName.c_str(),
                                    &isPart2Alive, &pidPart2_);
        if (ret < 0) {
            int release = flock(fd, LOCK_UN);
            if (release < 0) {
                LOG(ERROR) << "unlock file fail, file = "
                           << lifeCycleOptions_.lockFile;
            }
            LOG(ERROR) << "check part2 alive fail";
            close(fd);
            return -1;
        }

        if (isPart2Alive) {
            // 如果part2存活，先不释放文件锁，继续持有锁去查询port信息
            break;
        }

        // 清理初始化文件，拉起part2
        CleanMetadataFile();
        StartPart2();

        // 释放文件锁
        int release = flock(fd, LOCK_UN);
        if (release < 0) {
            LOG(ERROR) << "unlock file fail, file = "
                       << lifeCycleOptions_.lockFile;
            close(fd);
            return -1;
        }

        retryCount++;

        usleep(lifeCycleOptions_.part2StartRetryIntervalUs);
    } while (!isPart2Alive
             && retryCount < lifeCycleOptions_.part2StartRetryTimes);

    // 拉起part2多次失败
    if (!isPart2Alive) {
        LOG(ERROR) << "start part2 fail.";
        close(fd);
        return -1;
    }

    // 读取端口，端口读取成功后，启动心跳服务
    uint32_t port = 0;
    retryCount = 0;
    while (1) {
        ret = GetPortFromPart2(&port);

        // 前面在查询part2是否存活时已经加锁，这里读取port之后释放文件锁
        int release = flock(fd, LOCK_UN);
        if (release < 0) {
            LOG(ERROR) << "unlock file fail, file = "
                       << lifeCycleOptions_.lockFile;
            close(fd);
            return -1;
        }

        if (ret == 0) {
            // 找到port
            LOG(INFO) << "get port from part2 success, port = " << port;
            break;
        }

        retryCount++;
        LOG(WARNING) << "get port fail, retry, retryCount = " << retryCount;
        if (retryCount == lifeCycleOptions_.portGetRetryTimes) {
            // 达到重试次数，未找到port
            LOG(ERROR) << "get port fail reach portGetRetryTimes = "
                       << lifeCycleOptions_.portGetRetryTimes;
            break;
        }

        usleep(lifeCycleOptions_.portGetRetryIntervalUs);

        // 为下一次循环，加文件锁
        bool boolRet = LockFileWithRetry(fd,
                                    lifeCycleOptions_.fileLockRetryTimes,
                                    lifeCycleOptions_.fileLockRetryIntervalUs);

        if (!boolRet) {
            LOG(ERROR) << "lock file with retry fail, file = "
                       << lifeCycleOptions_.lockFile;
            close(fd);
            return -1;
        }
    }

    // 未成功读取port信息，kill part2
    if (port == 0) {
        bool boolRet = LockFileWithRetry(fd,
                                    lifeCycleOptions_.fileLockRetryTimes,
                                    lifeCycleOptions_.fileLockRetryIntervalUs);

        if (!boolRet) {
            LOG(ERROR) << "lock file with retry fail, file = "
                       << lifeCycleOptions_.lockFile;
            close(fd);
            return -1;
        }

        KillPart2();

        int release = flock(fd, LOCK_UN);
        if (release < 0) {
            LOG(ERROR) << "unlock file fail, file = "
                       << lifeCycleOptions_.lockFile;
            close(fd);
            return -1;
        }

        LOG(ERROR) << "get port fail, kill part2.";
        close(fd);
        return -1;
    }

    part2Port_ = port;

    close(fd);
    LOG(INFO) << "start LifeCycleManager success.";
    return 0;
}

void LifeCycleManager::Stop() {
    LOG(INFO) << "LifeCycleManager STOP start.";
    shouldHeartbeatStop_ = true;
    if (heartbeatThread_ != nullptr && heartbeatThread_->joinable()) {
        heartbeatThread_->join();
        delete heartbeatThread_;
        heartbeatThread_ = nullptr;
    }

    isHeartbeatThreadStart_ = false;

    KillPart2();

    pidPart2_ = -1;
    pidPart1_ = -1;
    part2Port_ = 0;

    LOG(INFO) << "LifeCycleManager STOPED.";

    return;
}

int LifeCycleManager::LoadConf(common::Configuration *conf) {
    if (!conf->GetStringValue("part2ProcName",
                            &lifeCycleOptions_.part2ProcName)) {
        LOG(ERROR) << "get part2ProcName fail.";
        return -1;
    }

    if (!conf->GetStringValue("part2ProcPath",
                            &lifeCycleOptions_.part2ProcPath)) {
        LOG(ERROR) << "get part2ProcPath fail.";
        return -1;
    }

    if (!conf->GetStringValue("part2Addr", &lifeCycleOptions_.part2Addr)) {
        LOG(ERROR) << "get part2Addr fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("part2KillCheckRetryTimes",
                            &lifeCycleOptions_.part2KillCheckRetryTimes)) {
        LOG(ERROR) << "get part2KillCheckRetryTimes fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("part2KillCheckRetryIntervalUs",
                            &lifeCycleOptions_.part2KillCheckRetryIntervalUs)) {
        LOG(ERROR) << "get part2KillCheckRetryIntervalUs fail.";
        return -1;
    }

    if (!conf->GetStringValue("qemuProcName",
                            &lifeCycleOptions_.qemuProcName)) {
        LOG(ERROR) << "get qemuProcName fail.";
        return -1;
    }

    if (!conf->GetStringValue("lockFile", &lifeCycleOptions_.lockFile)) {
        LOG(ERROR) << "get lockFile fail.";
        return -1;
    }

    if (!conf->GetStringValue("metadataPrefix",
                            &lifeCycleOptions_.metadataPrefix)) {
        LOG(ERROR) << "get metadataPrefix fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("part2StartRetryTimes",
                            &lifeCycleOptions_.part2StartRetryTimes)) {
        LOG(ERROR) << "get part2StartRetryTimes fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("connectibleCheckTimes",
                            &lifeCycleOptions_.connectibleCheckTimes)) {
        LOG(ERROR) << "get connectibleCheckTimes fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("connectibleCheckIntervalUs",
                            &lifeCycleOptions_.connectibleCheckIntervalUs)) {
        LOG(ERROR) << "get connectibleCheckIntervalUs fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("portGetRetryTimes",
                            &lifeCycleOptions_.portGetRetryTimes)) {
        LOG(ERROR) << "get portGetRetryTimes fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("portGetRetryIntervalUs",
                            &lifeCycleOptions_.portGetRetryIntervalUs)) {
        LOG(ERROR) << "get portGetRetryIntervalUs fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("heartbeatIntervalUs",
                            &lifeCycleOptions_.heartbeatIntervalUs)) {
        LOG(ERROR) << "get heartbeatIntervalUs fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("fileLockRetryTimes",
                            &lifeCycleOptions_.fileLockRetryTimes)) {
        LOG(ERROR) << "get fileLockRetryTimes fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("fileLockRetryIntervalUs",
                            &lifeCycleOptions_.fileLockRetryIntervalUs)) {
        LOG(ERROR) << "get fileLockRetryIntervalUs fail.";
        return -1;
    }

    LOG(INFO) << "LifeCycleManager load conf success";
    return 0;
}

char *LifeCycleManager::SkipSpace(char *in, int *offset, int len) {
    while (*offset < len) {
        if (isspace(in[*offset]) || in[*offset] == '\0') {
            ++*offset;
            continue;
        }
        break;
    }
    return in + *offset;
}

char *LifeCycleManager::SkipNoSpace(char *in, int *offset, int len) {
    while (*offset < len) {
        if (!isspace(in[*offset]) && in[*offset] != '\0') {
            ++*offset;
            continue;
        }
        break;
    }
    return in + *offset;
}

int LifeCycleManager::getUuid(pid_t pid, const char *procName, char *uuid) {
    char path[kCmdLinePathBufLength];
    snprintf(path, sizeof(path), "/proc/%d/cmdline", pid);
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return -1;
    }

    char line[kCmdLineBufLength];
    int nbytesread = read(fd, line, kCmdLineBufLength);
    close(fd);

    if (strncmp(basename(line), procName, strlen(procName)) != 0) {
        return -1;
    }

    char *p;
    int i = strlen(line);
    bool find = false;
    for (; i < nbytesread; ) {
        p = SkipSpace(line, &i, kCmdLineBufLength);
        if (strncmp(p, "-uuid", 5) == 0) {
            i += strlen(p);
            p = SkipSpace(line, &i, kCmdLineBufLength);
            strncpy(uuid, p, kUuidLength);
            find = true;
            break;
        }
        p = SkipNoSpace(line, &i, kCmdLineBufLength);
    }

    if (find) {
        return 0;
    } else {
        return -1;
    }
}

// 检查uuid和procName对应的进程在不在
// 返回值-1，查找失败；返回值0，查找成功，返回是否查到及pid
int LifeCycleManager::CheckProcAlive(const char* uuid, const char* procName,
                                bool *procExist, pid_t *pid) {
    DIR* dir = opendir("/proc");
    if (dir == NULL) {
        LOG(ERROR) << "opendir(/proc) fail.";
        return -1;
    }

    // 遍历/proc目录下的所有进程
    struct dirent* ent;
    while ((ent = readdir(dir))) {
        if (!isdigit(*ent->d_name)) {
            continue;
        }

        pid_t tempPid = strtol(ent->d_name, NULL, 10);

        char procUuid[kUuidLength + 1] = {0};
        int ret = getUuid(tempPid, procName, procUuid);
        if (ret < 0) {
            continue;
        }

        if (strncmp(procUuid, uuid, strlen(uuid)) == 0) {
            closedir(dir);
            *pid = tempPid;
            *procExist = true;
            DVLOG(6) << "CheckProcAlive alive, uuid = " << uuid
                      << ", procName = " << procName
                      << ", pid = " << tempPid;
            return 0;
        }
    }

    // 遍历 /proc目录下的所有进程，没有找到uuid和procName对应的进程
    closedir(dir);
    *procExist = false;
    LOG(INFO) << "CheckProcAlive proc not alive, uuid = " << uuid
              << ", procName = " << procName;
    return 0;
}

int LifeCycleManager::GetPortFromPart2(uint32_t *port) {
    // 读取json文件
    std::string metadataFile = lifeCycleOptions_.metadataPrefix + qemuUUID_;
    char jsonStr[kMetaFileBufLength] = {};
    int fd = open(metadataFile.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "meta file open failed, " << metadataFile
                   << ", errno = " << errno;
        return -1;
    }
    int ret = read(fd, jsonStr, kMetaFileBufLength);
    if (ret <= 0) {
        LOG(ERROR) << "meta file read failed, " << metadataFile;
        close(fd);
        return -1;
    }
    close(fd);

    // 解析json文件中的port信息
    Json::Reader reader;
    Json::Value value;
    if (!reader.parse(jsonStr, value)) {
        return false;
    }

    if (!value["port"].isNull() && value["port"].isString()) {
        *port = stoi(value["port"].asString());
        LOG(INFO) << "read port from file = " << metadataFile
                  << ", port = " << *port;
        return 0;
    } else {
        return -1;
    }

    return 0;
}

bool LifeCycleManager::IsPart2Connectible() {
    butil::EndPoint ep;
    std::string addr = GetPart2Addr();
    str2endpoint(addr.c_str(), &ep);
    int fd = butil::tcp_connect(ep, NULL);
    if (fd == -1) {
        LOG(WARNING) << "part2 is NOT connectible, addr = " << addr;
        return false;
    }
    close(fd);
    return true;
}

bool LifeCycleManager::IsPart2ConnectibleWithRetry(uint32_t retryTimes,
                                    uint32_t retryIntervalUs) {
    uint32_t retryCount = 0;
    while (retryCount < retryTimes) {
        if (IsPart2Connectible()) {
            return true;
        }

        // 如果检查part2不连通，再看下是否part2已经退出了
        if (!IsPart2Alive()) {
            LOG(WARNING) << "part2 not connectible and part2 not alive.";
            return false;
        }

        retryCount++;
        LOG(WARNING) << "part2 is not connectible, retry next time"
                     << ", unconnectCount = " << retryCount;
        usleep(retryIntervalUs);
    }

    return false;
}

void LifeCycleManager::CleanMetadataFile() {
    std::string metadataFile = lifeCycleOptions_.metadataPrefix + qemuUUID_;
    if (access(metadataFile.c_str(), F_OK) == 0) {
        LOG(INFO) << "CleanMetadataFile, meta file = " << metadataFile;
        if (unlink(metadataFile.c_str()) != 0) {
            LOG(WARNING) << "delete metadata file fail, file = "
                         << metadataFile;
        }
    }

    return;
}

void LifeCycleManager::StartPart2() {
    std::string cmd = lifeCycleOptions_.part2ProcPath + " -uuid " + qemuUUID_;
    LOG(INFO) << "StartPart2, rum cmd: " << cmd;
    system(cmd.c_str());

    return;
}

int LifeCycleManager::KillPart2() {
    if (pidPart2_ == -1) {
        return 0;
    }

    std::string cmd = "kill " + std::to_string(pidPart2_);
    LOG(INFO) << "KillPart2, run cmd: " << cmd;
    system(cmd.c_str());

    // 检查part2是否已经kill
    uint32_t retryCount = 0;
    bool procExist = true;
    pid_t tempPid = 0;
    while (retryCount < lifeCycleOptions_.part2KillCheckRetryTimes) {
        int ret = CheckProcAlive(qemuUUID_.c_str(),
                                 lifeCycleOptions_.part2ProcName.c_str(),
                                 &procExist, &tempPid);
        if (ret == 0 && (!procExist || tempPid != pidPart2_)) {
            pidPart2_ = -1;
            return 0;
        }

        retryCount++;
        LOG(INFO) << "kill part2, part2 still alive, retry"
                  << ", retryCount = " << retryCount;
        usleep(lifeCycleOptions_.part2KillCheckRetryIntervalUs);
    }

    // 多次检查失败，执行kill -9
    if (procExist) {
        std::string cmd = "kill -9 " + std::to_string(pidPart2_);
        LOG(INFO) << "KillPart2, run cmd = " << cmd;
        system(cmd.c_str());
    }

    int ret = CheckProcAlive(qemuUUID_.c_str(),
                                lifeCycleOptions_.part2ProcName.c_str(),
                                &procExist, &tempPid);
    if (ret != 0 || (procExist && tempPid == pidPart2_)) {
        return -1;
    }

    pidPart2_ = -1;
    return 0;
}

int LifeCycleManager::GetPortWithRetry(int fd, uint32_t *port) {
    uint32_t retryCount = 0;
    while (retryCount < lifeCycleOptions_.portGetRetryTimes) {
        // 加文件锁
        int lock = flock(fd, LOCK_EX | LOCK_NB);
        if (lock < 0) {
            LOG(WARNING) << "lock file fail, file = "
                         << lifeCycleOptions_.lockFile;
            retryCount++;
            usleep(lifeCycleOptions_.portGetRetryIntervalUs);
            continue;
        }

        // 重新读取并更新port信息
        uint32_t tempPort = 0;
        int ret = GetPortFromPart2(&tempPort);

        // 文件锁解锁
        int release = flock(fd, LOCK_UN);
        LOG_IF(WARNING, release < 0) << "unlock file fail, file = "
                                     << lifeCycleOptions_.lockFile;

        // 获取port信息失败，下次重试
        if (ret != 0 || tempPort == 0) {
            retryCount++;
            LOG(WARNING) << "update port from part2 fail, retry"
                         << "retryCount" << retryCount;
            usleep(lifeCycleOptions_.portGetRetryIntervalUs);
            continue;
        }

        // 成功获取port信息
        LOG(INFO) << "get port with retry success, port = " << tempPort;
        *port = tempPort;
        return 0;
    }

    // 尝试多次，未能成功获取port信息，返回失败
    return -1;
}

int LifeCycleManager::StartPart2AndGetPortRetry(int fd, uint32_t *port) {
    uint32_t retryCount = 0;
    do {
        StartPart2();
        bool isPart2Alive = false;
        int ret = CheckProcAlive(qemuUUID_.c_str(),
                                 lifeCycleOptions_.part2ProcName.c_str(),
                                 &isPart2Alive, &pidPart2_);
        if (ret < 0) {
            LOG(ERROR) << "check part2 alive fail";
            return -1;
        }

        if (isPart2Alive) {
            uint32_t tempPort;
            int ret = GetPortWithRetry(fd, &tempPort);
            if (ret != 0) {
                LOG(ERROR) << "get port fail";
                return -1;
            }
            *port = tempPort;
            return 0;
        }

        retryCount++;
        LOG(INFO) << "start part2 but check alive fail, retry, retryCount = "
                  << retryCount;

        usleep(lifeCycleOptions_.part2StartRetryIntervalUs);
    } while (retryCount < lifeCycleOptions_.part2StartRetryTimes);

    return -1;
}

void LifeCycleManager::HeartbeatThreadFunc() {
    LOG(INFO) << "heartbeat check thread start.";

    // open文件锁
    int fd = open(lifeCycleOptions_.lockFile.c_str(), O_RDONLY | O_CREAT, 0644);
    if (fd < 0) {
        LOG(ERROR) << "open lock file fail, file = "
                   << lifeCycleOptions_.lockFile;
        return;
    }

    while (!shouldHeartbeatStop_) {
        // 检查part2是否运行
        bool isPart2Alive = false;
        pid_t tempPid = 0;
        int ret = CheckProcAlive(qemuUUID_.c_str(),
                                 lifeCycleOptions_.part2ProcName.c_str(),
                                 &isPart2Alive, &tempPid);
        if (ret < 0) {
            LOG(ERROR) << "kill part1.";
            close(fd);
            exit(1);
            return;
        }

        // 检查part2是否存活，如果未存活，需要拉起。拉起多次失败之后，退出part1。
        if (!isPart2Alive) {
            uint32_t port;
            int ret = StartPart2AndGetPortRetry(fd, &port);
            if (ret < 0) {
                LOG(ERROR) << "kill part1.";
                close(fd);
                exit(1);
                return;
            }

            // 拉起成功之后，读取并更新端口信息。
            part2Port_ = port;
            continue;
        }

        // 检查端口的连通性，如果联通失败，kill part2并重新拉起
        if (!IsPart2ConnectibleWithRetry(
                            lifeCycleOptions_.connectibleCheckTimes,
                            lifeCycleOptions_.connectibleCheckIntervalUs)) {
            LOG(WARNING) << "part2 is not connectible, "
                         << "reach connectibleCheckTimes " << lifeCycleOptions_.connectibleCheckTimes  // NOLINT
                         << " or part2 not alive";

            // 连续多次未联通，kill part2
            int ret = KillPart2();
            if (ret != 0) {
                LOG(WARNING) << "part2 is not connectible, kill part2 fail";
                usleep(lifeCycleOptions_.heartbeatIntervalUs);
                continue;
            }

            uint32_t port;
            ret = StartPart2AndGetPortRetry(fd, &port);
            if (ret < 0) {
                LOG(ERROR) << "kill part1.";
                close(fd);
                exit(1);
                return;
            }

            if (!IsPart2ConnectibleWithRetry(
                            lifeCycleOptions_.connectibleCheckTimes,
                            lifeCycleOptions_.connectibleCheckIntervalUs)) {
                KillPart2();
                LOG(ERROR) << "kill part2, kill part1.";
                close(fd);
                exit(1);
                return;
            }

            part2Port_ = port;
            continue;
        }

        usleep(lifeCycleOptions_.heartbeatIntervalUs);
    }

    LOG(INFO) << "stop heartbeat func.";

    close(fd);
    return;
}

void LifeCycleManager::StartHeartbeat() {
    if (!isHeartbeatThreadStart_) {
        shouldHeartbeatStop_ = false;
        heartbeatThread_ = new std::thread(
                                &LifeCycleManager::HeartbeatThreadFunc,
                                this);
        isHeartbeatThreadStart_ = true;
    }

    return;
}

std::string LifeCycleManager::GetPart2Addr() {
    return lifeCycleOptions_.part2Addr + ":" + std::to_string(part2Port_);
}

bool LifeCycleManager::IsPart2Alive() {
    if (qemuUUID_ == "") {
        return false;
    }

    bool isPart2Alive = false;
    pid_t pidPart2 = -1;
    int ret = CheckProcAlive(qemuUUID_.c_str(),
                                lifeCycleOptions_.part2ProcName.c_str(),
                                &isPart2Alive, &pidPart2);
    if (ret < 0) {
        LOG(ERROR) << "check part2 alive fail";
        return false;
    }

    return isPart2Alive;
}

bool LifeCycleManager::IsHeartbeatThreadStart() {
    return isHeartbeatThreadStart_;
}

bool LifeCycleManager::LockFileWithRetry(int fd, uint32_t retryTimes,
                                  uint32_t retryIntervalUs) {
    uint32_t retryCount = 0;
    while (retryCount < retryTimes) {
        // 加文件锁
        int lock = flock(fd, LOCK_EX | LOCK_NB);
        if (lock < 0) {
            LOG(WARNING) << "lock file fail, file = "
                         << lifeCycleOptions_.lockFile
                         << ", retryCount = " << retryCount;
            retryCount++;
            usleep(retryIntervalUs);
        } else {
            return true;
        }
    }

    return false;
}

}  // namespace client
}  // namespace nebd
