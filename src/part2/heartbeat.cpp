/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#include "src/part2/heartbeat.h"
#include <assert.h>
#include <butil/logging.h>
#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstdint>
#include <map>
#include <string>
#include <vector>
#include "src/part2/common_type.h"
#include "src/part2/config.h"
#include "src/part2/interrupt_sleep.h"
#include "src/part2/rados_interface.h"

static int g_check_times = 0;
static int g_check_detached_times = 0;
static const char* g_proc_name = "qemu-system-x86_64";
static int64_t g_last_received(0);
static bool heartbeat_check_first = true;
static InterruptibleSleeper sleeper;
/**
 *@brief 关闭已卸载卷线程
 *@detail
 *若有已卸载卷，那么检查detacheedTimes次后，卸载
 */
void* CloseDetachedVolumesProcess(void*) {
    LOG(NOTICE) << "close detached volumes process start.";
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return NULL;
    }
    int checkdetached_interval = ReadCheckDetachedInterval(cfg);
    int detachedTimes = ReadDetachedTimes(cfg);
    config_destroy(cfg);
    sleep(checkdetached_interval);
    CloseQemuDetachedVolumes(detachedTimes);
    LOG(NOTICE) << "close detached volumes process end.";
    return NULL;
}
/**
 * @brief 心跳线程
 * @detail
 * 每隔heartbeat_interval秒检查一次qemu进程，
 * 若assert_times检查后，qemu进程依然不在，那么自杀
 */
void* HeartbeatProcess(void*) {
    int ret = 0;
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        return NULL;
    }
    int heartbeat_interval = ReadHeartbeatInterval(cfg);
    int check_assert_times = ReadAssertTimes(cfg);
    config_destroy(cfg);
    while (sleeper.wait_for(
        std::chrono::milliseconds(1000 * heartbeat_interval))) {
        ret = CheckProc(g_uuid);
        if (ret < 0) {
            if (++g_check_times < check_assert_times) {
                LOG(ERROR) << "donnt stop, check_time is: " << g_check_times
                           << ", "
                           << "check_assert_times is: " << check_assert_times;
                continue;
            }
            LOG(ERROR) << "qemu proc is stop, so stop myself";
            StopProc(false);
        } else {
            g_check_times = 0;
            if (requestForRpcWriteCounts > 0) {
                int64_t last_received;
                last_received = requestForRpcWriteCounts - g_last_received;
                g_last_received = requestForRpcWriteCounts;
                LOG(NOTICE) << "qemu proc is running, "
                            << "rpc inflight: " << requestForRpcWrite
                            << ", ceph inflight: " << requestForCephWrite
                            << ", last received: " << last_received;
            } else {
                requestForRpcWriteCounts = 0;
                g_last_received = 0;
                LOG(NOTICE) << "rpc write counts is zero or has overflowed.";
            }
        }
    }
    LOG(ERROR) << "the heartbeat is over.";
    return NULL;
}

/**
 * @brief 响应SIGINT以及SIGTERM信号，客户端升级使用
 */
void SigProcess(int sig_no) {
    LOG(ERROR) << "signal has been received, " << sig_no;
    StopProc(true);
}

/**
 * @brief 响应SIGHUP信号，日志回滚使用
 */
void SigLogReplace(int sig_no) {
    LOG(WARNING) << "signal has been received, " << sig_no;
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    std::string log_path = ReadLogPath(cfg);
    std::string log_file = log_path + "/" + g_uuid + ".log";
    logging::LoggingSettings log;
    log.logging_dest = logging::LOG_TO_FILE;
    log.log_file = log_file.c_str();
    logging::InitLogging(log);
    int loglevel = ReadLogLevel(cfg);
    logging::SetMinLogLevel(loglevel);

    LOG(WARNING) << "log has been logrotated.";
    config_destroy(cfg);
}

int StopProc(bool need_check_qemu_proc) {
    bool remove_file = true;
    std::string uuid_file = GetUuidFile();
    std::string uuid_lockfile = GetUuidLockfile();

    int lockfd = LockFile(uuid_lockfile.c_str());
    if (lockfd < 0) {
        LOG(ERROR) << "add lock failed.";
        return -1;
    }

    std::map<int, FdImage_t*>::iterator iter;
    for (iter = g_imageMap.begin(); iter != g_imageMap.end(); iter++) {
        if (iter->first <= 0 || iter->first > FD_CEPH_MAX) {
            LOG(ERROR) << "fd is abnormal. " << iter->first;
            continue;
        }
        int ret = CloseImage(iter->first);
        if (ret < 0) {
            LOG(ERROR) << "close image failed.";
            UnlockFile(lockfd);
            return -1;
        }
    }

    if (need_check_qemu_proc) {
        int ret = CheckProc(g_uuid);
        if (ret >= 0) remove_file = false;
    }

    if (remove_file) {
        if (remove(uuid_file.c_str()) == 0) {
            LOG(ERROR) << "remove uuid file suc. " << uuid_file;
        } else {
            LOG(ERROR) << "remove uuid file failed. " << uuid_file;
        }
    }
    UnlockFile(lockfd);

    sleeper.interrupt();
    return 0;
}

int CloseQemuDetachedVolumes(int detachedTimes) {
    LOG(NOTICE) << "close detached volumes start.";
    if (ReadQemuXmls() < 0) {
        LOG(NOTICE) << "read qrmu xmls failed.";
        return -1;
    }
    std::string uuid_lockfile = GetUuidLockfile();
    std::vector<std::string> fd_pool_vols;
    std::map<std::string, std::vector<std::string>>::iterator iter;

    // 遍历image_map，关闭所有qemu已经关闭的卷
    std::map<int, FdImage_t*>::iterator iter_fd;
    for (iter_fd = g_imageMap.begin(); iter_fd != g_imageMap.end(); iter_fd++) {
        char* filename = iter_fd->second->filename;
        std::vector<std::string> split_firstly = split(filename, ":");
        if (split_firstly.empty()) continue;
        std::string pool_vol;
        pool_vol = split_firstly[1];
        if (pool_vol.empty()) continue;

        LOG(ERROR) << "change, pool and vol is: " << pool_vol;
        fd_pool_vols.push_back(pool_vol);
        std::vector<std::string>::iterator ret;
        ret =
            find(g_qemuPoolVolumes.begin(), g_qemuPoolVolumes.end(), pool_vol);
        if (ret == g_qemuPoolVolumes.end()) {
            if (++g_check_detached_times < detachedTimes) {
                LOG(ERROR) << "volume has not been found, " << pool_vol
                           << ", no close now, " << g_check_detached_times
                           << " less " << detachedTimes;
                bthread_t th;
                if (bthread_start_background(
                        &th, NULL, CloseDetachedVolumesProcess, NULL) != 0) {
                    LOG(ERROR) << "Fail to create bthread";
                    return -1;
                }
                LOG(NOTICE) << "no volume has been detached.";
                return 0;
            }
            LOG(ERROR) << "volume has not been found, detach now: " << pool_vol;
            int lockfd = LockFile(uuid_lockfile.c_str());
            if (lockfd < 0) {
                LOG(ERROR) << "add lock failed.";
                return -1;
            }

            if (CloseImage(iter_fd->first) < 0) {
                LOG(ERROR) << "close image failed.";
                UnlockFile(lockfd);
                return -1;
            }
            if (RmFd(iter_fd->first) < 0) {
                LOG(ERROR) << "rm fd failed.";
                UnlockFile(lockfd);
                return -1;
            }
            UnlockFile(lockfd);
        } else {
            LOG(NOTICE) << "volume has been found. " << pool_vol;
        }
    }

    // 本次检查结束，清空vector
    g_qemuPoolVolumes.clear();
    LOG(NOTICE) << "close detached volumes end.";
    return 0;
}

char* SkipSpace(char* in, int* offset, int len) {
    while (*offset < len) {
        if (isspace(in[*offset]) || in[*offset] == '\0') {
            ++*offset;

            continue;
        }
        break;
    }
    return in + *offset;
}

int CheckCmdline(int64_t pid, const char* proc_name, char* uuid, int uuid_len) {
    int i = 0;
    char path[32], line[2048], *p;

    snprintf(path, sizeof(path), "/proc/%ld/cmdline", pid);

    int cmdline = open(path, O_RDONLY);
    if (!cmdline) return -1;

    int nbytesread = read(cmdline, line, 2048);
    close(cmdline);

    LOG(INFO) << "name is " << basename(line) << " " << proc_name << " "
              << strlen(proc_name);

    if (strncmp(basename(line), proc_name, strlen(proc_name)) != 0) return -1;

    i += strlen(line);

    for (; i < nbytesread; i++) {
        p = SkipSpace(line, &i, nbytesread);

        if (strncmp(p, "-uuid", 5) == 0) {
            i += strlen(p);
            p = SkipSpace(line, &i, nbytesread);
            strncpy(uuid, p, uuid_len);
            return 0;
        }
        i += strlen(p);
    }
    return -1;
}
/**
 * @brief 检查qemu进程是否存在
 */
int CheckProc(const char* uuid) {
    int64_t pid;
    char uuid_check[64];
    int r;
    if (g_oldPid < 0) {
        // get pid from proc name
        DIR* proc = opendir("/proc");
        struct dirent* ent;

        if (proc == NULL) {
            LOG(ERROR) << "open dir failed.";
            return -1;
        }

        while ((ent = readdir(proc))) {
            if (!isdigit(*ent->d_name)) continue;

            pid = strtol(ent->d_name, NULL, 10);

            LOG(INFO) << "pid is " << pid;

            r = CheckCmdline(pid, g_proc_name, uuid_check, strlen(uuid));
            if (r >= 0) {
                if (strncmp(uuid_check, uuid, strlen(uuid)) == 0) {
                    if (!heartbeat_check_first) {
                        LOG(NOTICE) << "pid change, close detached volumes.";
                        // 查看挂载的卷是否发生变化--卸载所有已变化的卷
                        bthread_t th;
                        if (bthread_start_background(
                                &th, NULL, CloseDetachedVolumesProcess, NULL) !=
                            0) {
                            LOG(ERROR) << "Fail to create bthread";
                            return -1;
                        }
                        // heartbeat_check_first = true;
                    }
                    LOG(NOTICE) << "qemu pid has changed, new pid is: " << pid;
                    g_oldPid = pid;
                    closedir(proc);
                    return 0;
                }
            }
        }

        g_oldPid = -1;  // reset old pid of checking proc
        closedir(proc);
        return -1;
    }
    // proc下面进程文件存在，则表示进程还存在
    char proc_file[32];
    snprintf(proc_file, sizeof(proc_file), "/proc/%ld", g_oldPid);
    if ((access(proc_file, F_OK)) != -1) {
        heartbeat_check_first = false;
        return 0;
    }
    LOG(NOTICE) << "reset old pid.";
    g_oldPid = -1;  // reset old pid of checking proc
    return -1;
}
