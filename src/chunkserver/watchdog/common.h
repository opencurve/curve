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
 * Created Date: 2020-12-11
 * Author: qinyi
 */

#ifndef SRC_CHUNKSERVER_WATCHDOG_COMMON_H_
#define SRC_CHUNKSERVER_WATCHDOG_COMMON_H_

#include <glog/logging.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

namespace curve {
namespace chunkserver {

const bool kWatchdogEnable = true;
const bool kWatchdogKillOnErr = false;
const uint32_t kWatchdogKillTimeout = 30;

const uint32_t kWatchdogCheckPeriod = 5;
const uint32_t kWatchdogCheckTimeout = 30;
const uint32_t kWatchdogSleepFreq = 10;
const uint32_t kWatchdogSleepMs = 100;

const uint32_t kWatchdogLatencyCheckPeriod = 1;
const uint32_t kWatchdogLatencyCheckWindow = 60;
const uint32_t kWatchdogLatencyExcessMs = 16;
const uint32_t kWatchdogLatencyAbnormalMs = 64;
const uint32_t kWatchdogLatencySlowRatio = 20;

const uint32_t kWatchdoDupLogFreq = 128;
const uint32_t kWatchdogTimeIntvMin = 1;
const uint32_t kWatchdogTimeIntvMax = 100;
const uint32_t kWatchdogMaxRatio = 100;
const uint32_t kWatchdogMaxRetry = 5;

const uint32_t kWatchdogExecBufferLen = 4096;
const uint32_t kWatchdogExecMaxBufferLen = 16384;
const uint32_t kWatchdogPathLen = 1024;
const uint32_t kWatchdogCmdBufLen = 4096;
const uint32_t kWatchdogWriteBufLen = 4096;
const uint32_t kWatchdogFileRetryTimes = 3;

using std::string;

enum class DogState {
    DOG_HEALTHY = 0,
    DOG_SICK,
    DOG_DEAD,
};

enum class WatcherName {
    THREAD_WATCHER,
    DISK_SMART_WATCHER,
    FILE_OP_WATCHER,
    FILE_CREATE_WATCHER,
    FILE_STAT_WATCHER,
    FILE_WRITE_WATCHER,
    FILE_READ_WATCHER,
    FILE_RENAME_WATCHER,
    FILE_DEL_WATCHER,
};

struct WatchConf {
    uint32_t watchdogCheckPeriodSec;
    uint32_t watchdogCheckTimeoutSec;
    uint32_t watchdogLatencyCheckPeriodSec;
    uint32_t watchdogLatencyCheckWindow;
    uint32_t watchdogLatencyExcessMs;
    uint32_t watchdogLatencyAbnormalMs;
    uint32_t watchdogLatencyWindowSlowRatio;
    bool watchdogKillOnErr;
    uint32_t watchdogKillTimeoutSec;
    string storDir;
    string device;
};

#define WATCHDOG_UPDATE_CONFIG(conf, x)                                        \
    do                                                                         \
        if (FLAGS_##x != conf.x) {                                             \
            conf.x = FLAGS_##x;                                                \
            LOG(INFO) << "watchdog config refreshed: " << #x << "=" << conf.x; \
        }                                                                      \
    while (0)

void FdCleaner(void* arg);

class PipeGuard {
 public:
    PipeGuard() : fp_(nullptr) {}
    explicit PipeGuard(FILE* fp) : fp_(fp) {}

    ~PipeGuard() {
        if (fp_) {
            ::pclose(fp_);
            fp_ = nullptr;
        }
    }

    operator FILE*() const {
        return fp_;
    }

 private:
    FILE* fp_;
};

class FdGuard {
 public:
    FdGuard() : fd_(-1) {}
    explicit FdGuard(int fd) : fd_(fd) {}

    ~FdGuard() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }
    operator int() const {
        return fd_;
    }

 private:
    // Copying this makes no sense.
    FdGuard(const FdGuard&);
    void operator=(const FdGuard&);

    int fd_;
};

int CheckEnv();
string PathToDeviceName(const string* storDir);
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_WATCHDOG_COMMON_H_
