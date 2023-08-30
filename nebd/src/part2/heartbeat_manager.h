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
 * Project: nebd
 * Created Date: Friday February 14th 2020
 * Author: yangyaokai
 */

#ifndef NEBD_SRC_PART2_HEARTBEAT_MANAGER_H_
#define NEBD_SRC_PART2_HEARTBEAT_MANAGER_H_

#include <bvar/bvar.h>
#include <thread>  // NOLINT
#include <atomic>
#include <memory>
#include <map>
#include <string>

#include "nebd/src/common/interrupt_sleep.h"
#include "nebd/src/common/rw_lock.h"
#include "nebd/src/common/stringstatus.h"
#include "nebd/src/part2/file_manager.h"
#include "nebd/src/part2/define.h"

namespace nebd {
namespace server {

using nebd::common::InterruptibleSleeper;
using nebd::common::RWLock;
using nebd::common::WriteLockGuard;
using nebd::common::ReadLockGuard;

struct HeartbeatManagerOption {
    //File heartbeat timeout (in seconds)
    uint32_t heartbeatTimeoutS;
    //Heartbeat timeout detection thread detection interval (duration: milliseconds)
    uint32_t checkTimeoutIntervalMs;
    //Filemanager object pointer
    NebdFileManagerPtr fileManager;
};

const char kNebdClientMetricPrefix[] = "nebd_client_pid_";
const char kVersion[] = "version";

struct NebdClientInfo {
    NebdClientInfo(int pid2, const std::string& version2,
                   uint64_t timeStamp2) :
                        pid(pid2), timeStamp(timeStamp2) {
        version.ExposeAs(kNebdClientMetricPrefix,
                          std::to_string(pid2) + "_version");
        version.Set(kVersion, version2);
        version.Update();
    }
    //Process number of nebd client
    int pid;
    //The metric of nebd version
    nebd::common::StringStatus version;
    //Time stamp of last heartbeat
    uint64_t timeStamp;
};

//Responsible for managing file heartbeat timeout
class HeartbeatManager {
 public:
    explicit HeartbeatManager(HeartbeatManagerOption option)
        : isRunning_(false)
        , heartbeatTimeoutS_(option.heartbeatTimeoutS)
        , checkTimeoutIntervalMs_(option.checkTimeoutIntervalMs)
        , fileManager_(option.fileManager) {
        nebdClientNum_.expose("nebd_client_num");
    }
    virtual ~HeartbeatManager() {}

    //Start Heartbeat Detection Thread
    virtual int Run();
    //Stop Heartbeat Detection Thread
    virtual int Fini();
    //After receiving the heartbeat, part2 will update the timestamp of the files included in the heartbeat recorded in memory through this interface
    //The heartbeat detection thread will determine whether the file needs to be closed based on this timestamp
    virtual bool UpdateFileTimestamp(int fd, uint64_t timestamp);
    //After receiving the heartbeat, part2 will update the timestamp of part1 through this interface
    virtual void UpdateNebdClientInfo(int pid, const std::string& version,
                                      uint64_t timestamp);
    std::map<int, std::shared_ptr<NebdClientInfo>> GetNebdClients() {
        ReadLockGuard readLock(rwLock_);
        return nebdClients_;
    }

 private:
    //Function execution body of heartbeat detection thread
    void CheckTimeoutFunc();
    //Determine if the file needs to be closed
    bool CheckNeedClosed(NebdFileEntityPtr entity);
    //Delete nebdClientInfo that has timed out from memory
    void RemoveTimeoutNebdClient();

 private:
    //The current running status of heartbeat manager, where true indicates running and false indicates not running
    std::atomic<bool> isRunning_;
    //File heartbeat timeout duration
    uint32_t heartbeatTimeoutS_;
    //Heartbeat timeout detection thread detection time interval
    uint32_t checkTimeoutIntervalMs_;
    //Heartbeat detection thread
    std::thread checkTimeoutThread_;
    //Sleeper for Heartbeat Detection Thread
    InterruptibleSleeper sleeper_;
    //Filemanager object pointer
    NebdFileManagerPtr fileManager_;
    //Information on nebd client
    std::map<int, std::shared_ptr<NebdClientInfo>> nebdClients_;
    //Counters for nebdClient
    bvar::Adder<uint64_t> nebdClientNum_;
    //File map read write protection lock
    RWLock rwLock_;
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_HEARTBEAT_MANAGER_H_
