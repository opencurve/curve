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

#include "src/chunkserver/watchdog/watcher.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>

namespace curve {
namespace chunkserver {

int DiskSmartWatcher::Run() {
    if (config_->device.empty()) {
        LOG(ERROR) << "SMART check failed, device is empty";
        return -1;
    }

    {
        /* test device open */
        int disk = -1;
        disk = helper_->Open(config_->device.c_str(), O_RDWR);
        if (disk < 0) {
            LOG(ERROR) << "SMART check failed, failed to open device "
                       << config_->device << ": " << strerror(errno);
            return -1;
        }
        if (helper_->Close(disk) < 0) {
            LOG(ERROR) << "SMART check failed, failed to close device "
                       << config_->device << ": " << strerror(errno);
            return -1;
        }
    }

    if (!dog_->IsHealthy()) {
        LOG(INFO) << "dog is bad, cancel SMART test";
        return -1;
    }

    {
        /* test SMART */
        std::string cmd =
            "smartctl -s on -H " + config_->device + " |egrep 'result|Status'";
        string output = "";
        int ret = helper_->Exec(cmd, &output);
        if (ret < 0) {
            LOG_EVERY_N(ERROR, kWatchdoDupLogFreq)
                << "SMART check ignored for " << config_->device;
            return 0;
        }

        if (output.empty()) {
            /* print entire smartcl output for debug */
            cmd = "smartctl -s on -H " + config_->device;
            int ret = helper_->Exec(cmd, &output);
            if (ret < 0) {
                LOG_EVERY_N(ERROR, kWatchdoDupLogFreq)
                    << "SMART check ignored for " << config_->device
                    << ", failed to get smartctl entire output";
                return 0;
            }

            LOG_EVERY_N(ERROR, kWatchdoDupLogFreq)
                << "SMART check ignored for " << config_->device
                << ", get unknown smartctl result:" << output;
            return 0;
        }

        if (output.find("OK") != std::string::npos ||
            output.find("PASSED") != std::string::npos) {
            /* SMART test passed */
            return 0;
        } else if (output.find("FAILED") != std::string::npos) {
            /* SMART test failed */
            LOG(ERROR) << "SMART check failed for " << config_->device
                       << ", smartctl result:" << output;
            return -1;
        } else {
            /* unknown test result */
            LOG_EVERY_N(ERROR, kWatchdoDupLogFreq)
                << "SMART check ignored for " << config_->device
                << ", get unknown smartctl result:" << output;
            return 0;
        }
    }

    return 0;
}

int FileOpWatcher::WriteData(int fd, const char* buf, uint64_t offset,
                             int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        if (!dog_->IsHealthy()) {
            LOG(INFO) << "dog is bad, interrupt file write";
            return -1;
        }
        int ret =
            helper_->Pwrite(fd, buf + relativeOffset, remainLength, offset);
        if (ret < 0) {
            if (errno == EINTR && retryTimes < kWatchdogFileRetryTimes) {
                ++retryTimes;
                continue;
            }
            LOG(ERROR) << "pwrite failed: " << strerror(errno);
            return -1;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }

    return 0;
}

int FileOpWatcher::ReadData(int fd, char* buf, uint64_t offset, int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        if (!dog_->IsHealthy()) {
            LOG(INFO) << "dog is bad, interrupt file read";
            return -1;
        }
        int ret =
            helper_->Pread(fd, buf + relativeOffset, remainLength, offset);
        // if offset > file length，pread return 0
        if (ret == 0) {
            LOG(ERROR) << "pread returns zero."
                       << "offset: " << offset << ", length: " << remainLength;
            return -1;
        }
        if (ret < 0) {
            if (errno == EINTR && retryTimes < kWatchdogFileRetryTimes) {
                ++retryTimes;
                continue;
            }
            LOG(ERROR) << "pread failed: " << strerror(errno);
            return -1;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }
    return 0;
}

int FileOpWatcher::Run() {
    string testPath = config_->storDir + "/watchdog.test";

    /* remove existed test file */
    if (!helper_->Access(testPath.c_str(), F_OK) &&
        helper_->Remove(testPath.c_str()) < 0) {
        LOG_EVERY_N(INFO, kWatchdoDupLogFreq)
            << "Unable to delete existed test file: " << testPath << ", "
            << strerror(errno);
        LOG_EVERY_N(INFO, kWatchdoDupLogFreq)
            << "Skip file test in " << config_->storDir;
        return 0;
    }

    if (!dog_->IsHealthy()) {
        LOG(INFO) << "dog is bad, cancel file test";
        return -1;
    }

    /* create test file */
    string writeBuf(kWatchdogWriteBufLen, 'R');
    char readBuf[kWatchdogWriteBufLen] = {0};
    int ret = 0;
    int fd = -1;
    fd =
        helper_->Open(testPath.c_str(), O_CREAT | O_RDWR | O_NOATIME | O_DSYNC);
    if (fd < 0) {
        LOG(ERROR) << "Failed to create test file: " << testPath << ", "
                   << strerror(errno);
        return -1;
    }

    do {
        /* write file */
        ret = WriteData(fd, writeBuf.c_str(), 0, kWatchdogWriteBufLen);
        if (ret < 0) {
            LOG(ERROR) << "Failed to write test file: " << testPath;
            ret = -1;
            break;
        }

        /* read file */
        ret = ReadData(fd, readBuf, 0, kWatchdogWriteBufLen);
        if (ret < 0) {
            LOG(ERROR) << "Failed to read test file: " << testPath;
            ret = -1;
            break;
        }

        if (memcmp(writeBuf.c_str(), readBuf, kWatchdogWriteBufLen)) {
            LOG(ERROR) << "Read invalid content from " << testPath;
            ret = -1;
            break;
        }

        if (!dog_->IsHealthy()) {
            LOG(INFO) << "dog is bad, cancel file remove test";
            ret = -1;
            break;
        }

        if (helper_->Remove(testPath.c_str()) < 0) {
            LOG(ERROR) << "Failed to remove test file: " << testPath << ", "
                       << strerror(errno);
            ret = -1;
            break;
        }
    } while (0);

    if (helper_->Close(fd)) {
        LOG(ERROR) << "Failed to close test file: " << testPath << ", "
                   << strerror(errno);
        return -1;
    }

    /* all file test passed */
    return ret;
}

}  // namespace chunkserver
}  // namespace curve
