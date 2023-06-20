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
 * File Created: Monday, 7th January 2019 5:22:52 pm
 * Author: tongguangxun
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fcntl.h>
#include <iostream>

#include "include/client/libcurve.h"
#include "src/client/config_info.h"
#include "src/client/client_common.h"
#include "test/client/fake/fakeMDS.h"

uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 16 * 1024 * 1024;   // NOLINT
std::string mdsMetaServerAddr = "127.0.0.1:6666";   // NOLINT

DECLARE_uint64(test_disk_size);
DEFINE_bool(pre_write, true, "write for test");
DEFINE_bool(fake_mds, true, "create fake mds");
DEFINE_bool(create_copysets, false, "create copysets on chunkserver");

int main(int argc, char ** argv) {
    google::ParseCommandLineFlags(&argc, &argv, false);
    // google::InitGoogleLogging(argv[0]);
    /*** init mds service ***/
    std::string filename = "./1_userinfo_";
    FakeMDS mds(filename);
    if (FLAGS_fake_mds) {
        mds.Initialize();
        mds.StartService();
        if (FLAGS_create_copysets) {
            mds.CreateCopysetNode();
        }
        if (FLAGS_pre_write) {
            std::string configpath = "./client.conf";   // NOLINT
            std::string config = ""\
            "mdsMetaServerAddr=127.0.0.1:6666\n" \
            "metacacheGetLeaderRetry=3\n"\
            "scheduleQueueCapacity=4096\n"\
            "scheduleThreadpoolSize=2\n"\
            "chunkserverOPRetryIntervalUS=200000\n"\
            "chunkserverOPMaxRetry=3\n"\
            "pre_allocate_context_num=1024\n"\
            "fileIOSplitMaxSizeKB=64\n"\
            "logLevel=0";

            int fd_ =  open(configpath.c_str(), O_CREAT | O_RDWR, 0644);
            int len = write(fd_, config.c_str(), config.length());
            close(fd_);

            Init(configpath.c_str());

            C_UserInfo_t userinfo;
            memcpy(userinfo.owner, "userinfo", 9);

            Create(filename.c_str(), &userinfo, FLAGS_test_disk_size);

            sleep(10);

            int fd;
            char* buffer;
            char* readbuffer;
            fd = Open(filename.c_str(), &userinfo);

            if (fd == -1) {
                LOG(FATAL) << "open file failed!";
                return -1;
            }

            buffer = new char[8 * 1024];
            memset(buffer, 'a', 1024);
            memset(buffer + 1024, 'b', 1024);
            memset(buffer + 2 * 1024, 'c', 1024);
            memset(buffer + 3 * 1024, 'd', 1024);
            memset(buffer + 4 * 1024, 'e', 1024);
            memset(buffer + 5 * 1024, 'f', 1024);
            memset(buffer + 6 * 1024, 'g', 1024);
            memset(buffer + 7 * 1024, 'h', 1024);

            uint64_t offset_base;
            for (int i = 0; i < 16; i ++) {
                uint64_t offset = i * chunk_size;
                Write(fd, buffer, offset, 4096);
            }
            unlink(configpath.c_str());
        }
    }

    return 0;
}
