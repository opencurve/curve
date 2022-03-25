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

/**
 * Project : curve
 * Created : 2020-03-13
 * Author : wuhanqing
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>    //NOLINT
#include <thread>    // NOLINT

#include "src/common/timeutility.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/file_record.h"

namespace curve {
namespace mds {

TEST(FileRecordTest, timeout_test) {
    butil::EndPoint ep;
    butil::str2endpoint("127.0.0.1:1111", &ep);

    // 设置有效时间为1ms
    FileRecord record(1 * 1000, "0.0.6", ep);

    // 判断超时
    ASSERT_EQ(false, record.IsTimeout());
    // 判断版本号
    ASSERT_EQ("0.0.6", record.GetClientVersion());

    // 睡眠一段时间判断超时是否生效
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    ASSERT_EQ(true, record.IsTimeout());

    auto clientIpPort = record.GetClientEndPoint();
    // ASSERT_EQ(clientIpPort.first, "127.0.0.1");
    // ASSERT_EQ(clientIpPort.second, 1111);
    ASSERT_EQ(butil::endpoint2str(clientIpPort).c_str(),
              std::string("127.0.0.1:1111"));
}

TEST(FileRecordManagerTest, normal_test) {
    FileRecordOptions fileRecordOptions;
    fileRecordOptions.scanIntervalTimeUs = 5 * 1000;
    fileRecordOptions.fileRecordExpiredTimeUs = 20 * 1000;

    FileRecordManager fileRecordManager;
    fileRecordManager.Init(fileRecordOptions);
    fileRecordManager.Start();

    ASSERT_EQ(0, fileRecordManager.GetOpenFileNum());
    ASSERT_EQ(fileRecordOptions.fileRecordExpiredTimeUs,
              fileRecordManager.GetFileRecordExpiredTimeUs());

    fileRecordManager.UpdateFileRecord("file1", "", "127.0.0.1", 1234);
    fileRecordManager.UpdateFileRecord("file2", "0.0.5", "127.0.0.1", 1235);
    ASSERT_EQ(2, fileRecordManager.GetOpenFileNum());
    std::string v;
    ASSERT_TRUE(fileRecordManager.GetMinimumFileClientVersion("file1", &v));
    ASSERT_EQ("", v);
    ASSERT_TRUE(fileRecordManager.GetMinimumFileClientVersion("file2", &v));
    ASSERT_EQ("0.0.5", v);
    ASSERT_FALSE(fileRecordManager.GetMinimumFileClientVersion("file3", &v));

    fileRecordManager.UpdateFileRecord("file2", "0.0.6", "127.0.0.1", 1235);
    ASSERT_EQ(2, fileRecordManager.GetOpenFileNum());
    ASSERT_TRUE(fileRecordManager.GetMinimumFileClientVersion("file1", &v));
    ASSERT_TRUE(fileRecordManager.GetMinimumFileClientVersion("file2", &v));
    ASSERT_EQ("0.0.6", v);

    fileRecordManager.UpdateFileRecord("file3", "0.0.6", "127.0.0.1",
                                       kInvalidPort);
    fileRecordManager.UpdateFileRecord("file4", "0.0.6", "127.0.0.1", 1235);

    // 总共记录了4个文件
    // 其中一个port为Invalid
    // 其中两个文件打开的client ip port相同
    ASSERT_EQ(2, fileRecordManager.ListAllClient().size());

    // ClientIpPortType clientIpPort;
    std::vector<butil::EndPoint> clients;
    ASSERT_TRUE(fileRecordManager.FindFileMountPoint("file1", &clients));
    ASSERT_EQ(1, clients.size());
    // ASSERT_EQ(butil::end, std::string("127.0.0.1"));
    // ASSERT_EQ(clientIpPort.second, 1234);
    ASSERT_EQ(std::string("127.0.0.1:1234"),
              butil::endpoint2str(clients[0]).c_str());

    clients.clear();

    ASSERT_TRUE(fileRecordManager.FindFileMountPoint("file2", &clients));
    ASSERT_EQ(std::string("127.0.0.1:1235"),
              butil::endpoint2str(clients[0]).c_str());

    clients.clear();
    ASSERT_FALSE(
        fileRecordManager.FindFileMountPoint("file100", &clients));

    fileRecordManager.Stop();
}

TEST(FileRecordManagerTest, open_file_num_test) {
    FileRecordOptions fileRecordOptions;
    fileRecordOptions.scanIntervalTimeUs = 1 * 1000;
    fileRecordOptions.fileRecordExpiredTimeUs = 4 * 1000;

    FileRecordManager fileRecordManager;
    fileRecordManager.Init(fileRecordOptions);
    fileRecordManager.Start();

    ASSERT_EQ(0, fileRecordManager.GetOpenFileNum());

    // 插入两个记录
    fileRecordManager.UpdateFileRecord("file1", "", "127.0.0.1", 0);
    fileRecordManager.UpdateFileRecord("file2", "", "127.0.0.1", 0);

    bool running = true;
    auto task = [&](const std::string& filename) {
        while (running) {
            fileRecordManager.UpdateFileRecord(filename, "", "127.0.0.1", 1234);
        }
    };

    // 只对 file1 定期续约
    std::thread th(task, "file1");

    // sleep 50ms后，file2 会超时
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_EQ(1, fileRecordManager.GetOpenFileNum());

    // 停止 file1 的定期续约
    running = false;
    th.join();

    // sleep 50ms后，file1 也会超时
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_EQ(0, fileRecordManager.GetOpenFileNum());

    fileRecordManager.Stop();
}

}  // namespace mds
}  // namespace curve
