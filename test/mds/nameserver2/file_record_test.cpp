/**
 * Project : curve
 * Created : 2020-03-13
 * Author : wuhanqing
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <thread>
#include <chrono>

#include "src/mds/nameserver2/file_record.h"
#include "src/common/timeutility.h"

namespace curve {
namespace mds {

TEST(FileRecordTest, timeout_test) {
    // 设置有效时间为1s
    FileRecord record(1 * 1000 * 1000);

    ASSERT_EQ(false, record.IsTimeout());

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_EQ(true, record.IsTimeout());
}

TEST(FileRecordManagerTest, normal_test) {
    FileRecordOptions fileRecordOptions;
    fileRecordOptions.scanIntervalTimeUs = 500 * 1000;
    fileRecordOptions.fileRecordExpiredTimeUs = 2 * 1000 * 1000;

    FileRecordManager fileRecordManager;
    fileRecordManager.Init(fileRecordOptions);
    fileRecordManager.Start();

    ASSERT_EQ(0, fileRecordManager.GetOpenFileNum());

    fileRecordManager.UpdateFileRecord("file1");
    fileRecordManager.UpdateFileRecord("file2");

    ASSERT_EQ(2, fileRecordManager.GetOpenFileNum());

    fileRecordManager.UpdateFileRecord("file2");
    ASSERT_EQ(2, fileRecordManager.GetOpenFileNum());

    fileRecordManager.Stop();
}

TEST(FileRecordManagerTest, open_file_num_test) {
    FileRecordOptions fileRecordOptions;
    fileRecordOptions.scanIntervalTimeUs = 500 * 1000;
    fileRecordOptions.fileRecordExpiredTimeUs = 2 * 1000 * 1000;


    FileRecordManager fileRecordManager;
    fileRecordManager.Init(fileRecordOptions);
    fileRecordManager.Start();

    ASSERT_EQ(0, fileRecordManager.GetOpenFileNum());

    // 插入两个记录
    fileRecordManager.UpdateFileRecord("file1");
    fileRecordManager.UpdateFileRecord("file2");

    bool running = true;
    auto task = [&](const std::string& filename) {
        while (running) {
            fileRecordManager.UpdateFileRecord(filename);
        }
    };

    // 只对 file1 定期续约
    std::thread th(task, "file1");

    // sleep 3s后，file2 会超时
    std::this_thread::sleep_for(std::chrono::seconds(3));
    ASSERT_EQ(1, fileRecordManager.GetOpenFileNum());

    // 停止 file1 的定期续约
    running = false;
    th.join();

    // sleep 3s后，file1 也会超时
    std::this_thread::sleep_for(std::chrono::seconds(3));
    ASSERT_EQ(0, fileRecordManager.GetOpenFileNum());

    fileRecordManager.Stop();
}

}  // namespace mds
}  // namespace curve
