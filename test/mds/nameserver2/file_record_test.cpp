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
    // 设置有效时间为1ms
    FileRecord record(1 * 1000, "0.0.6");

    // 判断超时
    ASSERT_EQ(false, record.IsTimeout());
    // 判断版本号
    ASSERT_EQ("0.0.6", record.GetClientVersion());

    // 睡眠一段时间判断超时是否生效
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    ASSERT_EQ(true, record.IsTimeout());
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

    fileRecordManager.UpdateFileRecord("file1", "");
    fileRecordManager.UpdateFileRecord("file2", "");
    ASSERT_EQ(2, fileRecordManager.GetOpenFileNum());
    std::string v;
    ASSERT_TRUE(fileRecordManager.GetFileClientVersion("file1", &v));
    ASSERT_EQ("", v);
    ASSERT_TRUE(fileRecordManager.GetFileClientVersion("file2", &v));
    ASSERT_EQ("", v);
    ASSERT_FALSE(fileRecordManager.GetFileClientVersion("file3", &v));

    fileRecordManager.UpdateFileRecord("file2", "0.0.6");
    ASSERT_EQ(2, fileRecordManager.GetOpenFileNum());
    ASSERT_TRUE(fileRecordManager.GetFileClientVersion("file1", &v));
    ASSERT_TRUE(fileRecordManager.GetFileClientVersion("file2", &v));
    ASSERT_EQ("0.0.6", v);

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
    fileRecordManager.UpdateFileRecord("file1", "");
    fileRecordManager.UpdateFileRecord("file2", "");

    bool running = true;
    auto task = [&](const std::string& filename) {
        while (running) {
            fileRecordManager.UpdateFileRecord(filename, "");
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
