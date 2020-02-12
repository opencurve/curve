/*
 * Project: nebd
 * Created Date: Friday February 14th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include <string>

#include "src/part2/heartbeat_manager.h"
#include "src/part2/filerecord_manager.h"
#include "tests/part2/mock_filerecord_manager.h"
#include "tests/part2/mock_file_manager.h"

using ::testing::_;
using ::testing::Return;

namespace nebd {
namespace server {

using ::testing::_;
using ::testing::AtLeast;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

class HeartbeatManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        fileRecordManager_ = std::make_shared<MockFileRecordManager>();
        fileManager_ = std::make_shared<MockFileManager>();
        HeartbeatManagerOption option;
        option.heartbeatTimeoutS = 10;
        option.checkTimeoutIntervalMs = 1000;
        option.fileManager = fileManager_;
        heartbeatManager_ = std::make_shared<HeartbeatManager>(option);
        EXPECT_CALL(*fileManager_, GetRecordManager())
        .WillRepeatedly(Return(fileRecordManager_));
    }
    std::shared_ptr<MockFileManager>  fileManager_;
    std::shared_ptr<MockFileRecordManager>  fileRecordManager_;
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
};

TEST_F(HeartbeatManagerTest, CheckTimeoutTest) {
    ASSERT_EQ(heartbeatManager_->Run(), 0);
    // 已经在run了不允许重复Run或者Init
    ASSERT_EQ(heartbeatManager_->Run(), -1);

    // 校验是否在检查超时
    uint64_t curTime = TimeUtility::GetTimeofDayMs();
    FileRecordMap fileRecords;
    NebdFileRecord record1;
    NebdFileRecord record2;
    NebdFileRecord record3;
    record1.fd = 1;
    record1.timeStamp = curTime - 2 * 10 * 1000;
    record1.status = NebdFileStatus::OPENED;
    record2.fd = 2;
    record2.timeStamp = curTime - 2 * 10 * 1000;
    record2.status = NebdFileStatus::CLOSED;
    record3.fd = 3;
    record3.timeStamp = curTime;
    record3.status = NebdFileStatus::OPENED;
    fileRecords.emplace(1, record1);
    fileRecords.emplace(2, record2);
    fileRecords.emplace(3, record3);

    EXPECT_CALL(*fileRecordManager_, ListRecords())
    .WillRepeatedly(Return(fileRecords));

    EXPECT_CALL(*fileManager_, Close(1, false))
    .Times(AtLeast(1));
    EXPECT_CALL(*fileManager_, Close(2, false))
    .Times(0);
    EXPECT_CALL(*fileManager_, Close(3, false))
    .Times(0);

    ::sleep(2);
    ASSERT_EQ(heartbeatManager_->Fini(), 0);
    // 重复Fini，也返回成功
    ASSERT_EQ(heartbeatManager_->Fini(), 0);
}


}  // namespace server
}  // namespace nebd

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

