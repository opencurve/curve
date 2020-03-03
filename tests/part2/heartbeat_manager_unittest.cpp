/*
 * Project: nebd
 * Created Date: Friday February 14th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include <string>

#include "src/part2/heartbeat_manager.h"
#include "tests/part2/mock_file_entity.h"
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
        fileManager_ = std::make_shared<MockFileManager>();
        HeartbeatManagerOption option;
        option.heartbeatTimeoutS = 10;
        option.checkTimeoutIntervalMs = 1000;
        option.fileManager = fileManager_;
        heartbeatManager_ = std::make_shared<HeartbeatManager>(option);
    }
    std::shared_ptr<MockFileManager>  fileManager_;
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
};

TEST_F(HeartbeatManagerTest, CheckTimeoutTest) {
    ASSERT_EQ(heartbeatManager_->Run(), 0);
    // 已经在run了不允许重复Run或者Init
    ASSERT_EQ(heartbeatManager_->Run(), -1);

    // 构造file entity
    uint64_t curTime = TimeUtility::GetTimeofDayMs();
    std::shared_ptr<MockFileEntity> entity1 =
        std::make_shared<MockFileEntity>();
    std::shared_ptr<MockFileEntity> entity2 =
        std::make_shared<MockFileEntity>();
    std::shared_ptr<MockFileEntity> entity3 =
        std::make_shared<MockFileEntity>();
    EXPECT_CALL(*entity1, GetFileTimeStamp())
    .WillRepeatedly(Return(curTime - 2 * 10 * 1000));
    EXPECT_CALL(*entity1, GetFileStatus())
    .WillRepeatedly(Return(NebdFileStatus::OPENED));
    EXPECT_CALL(*entity2, GetFileTimeStamp())
    .WillRepeatedly(Return(curTime - 2 * 10 * 1000));
    EXPECT_CALL(*entity2, GetFileStatus())
    .WillRepeatedly(Return(NebdFileStatus::CLOSED));
    EXPECT_CALL(*entity3, GetFileTimeStamp())
    .WillRepeatedly(Return(curTime));
    EXPECT_CALL(*entity3, GetFileStatus())
    .WillRepeatedly(Return(NebdFileStatus::OPENED));

    // 构造file map
    FileEntityMap entityMap;
    entityMap.emplace(1, entity1);
    entityMap.emplace(2, entity2);
    entityMap.emplace(3, entity3);
    EXPECT_CALL(*fileManager_, GetFileEntityMap())
    .WillRepeatedly(Return(entityMap));

    // 预期结果
    EXPECT_CALL(*entity1, Close(false))
    .Times(AtLeast(1));
    EXPECT_CALL(*entity2, Close(false))
    .Times(0);
    EXPECT_CALL(*entity3, Close(false))
    .Times(0);

    ::sleep(2);
    ASSERT_EQ(heartbeatManager_->Fini(), 0);
    // 重复Fini，也返回成功
    ASSERT_EQ(heartbeatManager_->Fini(), 0);
}

TEST_F(HeartbeatManagerTest, UpdateTimeStampTest) {
    std::shared_ptr<MockFileEntity> entity = std::make_shared<MockFileEntity>();

    EXPECT_CALL(*fileManager_, GetFileEntity(1))
    .WillOnce(Return(entity));
    EXPECT_CALL(*entity, UpdateFileTimeStamp(100))
    .Times(1);
    ASSERT_TRUE(heartbeatManager_->UpdateFileTimestamp(1, 100));

    EXPECT_CALL(*fileManager_, GetFileEntity(1))
    .WillOnce(Return(nullptr));
    ASSERT_FALSE(heartbeatManager_->UpdateFileTimestamp(1, 100));
}

}  // namespace server
}  // namespace nebd

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

