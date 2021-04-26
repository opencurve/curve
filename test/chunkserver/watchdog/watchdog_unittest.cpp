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
 * Created Date: 2021-02-24
 * Author: qinyi
 */

#include <errno.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <string.h>

#include <iostream>

#include "src/chunkserver/watchdog/common.h"
#include "src/chunkserver/watchdog/dog.h"
#include "src/chunkserver/watchdog/dog_keeper.h"
#include "test/chunkserver/chunkserver_test_util.h"
#include "test/chunkserver/watchdog/mock_helper.h"

namespace curve {
namespace chunkserver {

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

const char kStorDir[] = "./watchdog";
const char kTestFile[] = "./watchdog/watchdog.test";
const char kDevice[] = "/dev/watchdog";
const uint32_t kSleepMs = 200;

const WatchConf kDefaultConfig = {kWatchdogCheckPeriod,
                                  kWatchdogCheckTimeout,
                                  kWatchdogLatencyCheckPeriod,
                                  kWatchdogLatencyCheckWindow,
                                  kWatchdogLatencySlowRatio,
                                  kWatchdogLatencyExcessMs,
                                  kWatchdogLatencyAbnormalMs,
                                  kWatchdogKillOnErr,
                                  kWatchdogKillTimeout,
                                  kStorDir,
                                  kDevice};

DECLARE_bool(watchdogKillOnErr);
DECLARE_bool(watchdogRevive);
DECLARE_uint32(watchdogCheckPeriodSec);
DECLARE_uint32(watchdogCheckTimeoutSec);

class WatchdogTest : public ::testing::Test {
 public:
    void SetUp() override {
        Exec(("rm -rf " + kDefaultConfig.storDir).c_str());
        Exec(("mkdir " + kDefaultConfig.storDir).c_str());

        helper_ = std::make_shared<MockWatchdogHelper>();
        dog_ = std::make_shared<Dog>(helper_);
        config_ = kDefaultConfig;
    }

    void TearDown() override {
        LOG(INFO) << "run teardown";
    }

 protected:
    std::shared_ptr<MockWatchdogHelper> helper_;
    std::shared_ptr<Dog> dog_;
    std::shared_ptr<DogKeeper> keeper_;
    WatchConf config_;
};

ACTION_P(GetMem, buf) {
    memcpy(buf, arg1, kWatchdogWriteBufLen);
}

ACTION_P(SetMem, buf) {
    memcpy(arg1, buf, kWatchdogWriteBufLen);
}

ACTION_P(DelayMs, n) {
    std::this_thread::sleep_for(std::chrono::milliseconds(n));
}

ACTION_P(SetErrno, n) {
    errno = n;
}

MATCHER_P(CharsCompareTo, str, "") {
    return strcmp(str, arg) == 0;
}

MATCHER(IsR, "") {
    return (reinterpret_cast<const char*>(arg))[2] == 'R';
}

TEST_F(WatchdogTest, RunWithoutInit) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Run(), -1);
}

TEST_F(WatchdogTest, Smoke) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    InSequence s;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(_))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "Status: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* assume watchdog.test is not existed */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    char buf[kWatchdogWriteBufLen];
    EXPECT_CALL(*helper_, Pwrite(_, IsR(), _, _))
        .Times(1)
        .WillOnce(DoAll(GetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Pread(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Close(_))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(dog_->IsHealthy());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, EmptyConfigForDog) {
    dog_->Init(nullptr);
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _)).Times(0);
    ASSERT_EQ(dog_->Run(), -1);
}

TEST_F(WatchdogTest, EmptyDevice) {
    config_.device = "";
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, OpenDeviceFailed) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    InSequence s;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(_)).Times(0);
    EXPECT_CALL(*helper_, Exec(_, _)).Times(0);
    EXPECT_CALL(*helper_, Access(_, _)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, CloseDeviceFailed) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd = 5;
    InSequence s;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Exec(_, _)).Times(0);
    EXPECT_CALL(*helper_, Access(_, _)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, NoSmartctl) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd = 5;
    InSequence s;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* no smartctl cmd */
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "-bash: smartctl: command not found";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* ensure other checks are OK */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    char buf[kWatchdogWriteBufLen];
    EXPECT_CALL(*helper_, Pwrite(_, IsR(), _, _))
        .Times(1)
        .WillOnce(DoAll(GetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Pread(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Close(_))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsHealthy());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, SmartctlResultEmpty) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd = 5;
    InSequence s;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* smartctl cmd return empty */
    const string smartCmd1 =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "";
    EXPECT_CALL(*helper_, Exec(smartCmd1, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    const string smartCmd2 = "smartctl -s on -H " + config_.device;
    EXPECT_CALL(*helper_, Exec(smartCmd2, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* ensure other checks are OK */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    char buf[kWatchdogWriteBufLen];
    EXPECT_CALL(*helper_, Pwrite(_, IsR(), _, _))
        .Times(1)
        .WillOnce(DoAll(GetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Pread(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Close(_))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsHealthy());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, SmartctlResultFailed) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd = 5;
    InSequence s;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* smartctl cmd return empty */
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: FAILED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* ensure other checks will not run */
    EXPECT_CALL(*helper_, Access(_, _)).Times(0);
    EXPECT_CALL(*helper_, Open(_, _)).Times(0);
    EXPECT_CALL(*helper_, Pwrite(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Pread(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Remove(_)).Times(0);
    EXPECT_CALL(*helper_, Close(_)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, SmartctlResultUnknown) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd = 5;
    InSequence s;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* smartctl cmd return empty */
    const string smartCmd1 =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "Status: XXXX";
    EXPECT_CALL(*helper_, Exec(smartCmd1, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* ensure other checks are OK */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    char buf[kWatchdogWriteBufLen];
    EXPECT_CALL(*helper_, Pwrite(_, IsR(), _, _))
        .Times(1)
        .WillOnce(DoAll(GetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Pread(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Close(_))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsHealthy());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, DelTestFileFailed) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file failed */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();

    /* remain operation will not run */
    EXPECT_CALL(*helper_, Open(_, _)).Times(0);
    EXPECT_CALL(*helper_, Pwrite(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Pread(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Remove(_)).Times(0);
    EXPECT_CALL(*helper_, Close(_)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsHealthy());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, CreateTestFileFailed) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file successfully */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file failed */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();

    /* remain operation will not run */
    EXPECT_CALL(*helper_, Pwrite(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Pread(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Remove(_)).Times(0);
    EXPECT_CALL(*helper_, Close(_)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, WriteTestFileFailed) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();

    /* write test file failed */
    EXPECT_CALL(*helper_, Pwrite(fd, IsR(), _, _))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remain operation will not run */
    EXPECT_CALL(*helper_, Pread(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Remove(_)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, WriteFailedWithEINTRx4) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd1 = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd1))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    int fd2 = 6;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd2))
        .RetiresOnSaturation();

    /* write test file retry with EINTR 4 times, and finally failed */
    EXPECT_CALL(*helper_, Pwrite(fd2, IsR(), _, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetErrno(EINTR), Return(-1)))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Close(fd2)).Times(1).WillOnce(Return(0));

    /* remain operation will not run */
    EXPECT_CALL(*helper_, Pread(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Remove(_)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, WriteFailedWithEINTRx3) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd1 = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd1))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    int fd2 = 6;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd2))
        .RetiresOnSaturation();

    /* write test file retry with EINTR 3 times, and finally OK */
    char buf[kWatchdogWriteBufLen];
    EXPECT_CALL(*helper_, Pwrite(fd2, IsR(), _, _))
        .Times(4)
        .WillOnce(DoAll(SetErrno(EINTR), Return(-1)))
        .WillOnce(DoAll(SetErrno(EINTR), Return(-1)))
        .WillOnce(DoAll(SetErrno(EINTR), Return(-1)))
        .WillOnce(DoAll(GetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    /* read file OK */
    EXPECT_CALL(*helper_, Pread(fd2, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Remove(_))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Close(fd2))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsHealthy());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, WriteFailedAndCloseFailed) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();

    /* write test file failed */
    EXPECT_CALL(*helper_, Pwrite(fd, IsR(), _, _))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();

    /* remain operation will not run */
    EXPECT_CALL(*helper_, Pread(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Remove(_)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, ReadFileReturn0) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd1 = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd1))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    int fd2 = 6;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd2))
        .RetiresOnSaturation();

    /* write file OK */
    EXPECT_CALL(*helper_, Pwrite(fd2, IsR(), _, _))
        .Times(1)
        .WillOnce(ReturnArg<2>())
        .RetiresOnSaturation();

    /*  read return 0, failed */
    EXPECT_CALL(*helper_, Pread(fd2, _, _, _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Remove(_)).Times(0);
    EXPECT_CALL(*helper_, Close(fd2)).Times(1);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, ReadFileFailed) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();

    /* write test file OK */
    EXPECT_CALL(*helper_, Pwrite(_, IsR(), _, _))
        .Times(1)
        .WillOnce(ReturnArg<2>())
        .RetiresOnSaturation();

    /* read test file failed */
    EXPECT_CALL(*helper_, Pread(_, _, _, _))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(0);

    EXPECT_CALL(*helper_, Close(_))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, ReadFailedWithEINTRx4) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd1 = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd1))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    int fd2 = 6;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd2))
        .RetiresOnSaturation();

    /* write test file OK */
    char buf[kWatchdogWriteBufLen];
    EXPECT_CALL(*helper_, Pwrite(fd2, IsR(), _, _))
        .Times(1)
        .WillOnce(DoAll(GetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    /* read file retry with EINTR 4 times, and finally failed */
    EXPECT_CALL(*helper_, Pread(fd2, _, _, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetErrno(EINTR), Return(-1)))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Remove(_)).Times(0);

    EXPECT_CALL(*helper_, Close(fd2))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, ReadFailedWithEINTRx3) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd1 = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd1))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    int fd2 = 6;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd2))
        .RetiresOnSaturation();

    /* write test file OK */
    char buf[kWatchdogWriteBufLen];
    EXPECT_CALL(*helper_, Pwrite(fd2, IsR(), _, _))
        .Times(1)
        .WillOnce(DoAll(GetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    /* read file retry with EINTR 3 times, and finally OK */
    EXPECT_CALL(*helper_, Pread(fd2, _, _, _))
        .Times(4)
        .WillOnce(DoAll(SetErrno(EINTR), Return(-1)))
        .WillOnce(DoAll(SetErrno(EINTR), Return(-1)))
        .WillOnce(DoAll(SetErrno(EINTR), Return(-1)))
        .WillOnce(DoAll(SetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Remove(_))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Close(fd2))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsHealthy());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, ReadFileMismatch) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd1 = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd1))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    int fd2 = 6;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd2))
        .RetiresOnSaturation();

    /* write file OK */
    EXPECT_CALL(*helper_, Pwrite(fd2, IsR(), _, _))
        .Times(1)
        .WillOnce(ReturnArg<2>())
        .RetiresOnSaturation();

    /*  read file OK, but content is not match */
    EXPECT_CALL(*helper_, Pread(fd2, _, _, _))
        .Times(1)
        .WillOnce(ReturnArg<2>())
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Remove(_)).Times(0);
    EXPECT_CALL(*helper_, Close(fd2)).Times(1);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, RemoveTestFileFailed) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd1 = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd1))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    int fd2 = 6;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd2))
        .RetiresOnSaturation();

    /* write file OK */
    char buf[kWatchdogWriteBufLen];
    EXPECT_CALL(*helper_, Pwrite(fd2, IsR(), _, _))
        .Times(1)
        .WillOnce(DoAll(GetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    /*  read file OK */
    EXPECT_CALL(*helper_, Pread(fd2, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    /* remove failed */
    EXPECT_CALL(*helper_, Remove(_))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd2)).Times(1);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, CloseTestFileFailed) {
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd1 = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd1))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    int fd2 = 6;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd2))
        .RetiresOnSaturation();

    /* write file OK */
    char buf[kWatchdogWriteBufLen];
    EXPECT_CALL(*helper_, Pwrite(fd2, IsR(), _, _))
        .Times(1)
        .WillOnce(DoAll(GetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    /*  read file OK */
    EXPECT_CALL(*helper_, Pread(fd2, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetMem(buf), ReturnArg<2>()))
        .RetiresOnSaturation();

    /* remove file OK */
    EXPECT_CALL(*helper_, Remove(_))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* close file failed */
    EXPECT_CALL(*helper_, Close(fd2))
        .Times(1)
        .WillOnce(Return(-1))
        .RetiresOnSaturation();

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, SmartctlTimeout) {
    FLAGS_watchdogCheckTimeoutSec = 5;
    config_.watchdogCheckTimeoutSec = 5;
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    InSequence s;

    int fd = 5;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* smartctl blocked 6 sec, timeout */
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(DelayMs(6000), SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* remain operation will not run */
    EXPECT_CALL(*helper_, Access(_, _)).Times(0);
    EXPECT_CALL(*helper_, Remove(_)).Times(0);
    EXPECT_CALL(*helper_, Open(_, _)).Times(0);
    EXPECT_CALL(*helper_, Pwrite(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Pread(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Remove(_)).Times(0);
    EXPECT_CALL(*helper_, Close(_)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
    ASSERT_TRUE(dog_->IsHealthy());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, DISABLED_WriteFileTimeout) {
    FLAGS_watchdogCheckTimeoutSec = 4;
    config_.watchdogCheckTimeoutSec = 4;
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd1 = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd1))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    int fd2 = 6;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd2))
        .RetiresOnSaturation();

    /* write return 0 for over 100 times (will cause dog timeout) */
    EXPECT_CALL(*helper_, Pwrite(fd2, IsR(), _, _))
        .Times(AtLeast(40))
        .WillRepeatedly(DoAll(DelayMs(100), Return(0)));
    EXPECT_CALL(*helper_, Close(fd2)).Times(1);

    /* remain operation will not run */
    EXPECT_CALL(*helper_, Pread(_, _, _, _)).Times(0);
    EXPECT_CALL(*helper_, Remove(_)).Times(0);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(
        config_.watchdogCheckTimeoutSec * 1000 + 2000));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

TEST_F(WatchdogTest, ReadFileTimeout) {
    FLAGS_watchdogCheckTimeoutSec = 7;
    config_.watchdogCheckTimeoutSec = 7;
    keeper_ = std::make_shared<DogKeeper>(dog_, config_);
    ASSERT_EQ(keeper_->Init(), 0);

    int fd1 = 5;
    InSequence s;

    /* smartctl check ok */
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kDevice), _))
        .Times(1)
        .WillOnce(Return(fd1))
        .RetiresOnSaturation();
    EXPECT_CALL(*helper_, Close(fd1))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();
    const string smartCmd =
        "smartctl -s on -H " + config_.device + " |egrep 'result|Status'";
    string smartInfo = "result: PASSED";
    EXPECT_CALL(*helper_, Exec(smartCmd, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(smartInfo), Return(0)))
        .RetiresOnSaturation();

    /* test file existed  */
    EXPECT_CALL(*helper_,
                Access(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* remove test file OK */
    EXPECT_CALL(*helper_, Remove(CharsCompareTo(kTestFile)))
        .Times(1)
        .WillOnce(Return(0))
        .RetiresOnSaturation();

    /* create test file OK */
    int fd2 = 6;
    EXPECT_CALL(*helper_, Open(CharsCompareTo(kTestFile), _))
        .Times(1)
        .WillOnce(Return(fd2))
        .RetiresOnSaturation();

    /* write file OK */
    EXPECT_CALL(*helper_, Pwrite(fd2, IsR(), _, _))
        .Times(1)
        .WillOnce(ReturnArg<2>())
        .RetiresOnSaturation();

    /*  read delay 8 sec, timeout */
    EXPECT_CALL(*helper_, Pread(fd2, _, _, _))
        .Times(1)
        .WillOnce(DoAll(DelayMs(8000), Return(0)))
        .RetiresOnSaturation();

    EXPECT_CALL(*helper_, Remove(_)).Times(0);
    EXPECT_CALL(*helper_, Close(fd2)).Times(1);

    ASSERT_EQ(keeper_->Run(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(
        config_.watchdogCheckTimeoutSec * 1000 + 2000));
    ASSERT_TRUE(dog_->IsDead());
    keeper_.reset();
    dog_.reset();
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);
    int ret = RUN_ALL_TESTS();

    return ret;
}

}  // namespace chunkserver
}  // namespace curve
