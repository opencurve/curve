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
 * Created Date: Friday March 29th 2019
 * Author: yangyaokai
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <glog/logging.h>
#include <memory>

#include "src/chunkserver/clone_manager.h"
#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

class UTCloneTask : public CloneTask {
 public:
    UTCloneTask() : CloneTask(nullptr, nullptr, nullptr)
                  , sleepTime_(0) {}
    void Run() {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(sleepTime_));
        isComplete_ = true;
    }
    void SetSleepTime(uint32_t sleepTime) {
        sleepTime_ = sleepTime;
    }

 private:
    uint32_t sleepTime_;
};

class CloneManagerTest : public testing::Test  {
 public:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(CloneManagerTest, BasicTest) {
    CloneOptions options;
    options.checkPeriod = 100;
    CloneManager cloneMgr;
    // 如果线程数设置为0,启动线程池失败
    {
        options.threadNum = 0;
        ASSERT_EQ(cloneMgr.Init(options), 0);
        ASSERT_EQ(cloneMgr.Run(), -1);
    }
    // 队列深度为0，启动线程池会失败
    {
        options.threadNum = 5;
        options.queueCapacity = 0;
        ASSERT_EQ(cloneMgr.Init(options), 0);
        ASSERT_EQ(cloneMgr.Run(), -1);
    }
    // 线程数和队列深度都大于0，可以启动线程池
    {
        options.threadNum = 5;
        options.queueCapacity = 100;
        ASSERT_EQ(cloneMgr.Init(options), 0);
        ASSERT_EQ(cloneMgr.Run(), 0);
        // 线程池启动运行后，重复Run直接返回成功
        ASSERT_EQ(cloneMgr.Run(), 0);
    }
    // 通过Fini暂停任务
    {
        ASSERT_EQ(cloneMgr.Fini(), 0);
        // 重复Fini直接返回成功
        ASSERT_EQ(cloneMgr.Fini(), 0);
    }
}

TEST_F(CloneManagerTest, TaskTest) {
    // run clone manager
    CloneOptions options;
    CloneManager cloneMgr;
    options.threadNum = 5;
    options.queueCapacity = 100;
    options.checkPeriod = 100;
    ASSERT_EQ(cloneMgr.Init(options), 0);

    std::shared_ptr<ReadChunkRequest> req =
        std::make_shared<ReadChunkRequest>();
    // 测试 GenerateCloneTask 和 IssueCloneTask
    {
        // options.core为nullptr，则产生的任务也是nullptr
        std::shared_ptr<CloneTask> task =
            cloneMgr.GenerateCloneTask(req, nullptr);
        ASSERT_EQ(task, nullptr);

        options.core = std::make_shared<CloneCore>(1, false, nullptr);
        ASSERT_EQ(cloneMgr.Init(options), 0);
        task = cloneMgr.GenerateCloneTask(req, nullptr);
        ASSERT_NE(task, nullptr);

        // 自定义任务测试
        task = std::make_shared<UTCloneTask>();
        ASSERT_FALSE(task->IsComplete());
        // 如果clone manager还未启动，则无法发布任务
        ASSERT_FALSE(cloneMgr.IssueCloneTask(task));

        // 启动以后就可以发布任务
        ASSERT_EQ(cloneMgr.Run(), 0);
        ASSERT_TRUE(cloneMgr.IssueCloneTask(task));
        // 等待一点时间，任务执行完成，检查任务状态以及是否从队列中移除
        std::this_thread::sleep_for(
            std::chrono::milliseconds(200));
        ASSERT_TRUE(task->IsComplete());

        // 无法发布空任务
        ASSERT_FALSE(cloneMgr.IssueCloneTask(nullptr));
    }
    // 测试自定义的测试任务
    {
        // 初始化执行时间各不相同的任务
        std::shared_ptr<UTCloneTask> task1 = std::make_shared<UTCloneTask>();
        std::shared_ptr<UTCloneTask> task2 = std::make_shared<UTCloneTask>();
        std::shared_ptr<UTCloneTask> task3 = std::make_shared<UTCloneTask>();
        task1->SetSleepTime(100);
        task2->SetSleepTime(300);
        task3->SetSleepTime(500);
        // 同时发布所有任务
        ASSERT_TRUE(cloneMgr.IssueCloneTask(task1));
        ASSERT_TRUE(cloneMgr.IssueCloneTask(task2));
        ASSERT_TRUE(cloneMgr.IssueCloneTask(task3));
        // 此时任务还在执行中，此时引用计数为2
        ASSERT_FALSE(task1->IsComplete());
        ASSERT_FALSE(task2->IsComplete());
        ASSERT_FALSE(task3->IsComplete());
        // 等待220ms，task1执行成功，其他还没完成;220ms基本可以保证task1执行完
        std::this_thread::sleep_for(
            std::chrono::milliseconds(220));
        ASSERT_TRUE(task1->IsComplete());
        ASSERT_FALSE(task2->IsComplete());
        ASSERT_FALSE(task3->IsComplete());
        // 再等待200ms，task2执行成功，task3还在执行中
        std::this_thread::sleep_for(
            std::chrono::milliseconds(200));
        ASSERT_TRUE(task1->IsComplete());
        ASSERT_TRUE(task2->IsComplete());
        ASSERT_FALSE(task3->IsComplete());
        // 再等待200ms，所有任务执行成功，任务全被移出队列
        std::this_thread::sleep_for(
            std::chrono::milliseconds(200));
        ASSERT_TRUE(task1->IsComplete());
        ASSERT_TRUE(task2->IsComplete());
        ASSERT_TRUE(task3->IsComplete());
    }
}

}  // namespace chunkserver
}  // namespace curve
