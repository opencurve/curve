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

#include "src/chunkserver/clone_manager.h"

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

class UTCloneTask : public CloneTask {
 public:
    UTCloneTask() : CloneTask(nullptr, nullptr, nullptr), sleepTime_(0) {}
    void Run() {
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime_));
        isComplete_ = true;
    }
    void SetSleepTime(uint32_t sleepTime) { sleepTime_ = sleepTime; }

 private:
    uint32_t sleepTime_;
};

class CloneManagerTest : public testing::Test {
 public:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(CloneManagerTest, BasicTest) {
    CloneOptions options;
    options.checkPeriod = 100;
    CloneManager cloneMgr;
    // If the number of threads is set to 0, starting the thread pool fails
    {
        options.threadNum = 0;
        ASSERT_EQ(cloneMgr.Init(options), 0);
        ASSERT_EQ(cloneMgr.Run(), -1);
    }
    // Queue depth is 0, starting thread pool will fail
    {
        options.threadNum = 5;
        options.queueCapacity = 0;
        ASSERT_EQ(cloneMgr.Init(options), 0);
        ASSERT_EQ(cloneMgr.Run(), -1);
    }
    // If the number of threads and queue depth are both greater than 0, the
    // thread pool can be started
    {
        options.threadNum = 5;
        options.queueCapacity = 100;
        ASSERT_EQ(cloneMgr.Init(options), 0);
        ASSERT_EQ(cloneMgr.Run(), 0);
        // After the thread pool starts running, repeating the run directly
        // returns success
        ASSERT_EQ(cloneMgr.Run(), 0);
    }
    // Pause tasks through Fini
    {
        ASSERT_EQ(cloneMgr.Fini(), 0);
        // Repeated Fini directly returns success
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
    // Testing GenerateCloneTask and IssueCloneTask
    {
        // If options.core is nullptr, the resulting task is also nullptr
        std::shared_ptr<CloneTask> task =
            cloneMgr.GenerateCloneTask(req, nullptr);
        ASSERT_EQ(task, nullptr);

        options.core = std::make_shared<CloneCore>(1, false, nullptr);
        ASSERT_EQ(cloneMgr.Init(options), 0);
        task = cloneMgr.GenerateCloneTask(req, nullptr);
        ASSERT_NE(task, nullptr);

        // Custom task testing
        task = std::make_shared<UTCloneTask>();
        ASSERT_FALSE(task->IsComplete());
        // If the clone manager has not yet started, the task cannot be
        // published
        ASSERT_FALSE(cloneMgr.IssueCloneTask(task));

        // After startup, tasks can be published
        ASSERT_EQ(cloneMgr.Run(), 0);
        ASSERT_TRUE(cloneMgr.IssueCloneTask(task));
        // Wait for a moment, the task execution is completed, check the task
        // status and whether it has been removed from the queue
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        ASSERT_TRUE(task->IsComplete());

        // Unable to publish empty task
        ASSERT_FALSE(cloneMgr.IssueCloneTask(nullptr));
    }
    // Test custom test tasks
    {
        // Initialize tasks with varying execution times
        std::shared_ptr<UTCloneTask> task1 = std::make_shared<UTCloneTask>();
        std::shared_ptr<UTCloneTask> task2 = std::make_shared<UTCloneTask>();
        std::shared_ptr<UTCloneTask> task3 = std::make_shared<UTCloneTask>();
        task1->SetSleepTime(100);
        task2->SetSleepTime(300);
        task3->SetSleepTime(500);
        // Publish all tasks simultaneously
        ASSERT_TRUE(cloneMgr.IssueCloneTask(task1));
        ASSERT_TRUE(cloneMgr.IssueCloneTask(task2));
        ASSERT_TRUE(cloneMgr.IssueCloneTask(task3));
        // At this point, the task is still executing and the reference count is
        // 2
        ASSERT_FALSE(task1->IsComplete());
        ASSERT_FALSE(task2->IsComplete());
        ASSERT_FALSE(task3->IsComplete());
        // Waiting for 220ms, task1 successfully executed, but other tasks have
        // not been completed yet; 220ms basically guarantees the completion of
        // task1 execution
        std::this_thread::sleep_for(std::chrono::milliseconds(220));
        ASSERT_TRUE(task1->IsComplete());
        ASSERT_FALSE(task2->IsComplete());
        ASSERT_FALSE(task3->IsComplete());
        // Wait another 200ms, task2 successfully executed, and task3 is still
        // executing
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        ASSERT_TRUE(task1->IsComplete());
        ASSERT_TRUE(task2->IsComplete());
        ASSERT_FALSE(task3->IsComplete());
        // Wait for another 200ms, all tasks are successfully executed, and all
        // tasks are moved out of the queue
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        ASSERT_TRUE(task1->IsComplete());
        ASSERT_TRUE(task2->IsComplete());
        ASSERT_TRUE(task3->IsComplete());
    }
}

}  // namespace chunkserver
}  // namespace curve
