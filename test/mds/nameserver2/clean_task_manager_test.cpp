/*
 * Project: curve
 * Created Date: Thursday December 20th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <glog/logging.h>
#include "src/mds/nameserver2/clean_task_manager.h"
#include "src/mds/nameserver2/task_progress.h"

namespace curve {
namespace mds {



class SimpleTask  : public Task {
 public:
    explicit SimpleTask(TaskIDType id) {
        SetTaskProgress(TaskProgress());
        SetTaskID(id);
        Rund_ = false;
    }
    void Run(void) override {
        Rund_ = true;
        TaskProgress* progress = GetMutableTaskProgress();
        progress->SetProgress(100);
        progress->SetStatus(TaskStatus::SUCCESS);
        LOG(INFO) << "Simple Task runned, taskID = "<< GetTaskID();
    }
 public:
    bool Rund_;
};

class NthSuccessTask : public Task {
 public:
    NthSuccessTask(TaskIDType id, int Nth) {
        SetTaskProgress(TaskProgress());
        SetTaskID(id);
        RunTimes_ = 0;
        Nth_ = Nth;
    }

    void Run(void) override {
        RunTimes_++;
        if (RunTimes_ != Nth_) {
            TaskProgress* progress = GetMutableTaskProgress();
            progress->SetProgress(0);
            progress->SetStatus(TaskStatus::FAILED);
            return;
        } else {
            TaskProgress* progress = GetMutableTaskProgress();
            progress->SetProgress(100);
            progress->SetStatus(TaskStatus::SUCCESS);
            return;
        }
    }

 public:
    int RunTimes_;
    int Nth_;
};

TEST(CleanTaskManger, SimpleTask) {
    int threadNum = 10;
    int checkPeriod = 1000;
    auto taskManager = new CleanTaskManager(threadNum, checkPeriod);
    TaskIDType taskID = 1;

    // task manager not started, push error
    auto simpleTask = std::make_shared<SimpleTask>(taskID);
    ASSERT_EQ(taskManager->PushTask(simpleTask), false);

    // task manager started, get not found, not push ok
    ASSERT_TRUE(taskManager->Start());
    // wait the check thread to run
    std::this_thread::sleep_for(std::chrono::milliseconds(checkPeriod/4));

    ASSERT_EQ(taskManager->GetTask(taskID), nullptr);
    ASSERT_EQ(taskManager->PushTask(simpleTask), true);

    // task duplicated
    ASSERT_EQ(taskManager->PushTask(simpleTask), false);

    // task have runed
    // wait the thread pool to run
    std::this_thread::sleep_for(std::chrono::milliseconds(checkPeriod/4));
    LOG(INFO) << "check to see if task have runed";
    ASSERT_TRUE(simpleTask->Rund_);
    ASSERT_EQ(simpleTask->GetTaskProgress().GetProgress(), 100);
    ASSERT_EQ(simpleTask->GetTaskProgress().GetStatus(),
        TaskStatus::SUCCESS) << "CASE OK";

    // task have not been removed
    auto task = taskManager->GetTask(taskID);
    ASSERT_TRUE(task != nullptr);
    ASSERT_EQ(task->GetTaskID(), taskID);

    // task have  been removed
    // wait the check thread to run
    std::this_thread::sleep_for(std::chrono::milliseconds(checkPeriod/2));
    std::this_thread::sleep_for(std::chrono::milliseconds(checkPeriod/4));
    task = taskManager->GetTask(taskID);
    ASSERT_TRUE(task == nullptr);

    taskManager->Stop();
}

TEST(CleanTaskManger, NthSuccessTask) {
    int threadNum = 10;
    int checkPeriod = 1000;
    auto taskManager = new CleanTaskManager(threadNum, checkPeriod);
    TaskIDType taskID = 1;
    int Nth = 3;

    // task manager not started, push error
    auto nthSuccessTask = std::make_shared<NthSuccessTask>(taskID, Nth);
    ASSERT_EQ(taskManager->PushTask(nthSuccessTask), false);

    // task manager started, get not found, not push ok
    ASSERT_TRUE(taskManager->Start());
    // wait the check thread to run
    std::this_thread::sleep_for(std::chrono::milliseconds(checkPeriod/4));

    ASSERT_EQ(taskManager->GetTask(taskID), nullptr);
    ASSERT_EQ(taskManager->PushTask(nthSuccessTask), true);

    // task duplicated
    ASSERT_EQ(taskManager->PushTask(nthSuccessTask), false);

    // task have runed
    // wait the thread pool to run
    LOG(INFO) << "check to see if task have runed";
    ASSERT_EQ(1, nthSuccessTask->RunTimes_);
    ASSERT_EQ(nthSuccessTask->GetTaskProgress().GetProgress(), 0);
    ASSERT_EQ(nthSuccessTask->GetTaskProgress().GetStatus(),
        TaskStatus::FAILED);

    std::this_thread::sleep_for(std::chrono::milliseconds(checkPeriod));
    LOG(INFO) << "check to see if task have runed";
    ASSERT_EQ(2, nthSuccessTask->RunTimes_);
    ASSERT_EQ(nthSuccessTask->GetTaskProgress().GetProgress(), 0);
    ASSERT_EQ(nthSuccessTask->GetTaskProgress().GetStatus(),
        TaskStatus::FAILED);

    std::this_thread::sleep_for(std::chrono::milliseconds(checkPeriod));
    LOG(INFO) << "check to see if task have runed";
    ASSERT_EQ(3, nthSuccessTask->RunTimes_);
    ASSERT_EQ(nthSuccessTask->GetTaskProgress().GetProgress(), 100);
    ASSERT_EQ(nthSuccessTask->GetTaskProgress().GetStatus(),
        TaskStatus::SUCCESS);

    std::this_thread::sleep_for(std::chrono::milliseconds(checkPeriod));
    auto task = taskManager->GetTask(taskID);
    ASSERT_TRUE(task == nullptr);

    taskManager->Stop();
}

TEST(CleanTaskManger, SimpleTaskConcurret) {
    int threadNum = 10;
    int checkPeriod = 1000;
    auto taskManager = new CleanTaskManager(threadNum, checkPeriod);

    ASSERT_TRUE(taskManager->Start());

    for (int i = 0; i != 1000; i++) {
        taskManager->PushTask(std::make_shared<SimpleTask>(i));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(checkPeriod/4));
    for (int i = 0; i != 1000; i++) {
        auto tsk = taskManager->GetTask(static_cast<TaskIDType>(i));
        if (tsk == nullptr) {
            continue;
        }
        ASSERT_EQ(tsk->GetTaskID(), static_cast<TaskIDType>(i));
        ASSERT_EQ(tsk->GetTaskProgress().GetStatus(), TaskStatus::SUCCESS);
        ASSERT_EQ(tsk->GetTaskProgress().GetProgress(), 100);
    }
    taskManager->Stop();
}

}  // namespace mds
}  // namespace curve
