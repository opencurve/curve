/*
 * Project: curve
 * Created Date: Thursday December 20th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_TASK_PROGRESS_H_
#define SRC_MDS_NAMESERVER2_TASK_PROGRESS_H_

#include "src/common/timeutility.h"

namespace curve {
namespace mds {

enum class TaskStatus {
    PROGRESSING,
    FAILED,
    SUCCESS,
};

class TaskProgress {
 public:
    TaskProgress() {
        progress_ = 0;
        status_ = TaskStatus::PROGRESSING;
        startTime_ = ::curve::common::TimeUtility::GetTimeofDayUs();
    }

    void SetProgress(uint32_t persent) {
        progress_ = persent;
    }

    uint32_t GetProgress() const {
        return progress_;
    }

    TaskStatus GetStatus() const {
        return status_;
    }

    void  SetStatus(TaskStatus status) {
        status_ = status;
    }

 private:
    uint32_t progress_;
    TaskStatus status_;
    uint64_t startTime_;
};

}   //  namespace mds
}   //  namespace curve
#endif  //  SRC_MDS_NAMESERVER2_TASK_PROGRESS_H_
