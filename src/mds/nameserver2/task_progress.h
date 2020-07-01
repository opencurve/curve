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
 * Created Date: Thursday December 20th 2018
 * Author: hzsunjianliang
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
