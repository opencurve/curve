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
 * Created Date: Wednesday August 22nd 2018
 * Author: hzsunjianliang
 */
#ifndef SRC_COMMON_TIMEUTILITY_H_
#define SRC_COMMON_TIMEUTILITY_H_

#include <stdint.h>
#include <sys/time.h>
#include <sys/types.h>
#include <string>
#include <vector>

namespace curve {
namespace common {

class TimeUtility {
 public:
    static inline uint64_t GetTimeofDayUs() {
        timeval now;
        gettimeofday(&now, NULL);
        return now.tv_sec * 1000000L + now.tv_usec;
    }

    static inline uint64_t GetTimeofDayMs() {
        timeval now;
        gettimeofday(&now, NULL);
        return now.tv_sec * 1000L + now.tv_usec / 1000;
    }

    static inline uint64_t GetTimeofDaySec() {
        timeval tm;
        gettimeofday(&tm, NULL);
        return tm.tv_sec;
    }

    // 时间戳转成标准时间输出在standard里面,时间戳单位为秒
    static inline void TimeStampToStandard(time_t timeStamp,
                                           std::string* standard) {
        char now[64];
        struct tm p;
        p = *localtime_r(&timeStamp, &p);
        strftime(now, 64, "%Y-%m-%d %H:%M:%S", &p);
        *standard = std::string(now);
    }
};

}   // namespace common
}   // namespace curve

#endif   // SRC_COMMON_TIMEUTILITY_H_
