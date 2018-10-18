/*
 * Project: curve
 * Created Date: Wednesday August 22nd 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#ifndef SRC_COMMON_TIMEUTILITY_H_
#define SRC_COMMON_TIMEUTILITY_H_

#include <stdint.h>
#include <sys/time.h>
#include <sys/types.h>
#include <vector>

namespace curve {
namespace common {

class TimeUtility{
 public:
    static inline uint64_t GetTimeofDayUs() {
        timeval now;
        gettimeofday(&now, NULL);
        return now.tv_sec * 1000000L + now.tv_usec;
    }
};

}   // namespace common
}   // namespace curve

#endif   // SRC_COMMON_TIMEUTILITY_H_
