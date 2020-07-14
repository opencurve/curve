/*
 *     Copyright (c) 2020 NetEase Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/**
 * Project: curve
 * Date: Sun Apr 26 15:54:01 CST 2020
 * Author: wuhanqing
 */

#include "nbd/src/NBDWatchContext.h"
#include <glog/logging.h>

namespace curve {
namespace nbd {

void NBDWatchContext::WatchImageSize() {
    if (started_ == false) {
        started_ = true;
        watchThread_ = std::thread(&NBDWatchContext::WatchFunc, this);
    }
}

void NBDWatchContext::WatchFunc() {
    while (started_) {
        int64_t newSize = image_->GetImageSize();
        if (newSize > 0 && newSize != currentSize_) {
            LOG(INFO) << "image size changed, old size = " << currentSize_
                      << ", new size = " << newSize;
            nbdCtrl_->Resize(newSize);
            currentSize_ = newSize;
        }

        sleeper_.wait_for(std::chrono::seconds(1));
    }
}

void NBDWatchContext::StopWatch() {
    if (started_) {
        started_ = false;
        sleeper_.interrupt();
        watchThread_.join();

        started_ = false;
    }
}

}  // namespace nbd
}  // namespace curve
