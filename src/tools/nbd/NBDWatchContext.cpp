/**
 * Project: curve
 * Date: Sun Apr 26 15:54:01 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 NetEase
 */

#include "src/tools/nbd/NBDWatchContext.h"
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
        if (newSize >= 0 && newSize != currentSize_) {
            LOG(INFO) << "image size changed, old size = " << currentSize_
                      << ", new size = " << newSize;
            nbdCtrl_->Resize(newSize);
        }

        currentSize_ = newSize;

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
