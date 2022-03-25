/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Fri Aug 13 17:08:57 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_METASERVICE_CLOSURE_H_
#define CURVEFS_SRC_METASERVER_METASERVICE_CLOSURE_H_

#include <google/protobuf/stubs/callback.h>

#include <memory>

#include "curvefs/src/metaserver/inflight_throttle.h"

namespace curvefs {
namespace metaserver {

// Basic inflight throttle
class MetaServiceClosure : public google::protobuf::Closure {
 public:
    MetaServiceClosure(InflightThrottle* throttle,
                       google::protobuf::Closure* done)
        : throttle_(throttle), rpcDone_(done) {
            throttle_->Increment();
    }

    ~MetaServiceClosure() = default;

    MetaServiceClosure(const MetaServiceClosure&) = delete;
    MetaServiceClosure& operator=(const MetaServiceClosure&) = delete;

    void Run() override {
        std::unique_ptr<MetaServiceClosure> selfGuard(this);
        rpcDone_->Run();
        throttle_->Decrement();
    }

 private:
    InflightThrottle* throttle_;
    google::protobuf::Closure* rpcDone_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_METASERVICE_CLOSURE_H_
