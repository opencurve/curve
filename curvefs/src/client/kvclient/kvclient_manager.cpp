/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-10-18
 * Author: lixiaocui
 */

#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include <memory>
#include "src/client/client_metric.h"
#include "src/common/concurrent/count_down_event.h"

using curve::client::LatencyGuard;
using curve::common::CountDownEvent;
using curvefs::client::metric::KVClientMetric;

namespace curvefs {
namespace client {

#define ONRETURN(TYPE, RES)                                                    \
    if (RES) {                                                                 \
        kvClientMetric_.kvClient##TYPE.qps.count << 1;                        \
    } else {                                                                   \
        kvClientMetric_.kvClient##TYPE.eps.count << 1;                        \
    }                                                                          \

bool KVClientManager::Init(const KVClientManagerOpt &config,
                           const std::shared_ptr<KVClient> &kvclient) {
    client_ = kvclient;
    return threadPool_.Start(config.setThreadPooln) == 0;
}

void KVClientManager::Uninit() {
    client_->UnInit();
    threadPool_.Stop();
}

void KVClientManager::Set(std::shared_ptr<SetKVCacheTask> task) {
    threadPool_.Enqueue([task, this]() {
        LatencyGuard guard(&kvClientMetric_.kvClientSet.latency);

        std::string error_log;
        auto res =
            client_->Set(task->key, task->value, task->length, &error_log);
        ONRETURN(Set, res);

        task->done(task);
    });
}

void KVClientManager::Get(std::shared_ptr<GetKVCacheTask> task) {
    threadPool_.Enqueue([task, this]() {
        LatencyGuard guard(&kvClientMetric_.kvClientGet.latency);

        std::string error_log;
        task->res = client_->Get(task->key, task->value, task->offset,
                                task->length, &error_log);
        ONRETURN(Get, task->res);

        task->done(task);
    });
}

}  // namespace client
}  // namespace curvefs
