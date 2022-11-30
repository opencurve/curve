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
#include "src/client/client_metric.h"
#include "src/common/concurrent/count_down_event.h"

using curve::client::LatencyGuard;
using curve::common::CountDownEvent;
using curvefs::client::metric::KVClientMetric;

namespace curvefs {
namespace client {

KVClientManager *g_kvClientManager = nullptr;
KVClientMetric *g_kvClientMetric = nullptr;

#define ONRETURN(TYPE, RES, KEY, ERRORLOG)                                     \
    if (RES) {                                                                 \
        g_kvClientMetric->kvClient##TYPE.qps.count << 1;                       \
        VLOG(9) << "##TYPE key = " << KEY << " OK";                            \
    } else {                                                                   \
        g_kvClientMetric->kvClient##TYPE.eps.count << 1;                       \
        LOG(ERROR) << "##TYPE key = " << KEY << " error = " << ERRORLOG;       \
    }

bool KVClientManager::Init(const KVClientManagerOpt &config,
                           const std::shared_ptr<KVClient> &kvclient) {
    client_ = kvclient;
    g_kvClientMetric = new KVClientMetric();
    return threadPool_.Start(config.setThreadPooln) == 0;
}

void KVClientManager::Uninit() {
    client_->UnInit();
    threadPool_.Stop();
    delete g_kvClientMetric;
}

void KVClientManager::Set(std::shared_ptr<SetKVCacheTask> task) {
    threadPool_.Enqueue([task, this]() {
        LatencyGuard guard(&g_kvClientMetric->kvClientSet.latency);

        std::string error_log;
        auto res =
            client_->Set(task->key, task->value, task->length, &error_log);
        ONRETURN(Set, res, task->key, error_log);

        task->done(task);
    });
}

bool KVClientManager::Get(std::shared_ptr<GetKvCacheContext> task) {
    assert(nullptr != task->value);
    return Get(task->key, task->value, task->offset, task->length);
}


bool KVClientManager::Get(const std::string &key, char *value, uint64_t offset,
                          uint64_t length) {
    LatencyGuard guard(&g_kvClientMetric->kvClientGet.latency);

    assert(nullptr != value);
    std::string error_log;
    auto res = client_->Get(key, value, offset, length, &error_log);
    ONRETURN(Get, res, key, error_log);
    return res;
}

}  // namespace client
}  // namespace curvefs
