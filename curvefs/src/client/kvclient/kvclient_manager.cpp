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

#include "absl/memory/memory.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "src/client/client_metric.h"
#include "src/common/concurrent/count_down_event.h"

using curve::client::LatencyGuard;
using curve::common::CountDownEvent;
using curvefs::client::metric::KVClientManagerMetric;

namespace curvefs {
namespace client {

template <typename Metric, typename TaskSharePtr>
void OnReturn(Metric* metric, const TaskSharePtr task) {
    task->timer.stop();
    if (task->res) {
        curve::client::CollectMetrics(metric, task->length,
                                      task->timer.u_elapsed());
    } else {
        metric->eps.count << 1;
    }
    task->done(task);
}

bool KVClientManager::Init(const KVClientManagerOpt& config,
                           const std::shared_ptr<KVClient>& kvclient,
                           const std::string& fsName) {
    client_ = kvclient;
    kvClientManagerMetric_ = absl::make_unique<KVClientManagerMetric>(fsName);
    return threadPool_.Start(config.setThreadPooln) == 0;
}

void KVClientManager::Uninit() {
    client_->UnInit();
    threadPool_.Stop();
}

void KVClientManager::Set(std::shared_ptr<SetKVCacheTask> task) {
    threadPool_.Enqueue([task, this]() {
        std::string error_log;
        task->res =
            client_->Set(task->key, task->value, task->length, &error_log);
        if (task->res) {
            kvClientManagerMetric_->count << 1;
        }
        OnReturn(&kvClientManagerMetric_->set, task);
    });
}

void UpdateHitMissMetric(memcached_return_t retCode,
                         KVClientManagerMetric* metric) {
    // https://awesomized.github.io/libmemcached/libmemcached/memcached_return_t.html
    switch (retCode) {
        case MEMCACHED_SUCCESS:
            metric->hit << 1;
            break;
        case MEMCACHED_DATA_DOES_NOT_EXIST:
            // The data requested with the key given was not found.
        case MEMCACHED_DATA_EXISTS:
            // The data requested with the key given was not found.
        case MEMCACHED_DELETED:
            // The object requested by the key has been deleted.
        case MEMCACHED_NOTFOUND:
            // The object requested was not found.
            metric->miss << 1;
            break;
        default:
            break;
    }
}

void KVClientManager::Get(std::shared_ptr<GetKVCacheTask> task) {
    threadPool_.Enqueue([task, this]() {
        std::string error_log;
        memcached_return_t retCode;
        task->res = client_->Get(task->key, task->value, task->offset,
                                 task->valueLength, &error_log, &task->length,
                                 &retCode);
        UpdateHitMissMetric(retCode, kvClientManagerMetric_.get());
        OnReturn(&kvClientManagerMetric_->get, task);
    });
}

void KVClientManager::Enqueue(std::shared_ptr<GetObjectAsyncContext> context) {
    auto task = [this, context]() { this->GetKvCache(context); };
    threadPool_.Enqueue(task);
}

int KVClientManager::GetKvCache(
    std::shared_ptr<GetObjectAsyncContext> context) {
    VLOG(9) << "GetKvCache start: " << context->key;
    std::string error_log;
    memcached_return_t retCode;
    uint64_t actLength = 0;
    context->retCode =
        !client_->Get(context->key, context->buf, context->offset, context->len,
                      &error_log, &actLength, &retCode);
    context->actualLen = actLength;
    context->cb(nullptr, context);
    VLOG(9) << "GetKvCache end: " << context->key << ", " << context->retCode
            << ", " << context->actualLen;
    return 0;
}

void KVClientManager::Exist(std::shared_ptr<ExistKVCacheTask> task) {
    threadPool_.Enqueue([task, this]() {
        task->res = client_->Exist(task->key);
        OnReturn(&kvClientManagerMetric_->exist, task);
    });
}

}  // namespace client
}  // namespace curvefs
