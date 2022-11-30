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
 * Created Date: 2022-09-23
 * Author: YangFan (fansehep)
 */

#ifndef CURVEFS_SRC_CLIENT_KVCLIENT_KVCLIENT_MANAGER_H_
#define CURVEFS_SRC_CLIENT_KVCLIENT_KVCLIENT_MANAGER_H_

#include <bthread/condition_variable.h>

#include <thread>
#include <memory>
#include <utility>
#include <string>

#include "absl/strings/string_view.h"
#include "curvefs/src/client/kvclient/kvclient.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "src/common/concurrent/thread_pool.h"
#include "src/common/s3_adapter.h"

using curvefs::client::metric::KVClientMetric;

namespace curvefs {
namespace client {

class KVClientManager;
class SetKVCacheTask;
using curve::common::TaskThreadPool;
using curvefs::client::common::KVClientManagerOpt;

extern KVClientManager *g_kvClientManager;
extern KVClientMetric *g_kvClientMetric;

typedef std::function<void(const std::shared_ptr<SetKVCacheTask> &)>
    SetKVCacheDone;

struct SetKVCacheTask {
    std::string key;
    const char *value;
    uint64_t length;
    SetKVCacheDone done;
    SetKVCacheTask() = default;
    SetKVCacheTask(const std::string &k, const char *val, const uint64_t len)
        : key(k), value(val), length(len) {}
};

struct GetKvCacheContext {
    const std::string &key;
    char *value;
    uint64_t offset;
    uint64_t length;
    GetKvCacheContext(const std::string &k, char *v, uint64_t off, uint64_t len)
        : key(k), value(v), offset(off), length(len) {}
};

class KVClientManager {
 public:
    KVClientManager() = default;
    ~KVClientManager() { Uninit(); }

    bool Init(const KVClientManagerOpt &config,
              const std::shared_ptr<KVClient> &kvclient);

    /**
     * It will get a db client and set the key value asynchronusly.
     * The set task will push threadpool, you'd better
     * don't get the key immediately.
     */
    void Set(std::shared_ptr<SetKVCacheTask> task);

    bool Get(const std::string &key, char *value, uint64_t offset,
             uint64_t length);

    bool Get(std::shared_ptr<GetKvCacheContext> task);

 private:
    void Uninit();

 private:
    TaskThreadPool<bthread::Mutex, bthread::ConditionVariable> threadPool_;
    std::shared_ptr<KVClient> client_;
};

}  // namespace client
}  // namespace curvefs
#endif   // CURVEFS_SRC_CLIENT_KVCLIENT_KVCLIENT_MANAGER_H_
