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

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "absl/strings/string_view.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/kvclient/kvclient.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/common/s3_adapter.h"

using curvefs::client::metric::KVClientManagerMetric;

namespace curvefs {
namespace client {

class KVClientManager;
struct SetKVCacheTask;
struct GetKVCacheTask;

class GetKvCacheContext;
class SetKvCacheContext;

using curve::common::GetObjectAsyncContext;
using curve::common::TaskThreadPool;
using curvefs::client::common::KVClientManagerOpt;

using SetKVCacheDone =
    std::function<void(const std::shared_ptr<SetKVCacheTask>&)>;
using GetKVCacheDone =
    std::function<void(const std::shared_ptr<GetKVCacheTask>&)>;

struct SetKVCacheTask {
    std::string key;
    const char* value;
    uint64_t length;
    bool res;
    SetKVCacheDone done;
    butil::Timer timer;

    explicit SetKVCacheTask(
        const std::string& k, const char* val, const uint64_t len,
        SetKVCacheDone done = [](const std::shared_ptr<SetKVCacheTask>&) {})
        : key(k),
          value(val),
          length(len),
          res(false),
          done(std::move(done)),
          timer(butil::Timer::STARTED) {}
};

struct GetKVCacheTask {
    const std::string& key;
    char* value;
    uint64_t offset;
    uint64_t valueLength;
    uint64_t length;  // actual length of value
    bool res;
    GetKVCacheDone done;
    butil::Timer timer;

    explicit GetKVCacheTask(
        const std::string& k, char* v, uint64_t off, uint64_t len,
        GetKVCacheDone done = [](const std::shared_ptr<GetKVCacheTask>&) {})
        : key(k),
          value(v),
          offset(off),
          valueLength(len),
          length(0),
          res(false),
          done(std::move(done)),
          timer(butil::Timer::STARTED) {}
};

using GetKvCacheCallBack =
    std::function<void(const std::shared_ptr<GetKvCacheContext>&)>;

using SetKvCacheCallBack =
    std::function<void(const std::shared_ptr<SetKvCacheContext>&)>;

struct KvCacheContext {
    std::string key;
    uint64_t inodeId;
    uint64_t offset;
    uint64_t length;
    uint64_t chunkIndex;
    uint64_t chunkPos;
    uint64_t startTime;
};

struct GetKvCacheContext : KvCacheContext {
    char* value;
    bool res;
    GetKvCacheCallBack cb;
};

struct SetKvCacheContext : KvCacheContext {
    const char* value;
    SetKvCacheCallBack cb;
};

class KVClientManager {
 public:
    KVClientManager() = default;
    ~KVClientManager() { Uninit(); }

    bool Init(const KVClientManagerOpt& config,
              const std::shared_ptr<KVClient>& kvclient,
              const std::string& fsName);

    /**
     * It will get a db client and set the key value asynchronusly.
     * The set task will push threadpool, you'd better
     * don't get the key immediately.
     */
    void Set(std::shared_ptr<SetKVCacheTask> task);

    void Get(std::shared_ptr<GetKVCacheTask> task);

    KVClientManagerMetric* GetMetricForTesting() {
        return kvClientManagerMetric_.get();
    }

    void Enqueue(std::shared_ptr<GetObjectAsyncContext> context);

 private:
    void Uninit();
    int GetKvCache(std::shared_ptr<GetObjectAsyncContext> context);

 private:
    TaskThreadPool<bthread::Mutex, bthread::ConditionVariable> threadPool_;
    std::shared_ptr<KVClient> client_;
    std::unique_ptr<KVClientManagerMetric> kvClientManagerMetric_;
};

}  // namespace client
}  // namespace curvefs
#endif  // CURVEFS_SRC_CLIENT_KVCLIENT_KVCLIENT_MANAGER_H_
