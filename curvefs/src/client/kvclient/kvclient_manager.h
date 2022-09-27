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
#include "src/common/concurrent/thread_pool.h"
#include "src/common/s3_adapter.h"

namespace curvefs {

namespace client {

using ::curve::common::TaskThreadPool;

struct KvClientManagerConfig {
    std::unique_ptr<KvClient> kvclient;
    int threadPooln;
};

struct SetKvCacheTask;

using SetKvCacheCallBack =
    std::function<void(std::shared_ptr<SetKvCacheTask>)>;

struct SetKvCacheTask {
    const std::string& key;
    const char* value;
    const size_t len;
    SetKvCacheCallBack cb;
    SetKvCacheTask(const std::string& k, const char* val, const size_t length)
        : key(k), value(val), len(length) {}
};

struct GetKvCacheContext {
    const std::string& key;
    std::string* value;
    GetKvCacheContext(const std::string& k, std::string* v)
        : key(k), value(v) {}
};


class KvClientManager {
 public:
    KvClientManager() = default;
    ~KvClientManager() { Uninit(); }

    bool Init(KvClientManagerConfig* config) {
        client_ = std::move(config->kvclient);
        return threadPool_.Start(config->threadPooln) == 0;
    }

    /**
     * close the connection with kv
     */
    void Uninit() {
        client_->UnInit();
        threadPool_.Stop();
    }

    /**
     * It will get a db client and set the key value asynchronusly.
     * The set task will push threadpool, you'd better
     * don't get the key immediately.
     */
    void Set(const std::string& key,
             const char* value,
             const size_t value_len) {
        threadPool_.Enqueue([=]() {
            std::string error_log;
            auto res = client_->Set(key, value, value_len, &error_log);
            if (!res) {
                auto val_view = absl::string_view(value,
                    value_len);
                LOG(ERROR) << "Set key = " << key << " value = " << val_view
                           << " " << "vallen = " << value_len <<
                           " " << error_log;
            }
        });
    }

    void Enqueue(std::shared_ptr<SetKvCacheTask> task) {
        threadPool_.Enqueue([task, this](){
            std::string error_log;
            auto res = client_->Set(task->key, task->value, task->len,
                &error_log);
            if (!res) {
                auto val_view = absl::string_view(task->value,
                    task->len);
                LOG(ERROR) << "Set key = " << task->key << " value = "
                           << val_view << " " << "vallen = " <<
                           task->len << " " << error_log;
                return;
            }
            if (task->cb) {
                task->cb(task);
            }
        });
    }

    bool Get(std::shared_ptr<GetKvCacheContext> task) {
        std::string error_log;
        assert(nullptr != task->value);
        auto res = client_->Get(task->key, task->value, &error_log);
        if (!res) {
            VLOG(9) << "Get Key = " << task->key << " " << error_log;
        }
        return res;
    }

    /**
     * get value by key.
     * the value must be empty.
     */
    bool Get(const std::string& key, std::string* value) {
        std::string error_log;
        auto res = client_->Get(key, value, &error_log);
        if (!res) {
            VLOG(9) << "Get Key = " << key << " " << error_log;
        }
        return res;
    }


 private:
    TaskThreadPool<bthread::Mutex, bthread::ConditionVariable> threadPool_;
    std::unique_ptr<KvClient> client_;
};

}  // namespace client
}  // namespace curvefs
#endif   // CURVEFS_SRC_CLIENT_KVCLIENT_KVCLIENT_MANAGER_H_
