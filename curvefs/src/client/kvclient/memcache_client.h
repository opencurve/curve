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
 * Created Date: 2022-09-25
 * Author: YangFan (fansehep)
 */

#ifndef CURVEFS_SRC_CLIENT_KVCLIENT_MEMCACHE_CLIENT_H_
#define CURVEFS_SRC_CLIENT_KVCLIENT_MEMCACHE_CLIENT_H_

#include <glog/logging.h>
#include <libmemcached-1.0/memcached.h>
#include <libmemcached-1.0/types/return.h>

#include <cstdint>
#include <memory>
#include <string>

#include "absl/memory/memory.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/client/kvclient/kvclient.h"
#include "curvefs/src/client/metric/client_metric.h"

namespace curvefs {

namespace client {

using curvefs::mds::topology::MemcacheClusterInfo;

/**
 * only the threadpool will operate the kvclient,
 * for threadsafe and fast, we can make every thread has a client.
 */
extern thread_local memcached_st *tcli;

/**
 * MemCachedClient is a client to memcached cluster. You'd better
 * don't use it directly.
 * normal usage example:
 * auto client = std::make_unique<MemCachedClient>();
 *      (make_unique is after c++14)
 * std::string error_log;
 * auto ue = client->AddServer("x.x.x.x", port);
 * ... (the number of your memcached cluster)
 * if (!ue) {...}
 * ue = client->PushServer();
 * if (!ue) {...}
 * uint64_t data;
 * client->SetClientAttr(MEMCACHED_BEHAVIOR_NO_BLOCK, data);
 * client->SetClientAttr(MEMCACHED_BEHAVIOR_TCP_NODELAY, data);
 * KVClientManager manager;
 * config.kvclient = std::move(client_);
 * config.threadPooln = n;
 * manager.Init(&conf);
 * char key[] = "curve";
 * char value[] = "yyds"
 * manager.Set(key, value, strlen(value));
 * ... (don't get it immediately, the kvmanager has a threadpool
 * it will perform tasks asynchronously)
 * std::string res;
 * ue = manager.Get(key, &res);
 * if (!ue) {...}
 * then ...
 * manager.Unint();
 */

class MemCachedClient : public KVClient {
 public:
    MemCachedClient() : server_(nullptr) {
        client_ = memcached_create(nullptr);
    }
    explicit MemCachedClient(memcached_st *cli) : client_(cli) {}
    ~MemCachedClient() { UnInit(); }

    bool Init(const MemcacheClusterInfo& kvcachecluster,
              const std::string& fsName) {
        metric_ = absl::make_unique<metric::MemcacheClientMetric>(fsName);
        client_ = memcached(nullptr, 0);

        for (int i = 0; i < kvcachecluster.servers_size(); i++) {
            if (!AddServer(kvcachecluster.servers(i).ip(),
                           kvcachecluster.servers(i).port())) {
                return false;
            }
        }
        memcached_behavior_set(client_, MEMCACHED_BEHAVIOR_DISTRIBUTION,
                               MEMCACHED_DISTRIBUTION_CONSISTENT);
        memcached_behavior_set(client_, MEMCACHED_BEHAVIOR_RETRY_TIMEOUT, 5);

        return PushServer();
    }

    void UnInit() override {
        if (client_) {
            memcached_free(client_);
            client_ = nullptr;
        }
    }

    bool Set(const std::string& key, const char* value,
             const uint64_t value_len, std::string* errorlog) override {
        uint64_t start = butil::cpuwide_time_us();
        if (nullptr == tcli) {
            tcli = memcached_clone(nullptr, client_);
        }
        auto res = memcached_set(tcli, key.c_str(), key.length(), value,
                                 value_len, 0, 0);
        if (MEMCACHED_SUCCESS == res) {
            VLOG(9) << "Set key = " << key << " OK";
            curve::client::CollectMetrics(&metric_->set, value_len,
                                          butil::cpuwide_time_us() - start);
            return true;
        }
        *errorlog = ResError(res);
        memcached_free(tcli);
        tcli = nullptr;
        LOG(ERROR) << "Set key = " << key << " error = " << *errorlog;
        metric_->set.eps.count << 1;
        return false;
    }

    bool Get(const std::string& key, char* value, uint64_t offset,
             uint64_t length, std::string* errorlog, uint64_t* actLength,
             memcached_return_t* retCode) override {
        uint64_t start = butil::cpuwide_time_us();
        if (nullptr == tcli) {
            // multi thread use a memcached_st* client is unsafe.
            // should clone it or use memcached_st_pool.
            tcli = memcached_clone(nullptr, client_);
        }
        uint32_t flags = 0;
        size_t value_length = 0;
        memcached_return_t ue;
        char *res = memcached_get(tcli, key.c_str(), key.length(),
                                  &value_length, &flags, &ue);
        if (actLength != nullptr) {
            (*actLength) = value_length;
        }
        if (retCode != nullptr) {
            (*retCode) = ue;
        }
        if (MEMCACHED_SUCCESS == ue && res != nullptr && value &&
            value_length >= length) {
            VLOG(9) << "Get key = " << key << " OK";
            memcpy(value, res + offset, length);
            free(res);
            curve::client::CollectMetrics(&metric_->get, value_length,
                                          butil::cpuwide_time_us() - start);
            return true;
        }

        *errorlog = ResError(ue);
        if (ue != MEMCACHED_NOTFOUND) {
          LOG(ERROR) << "Get key = " << key << " error = " << *errorlog
                     << ", get_value_len = " << value_length
                     << ", expect_value_len = " << length;
          memcached_free(tcli);
          tcli = nullptr;
        }

        metric_->get.eps.count << 1;
        return false;
    }

    // transform the res to a error string
    const std::string ResError(const memcached_return_t res) {
        return memcached_strerror(nullptr, res);
    }

    /**
     * @brief: add a remote memcache server to client,
     * this means just add, you must use push after all server add.
     */
    bool AddServer(const std::string &hostname, const uint32_t port) {
        memcached_return_t res;
        server_ =
            memcached_server_list_append(server_, hostname.c_str(), port, &res);
        if (MEMCACHED_SUCCESS == res) {
            return true;
        }
        LOG(ERROR) << "client add " << hostname << " " << port << " error";
        return false;
    }

    /**
     * @brief: push the server list to the client
     */
    bool PushServer() {
        memcached_return_t res = memcached_server_push(client_, server_);
        if (MEMCACHED_SUCCESS == res) {
            return true;
        }
        memcached_server_list_free(server_);
        server_ = nullptr;
        return false;
    }

    /**
     * @return: return this client number of remote servers
     */
    int ServerCount() {
        return static_cast<int>(memcached_server_count(client_));
    }

 private:
    memcached_server_st *server_;
    memcached_st* client_;
    std::unique_ptr<metric::MemcacheClientMetric> metric_;
};

}  //  namespace client
}  //  namespace curvefs
#endif  //  CURVEFS_SRC_CLIENT_KVCLIENT_MEMCACHE_CLIENT_H_
