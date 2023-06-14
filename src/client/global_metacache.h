/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * File Created: 2023-06-15
 * Author: xuchaojie
 */

#ifndef SRC_CLIENT_GLOBAL_METACACHE_H_
#define SRC_CLIENT_GLOBAL_METACACHE_H_

#include "src/client/metacache.h"
#include "src/common/concurrent/rw_lock.h"

namespace curve {
namespace client {

class GlobalMetaCache {
 public:
    static GlobalMetaCache& GetInstance() {
        static GlobalMetaCache gcache;
        return gcache;
    }

    void SetMetaCacheOption(const MetaCacheOption& metaCacheOpt) {
        metaCacheOpt_ = metaCacheOpt;
    }

    MetaCache* GetOrNewMetaCacheInstance(
        const std::string &fileName, 
        const UserInfo_t &userInfo,
        MDSClient* mdsclient);

 private:
    GlobalMetaCache() {}
    ~GlobalMetaCache() {};

    std::map<std::string, MetaCache*> mCacheMap_;
    curve::common::RWLock mCacheMapMutex_;
    MetaCacheOption metaCacheOpt_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_GLOBAL_METACACHE_H_
