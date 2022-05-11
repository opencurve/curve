/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_TEST_CLIENT_MOCK_DENTRY_CACHE_MAMAGER_H_
#define CURVEFS_TEST_CLIENT_MOCK_DENTRY_CACHE_MAMAGER_H_

#include <gmock/gmock.h>

#include <string>
#include <list>

#include "curvefs/src/client/dentry_cache_manager.h"

namespace curvefs {
namespace client {

class MockDentryCacheManager : public DentryCacheManager {
 public:
    MockDentryCacheManager() {}
    ~MockDentryCacheManager() {}

    MOCK_METHOD2(Init, CURVEFS_ERROR(
        uint64_t cacheSize, bool enableCacheMetrics));

    MOCK_METHOD1(InsertOrReplaceCache, void(const Dentry& dentry));

    MOCK_METHOD2(DeleteCache, void(uint64_t parentId, const std::string& name));

    MOCK_METHOD3(GetDentry, CURVEFS_ERROR(uint64_t parent,
        const std::string &name, Dentry *out));

    MOCK_METHOD1(CreateDentry, CURVEFS_ERROR(const Dentry &dentry));

    MOCK_METHOD2(DeleteDentry, CURVEFS_ERROR(uint64_t parent,
                                             const std::string &name));

    MOCK_METHOD5(ListDentry, CURVEFS_ERROR(uint64_t parent,
                                           std::list<Dentry> *dentryList,
                                           uint32_t limit,
                                           bool onlyDir,
                                           bool needCache));

    MOCK_METHOD1(ReleaseCache, void(uint64_t parentId));
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_DENTRY_CACHE_MAMAGER_H_
