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
#include <cstdint>
#include <string>
#include <list>
#include "curvefs/src/client/dentry_cache_manager.h"

namespace curvefs {
namespace client {

class MockDentryCacheManager : public DentryCacheManager {
 public:
    MockDentryCacheManager() {}
    ~MockDentryCacheManager() {}

    MOCK_METHOD3(GetDentry, CURVEFS_ERROR(uint64_t parent,
        const std::string &name, Dentry *out));

    MOCK_METHOD1(CreateDentry, CURVEFS_ERROR(const Dentry &dentry));

    MOCK_METHOD3(DeleteDentry, CURVEFS_ERROR(uint64_t parent,
                                             const std::string &name,
                                             FsFileType type));

    MOCK_METHOD5(ListDentry, CURVEFS_ERROR(uint64_t parent,
                                           std::list<Dentry> *dentryList,
                                           uint32_t limit,
                                           bool onlyDir,
                                           uint32_t nlink));

    MOCK_METHOD2(CheckDirEmpty, CURVEFS_ERROR(const Dentry &dentry,
                                              bool *empty));
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_DENTRY_CACHE_MAMAGER_H_
