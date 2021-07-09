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

#ifndef CURVEFS_TEST_CLIENT_MOCK_INODE_CACHE_MANAGER_H_
#define CURVEFS_TEST_CLIENT_MOCK_INODE_CACHE_MANAGER_H_

#include <gmock/gmock.h>

#include "curvefs/src/client/inode_cache_manager.h"

namespace curvefs {
namespace client {

class MockInodeCacheManager : public InodeCacheManager {
 public:
    MockInodeCacheManager() {}
    ~MockInodeCacheManager() {}

    MOCK_METHOD2(GetInode, CURVEFS_ERROR(uint64_t inodeid, Inode *out));

    MOCK_METHOD1(UpdateInode, CURVEFS_ERROR(const Inode &inode));

    MOCK_METHOD2(CreateInode, CURVEFS_ERROR(const InodeParam &param,
                                            Inode *out));

    MOCK_METHOD1(DeleteInode, CURVEFS_ERROR(uint64_t inodeid));
};

}  // namespace client
}  // namespace curvefs


#endif  // CURVEFS_TEST_CLIENT_MOCK_INODE_CACHE_MANAGER_H_
