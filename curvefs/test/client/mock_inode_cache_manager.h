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
#include <memory>
#include <set>
#include <list>
#include <map>

#include "curvefs/src/client/inode_cache_manager.h"

namespace curvefs {
namespace client {

class MockInodeCacheManager : public InodeCacheManager {
 public:
    MockInodeCacheManager() {}
    ~MockInodeCacheManager() {}

    MOCK_METHOD3(Init,
                 CURVEFS_ERROR(uint64_t cacheSize, bool enableCacheMetrics,
                               uint32_t flushPeriodSec));

    MOCK_METHOD0(Run, void());

    MOCK_METHOD0(Stop, void());

    MOCK_METHOD2(GetInode,
                 CURVEFS_ERROR(uint64_t inodeId,
                               std::shared_ptr<InodeWrapper> &out));  // NOLINT

    MOCK_METHOD2(GetInodeAttr, CURVEFS_ERROR(
        uint64_t inodeId, InodeAttr *out));

    MOCK_METHOD1(RefreshInode, CURVEFS_ERROR(uint64_t inodeId));

    MOCK_METHOD2(BatchGetInodeAttr, CURVEFS_ERROR(
        std::set<uint64_t> *inodeIds, std::list<InodeAttr> *attrs));

    MOCK_METHOD3(BatchGetInodeAttrAsync,
        CURVEFS_ERROR(uint64_t parentId, const std::set<uint64_t> &inodeIds,
        std::map<uint64_t, InodeAttr> *attrs));

    MOCK_METHOD2(BatchGetXAttr, CURVEFS_ERROR(
        std::set<uint64_t> *inodeIds, std::list<XAttr> *xattrs));

    MOCK_METHOD2(CreateInode, CURVEFS_ERROR(const InodeParam &param,
        std::shared_ptr<InodeWrapper> &out));     // NOLINT

    MOCK_METHOD1(DeleteInode, CURVEFS_ERROR(uint64_t inodeid));

    MOCK_METHOD1(InvalidateNlinkCache, void(uint64_t inodeid));

    MOCK_METHOD2(AddInodeAttrs, void(uint64_t parentId,
        const RepeatedPtrField<InodeAttr>& inodeAttrs));

    MOCK_METHOD1(ClearInodeCache, void(uint64_t inodeid));

    MOCK_METHOD1(ShipToFlush, void(
        const std::shared_ptr<InodeWrapper> &inodeWrapper));

    MOCK_METHOD0(FlushAll, void());

    MOCK_METHOD0(FlushInodeOnce, void());

    MOCK_METHOD1(ReleaseCache, void(uint64_t parentId));
};

}  // namespace client
}  // namespace curvefs


#endif  // CURVEFS_TEST_CLIENT_MOCK_INODE_CACHE_MANAGER_H_
