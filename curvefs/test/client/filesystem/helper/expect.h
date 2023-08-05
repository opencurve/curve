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
 * Project: Curve
 * Created Date: 2023-03-29
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_TEST_CLIENT_FILESYSTEM_HELPER_EXPECT_H_
#define CURVEFS_TEST_CLIENT_FILESYSTEM_HELPER_EXPECT_H_

#include <gmock/gmock.h>

#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/mock_dentry_cache_mamager.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::testing::_;
using ::testing::Return;
using ::testing::Invoke;
using ::curvefs::client::MockDentryCacheManager;
using ::curvefs::client::MockInodeCacheManager;
using ::curvefs::client::rpcclient::MockMetaServerClient;

/*
 * Reference:
 *
 * DentryCacheManager:
 *   ListDentry(uint64_t parent,
 *              std::list<Dentry> *dentryList,
 *              uint32_t limit,
 *              bool dirOnly = false,
 *              uint32_t nlink = 0);
 *
 *
 * InodeCacheManager:
 *   GetInodeAttr(uint64_t inodeId, InodeAttr *out);
 *
 *   BatchGetInodeAttrAsync(uint64_t parentId,
 *                          std::set<uint64_t>* inodeIds,
 *                          std::map<uint64_t, InodeAttr> *attrs);
 *
 *   GetInode(uint64_t inodeId,
 *            std::shared_ptr<InodeWrapper>& out);
 */

// times
#define EXPECT_CALL_INDOE_SYNC_TIMES(CLIENT, INO, TIMES)                   \
    EXPECT_CALL(CLIENT, UpdateInodeWithOutNlinkAsync_rvr(_, INO, _, _, _)) \
        .Times(TIMES)

// return
#define EXPECT_CALL_RETURN_GetDentry(MANAGER, CODE) \
do {                                                \
    EXPECT_CALL(MANAGER, GetDentry(_, _, _))        \
        .WillOnce(Return(CODE));                    \
} while (0)

#define EXPECT_CALL_RETURN_ListDentry(MANAGER, CODE) \
do {                                                 \
    EXPECT_CALL(MANAGER, ListDentry(_, _, _, _, _))  \
        .WillOnce(Return(CODE));                     \
} while (0)

#define EXPECT_CALL_RETURN_GetInodeAttr(MANAGER, CODE) \
do {                                                   \
    EXPECT_CALL(MANAGER, GetInodeAttr(_, _))           \
        .WillOnce(Return(CODE));                       \
} while (0)

#define EXPECT_CALL_RETURN_BatchGetInodeAttrAsync(MANAGER, CODE) \
do {                                                             \
    EXPECT_CALL(MANAGER, BatchGetInodeAttrAsync(_, _, _))        \
        .WillOnce(Return(CODE));                                 \
} while (0)

#define EXPECT_CALL_RETURN_GetInode(MANAGER, CODE) \
do {                                               \
    EXPECT_CALL(MANAGER, GetInode(_, _))           \
        .WillOnce(Return(CODE));                   \
} while (0)

// invoke
#define EXPECT_CALL_INVOKE_ListDentry(MANAGER, CALLBACK) \
do {                                                     \
    EXPECT_CALL(MANAGER, ListDentry(_, _, _, _, _))      \
        .WillOnce(Invoke(CALLBACK));                     \
} while (0)

#define EXPECT_CALL_INVOKE_GetInodeAttr(MANAGER, CALLBACK) \
do {                                                       \
    EXPECT_CALL(MANAGER, GetInodeAttr(_, _))               \
        .WillOnce(Invoke(CALLBACK));                       \
} while (0)

#define EXPECT_CALL_INVOKE_BatchGetInodeAttrAsync(MANAGER, CALLBACK) \
do {                                                                 \
    EXPECT_CALL(MANAGER, BatchGetInodeAttrAsync(_, _, _))            \
        .WillOnce(Invoke(CALLBACK));                                 \
} while (0)

#define EXPECT_CALL_INVOKE_GetInode(MANAGER, CALLBACK) \
do {                                                   \
    EXPECT_CALL(MANAGER, GetInode(_, _))               \
        .WillOnce(Invoke(CALLBACK));                   \
} while (0)

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_FILESYSTEM_HELPER_EXPECT_H_
