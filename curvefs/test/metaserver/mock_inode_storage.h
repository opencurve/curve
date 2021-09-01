/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-09-01
 * @Author: xuchaojie
 */

#ifndef CURVEFS_TEST_METASERVER_MOCK_INODE_STORAGE_H_
#define CURVEFS_TEST_METASERVER_MOCK_INODE_STORAGE_H_

#include <gmock/gmock.h>

#include "curvefs/src/metaserver/inode_storage.h"

namespace curvefs {
namespace metaserver {

class MockInodeStorage : public InodeStorage {
 public:
    MockInodeStorage() {}
    ~MockInodeStorage() {}

    MOCK_METHOD1(Insert, MetaStatusCode(const Inode &inode));
    MOCK_METHOD2(Get, MetaStatusCode(const InodeKey &key, Inode *inode));
    MOCK_METHOD1(Delete, MetaStatusCode(const InodeKey &key));
    MOCK_METHOD1(Update, MetaStatusCode(const Inode &inode));
    MOCK_METHOD0(Count, int());
    MOCK_METHOD0(GetInodeContainer, InodeContainerType *());
};


}  // namespace metaserver
}  // namespace curvefs


#endif  // CURVEFS_TEST_METASERVER_MOCK_INODE_STORAGE_H_
