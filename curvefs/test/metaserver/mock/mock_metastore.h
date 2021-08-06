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
 * Project: curve
 * Date: Wed Aug 18 15:10:17 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_METASERVER_MOCK_MOCK_METASTORE_H_
#define CURVEFS_TEST_METASERVER_MOCK_MOCK_METASTORE_H_

#include <gmock/gmock.h>

#include <string>

#include "curvefs/src/metaserver/metastore.h"

namespace curvefs {
namespace metaserver {
namespace mock {

class MockMetaStore : public curvefs::metaserver::MetaStore {
 public:
    MOCK_METHOD1(Load, bool(const std::string&));
    MOCK_METHOD2(Save, bool(const std::string&, OnSnapshotSaveDoneClosure*));
    MOCK_METHOD0(Clear, bool());

    MOCK_METHOD2(CreatePartition, MetaStatusCode(const CreatePartitionRequest*,
                                                 CreatePartitionResponse*));
    MOCK_METHOD2(DeletePartition, MetaStatusCode(const DeletePartitionRequest*,
                                                 DeletePartitionResponse*));

    MOCK_METHOD2(CreateDentry, MetaStatusCode(const CreateDentryRequest*,
                                              CreateDentryResponse*));
    MOCK_METHOD2(DeleteDentry, MetaStatusCode(const DeleteDentryRequest*,
                                              DeleteDentryResponse*));
    MOCK_METHOD2(GetDentry,
                 MetaStatusCode(const GetDentryRequest*, GetDentryResponse*));
    MOCK_METHOD2(ListDentry,
                 MetaStatusCode(const ListDentryRequest*, ListDentryResponse*));

    MOCK_METHOD2(CreateInode, MetaStatusCode(const CreateInodeRequest*,
                                             CreateInodeResponse*));
    MOCK_METHOD2(CreateRootInode, MetaStatusCode(const CreateRootInodeRequest*,
                                                 CreateRootInodeResponse*));
    MOCK_METHOD2(GetInode,
                 MetaStatusCode(const GetInodeRequest*, GetInodeResponse*));
    MOCK_METHOD2(DeleteInode, MetaStatusCode(const DeleteInodeRequest*,
                                             DeleteInodeResponse*));
    MOCK_METHOD2(UpdateInode, MetaStatusCode(const UpdateInodeRequest*,
                                             UpdateInodeResponse*));
    MOCK_METHOD2(UpdateInodeS3Version,
                 MetaStatusCode(const UpdateInodeS3VersionRequest*,
                                UpdateInodeS3VersionResponse*));
    MOCK_METHOD2(PrepareRenameTx, MetaStatusCode(const PrepareRenameTxRequest*,
                                                 PrepareRenameTxResponse*));
};

}  // namespace mock
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_MOCK_MOCK_METASTORE_H_
