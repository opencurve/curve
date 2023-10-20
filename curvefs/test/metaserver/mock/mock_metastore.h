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

#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/src/metaserver/metastore.h"

namespace curvefs {
namespace metaserver {
namespace mock {

class MockMetaStore : public curvefs::metaserver::MetaStore {
 public:
    MOCK_METHOD1(Load, bool(const std::string&));
    MOCK_METHOD2(SaveMeta,
                 bool(const std::string&, std::vector<std::string>* files));
    MOCK_METHOD2(SaveData,
                 bool(const std::string&, std::vector<std::string>* files));
    MOCK_METHOD0(Clear, bool());
    MOCK_METHOD0(Destroy, bool());

    MOCK_METHOD3(CreatePartition,
                 MetaStatusCode(const CreatePartitionRequest*,
                                CreatePartitionResponse*, int64_t logIndex));
    MOCK_METHOD3(DeletePartition,
                 MetaStatusCode(const DeletePartitionRequest*,
                                DeletePartitionResponse*, int64_t logIndex));
    MOCK_METHOD1(GetPartitionInfoList, bool(std::list<PartitionInfo>*));
    MOCK_METHOD1(GetPartitionSnap,
                 bool(std::map<uint32_t, std::shared_ptr<Partition>>*));

    MOCK_METHOD3(CreateDentry,
                 MetaStatusCode(const CreateDentryRequest*,
                                CreateDentryResponse*, int64_t logIndex));
    MOCK_METHOD3(DeleteDentry,
                 MetaStatusCode(const DeleteDentryRequest*,
                                DeleteDentryResponse*, int64_t logIndex));
    MOCK_METHOD3(GetDentry,
                 MetaStatusCode(const GetDentryRequest*, GetDentryResponse*,
                                int64_t logIndex));
    MOCK_METHOD3(ListDentry,
                 MetaStatusCode(const ListDentryRequest*, ListDentryResponse*,
                                int64_t logIndex));

    MOCK_METHOD3(CreateInode,
                 MetaStatusCode(const CreateInodeRequest*, CreateInodeResponse*,
                                int64_t logIndex));
    MOCK_METHOD3(CreateRootInode,
                 MetaStatusCode(const CreateRootInodeRequest*,
                                CreateRootInodeResponse*, int64_t logIndex));
    MOCK_METHOD3(CreateManageInode,
                 MetaStatusCode(const CreateManageInodeRequest*,
                                CreateManageInodeResponse*, int64_t logIndex));
    MOCK_METHOD3(GetInode, MetaStatusCode(const GetInodeRequest*,
                                          GetInodeResponse*, int64_t logIndex));
    MOCK_METHOD3(BatchGetInodeAttr,
                 MetaStatusCode(const BatchGetInodeAttrRequest*,
                                BatchGetInodeAttrResponse*, int64_t logIndex));
    MOCK_METHOD3(BatchGetXAttr,
                 MetaStatusCode(const BatchGetXAttrRequest*,
                                BatchGetXAttrResponse*, int64_t logIndex));
    MOCK_METHOD3(DeleteInode,
                 MetaStatusCode(const DeleteInodeRequest*, DeleteInodeResponse*,
                                int64_t logIndex));
    MOCK_METHOD3(UpdateInode,
                 MetaStatusCode(const UpdateInodeRequest*, UpdateInodeResponse*,
                                int64_t logIndex));

    MOCK_METHOD3(PrepareRenameTx,
                 MetaStatusCode(const PrepareRenameTxRequest*,
                                PrepareRenameTxResponse*, int64_t logIndex));

    MOCK_METHOD0(GetStreamServer, std::shared_ptr<StreamServer>());

    MOCK_METHOD4(GetOrModifyS3ChunkInfo,
                 MetaStatusCode(const GetOrModifyS3ChunkInfoRequest* request,
                                GetOrModifyS3ChunkInfoResponse* response,
                                std::shared_ptr<Iterator>* iterator,
                                int64_t logIndex));

    MOCK_METHOD2(SendS3ChunkInfoByStream,
                 MetaStatusCode(std::shared_ptr<StreamConnection> connection,
                                std::shared_ptr<Iterator> iterator));

    MOCK_METHOD3(GetVolumeExtent,
                 MetaStatusCode(const GetVolumeExtentRequest*,
                                GetVolumeExtentResponse*, int64_t logIndex));

    MOCK_METHOD3(UpdateVolumeExtent,
                 MetaStatusCode(const UpdateVolumeExtentRequest*,
                                UpdateVolumeExtentResponse*, int64_t logIndex));

    MOCK_METHOD3(UpdateDeallocatableBlockGroup,
                 MetaStatusCode(const UpdateDeallocatableBlockGroupRequest*,
                                UpdateDeallocatableBlockGroupResponse*,
                                int64_t logIndex));

    MOCK_METHOD3(UpdateFsUsed,
                 MetaStatusCode(const UpdateFsUsedRequest*,
                                UpdateFsUsedResponse*, int64_t logIndex));

    MOCK_METHOD0(GetFsId2FsUsage, FsId2FsUsage());
};

}  // namespace mock
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_MOCK_MOCK_METASTORE_H_
