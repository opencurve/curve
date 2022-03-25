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

/*************************************************************************
> File Name: mock_s3compact_impl.h
> Author:
> Created Time: Tue 7 Sept 2021
 ************************************************************************/

#ifndef CURVEFS_TEST_METASERVER_MOCK_S3COMPACTWQ_IMPL_H_
#define CURVEFS_TEST_METASERVER_MOCK_S3COMPACTWQ_IMPL_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "curvefs/src/metaserver/s3compact_wq_impl.h"
#include "curvefs/test/metaserver/mock_s3infocache.h"

using curve::common::S3Adapter;
using curvefs::metaserver::copyset::CopysetNode;
using ::testing::Return;

namespace curvefs {
namespace metaserver {

class MockS3CompactWorkQueueImpl : public S3CompactWorkQueueImpl {
 public:
    MockS3CompactWorkQueueImpl(
        std::shared_ptr<S3AdapterManager> s3AdapterManager,
        std::shared_ptr<S3InfoCache> s3infoCache,
        const S3CompactWorkQueueOption& opts, copyset::CopysetNodeManager* mgr)
        : S3CompactWorkQueueImpl(s3AdapterManager, s3infoCache, opts, mgr) {}
    MetaStatusCode UpdateInode(
        CopysetNode* copysetNode, const PartitionInfo& pinfo, uint64_t inodeId,
        ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&& s3ChunkInfoAdd,
        ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&&
            s3ChunkInfoRemove) {
        return UpdateInode_rvr(copysetNode, pinfo, inodeId, s3ChunkInfoAdd,
                               s3ChunkInfoRemove);
    }
    MOCK_METHOD5(
        UpdateInode_rvr,
        MetaStatusCode(CopysetNode*, const PartitionInfo&, uint64_t,
                       ::google::protobuf::Map<uint64_t, S3ChunkInfoList>,
                       ::google::protobuf::Map<uint64_t, S3ChunkInfoList>));
};

class MockCopysetNodeWrapper : public CopysetNodeWrapper {
 public:
    MockCopysetNodeWrapper() : CopysetNodeWrapper(nullptr) {}
    MOCK_METHOD0(IsLeaderTerm, bool());
    MOCK_METHOD0(IsValid, bool());
};

}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_TEST_METASERVER_MOCK_S3COMPACTWQ_IMPL_H_
