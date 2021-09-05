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

using curve::common::S3Adapter;
using curvefs::metaserver::copyset::CopysetNode;
using ::testing::Return;

namespace curvefs {
namespace metaserver {

class MockS3CompactWorkQueueImpl : public S3CompactWorkQueueImpl {
 public:
    MockS3CompactWorkQueueImpl(std::shared_ptr<S3Adapter> s3Adapter,
                               const S3CompactWorkQueueOption& opts)
        : S3CompactWorkQueueImpl(s3Adapter, opts) {}
    MOCK_METHOD3(UpdateInode, MetaStatusCode(CopysetNode*, const PartitionInfo&,
                                             const Inode&));
};

class MockCopysetNodeWrapper : public CopysetNodeWrapper {
 public:
    MockCopysetNodeWrapper() : CopysetNodeWrapper(nullptr) {}
    MOCK_METHOD0(IsLeaderTerm, bool());
};

}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_TEST_METASERVER_MOCK_S3COMPACTWQ_IMPL_H_
