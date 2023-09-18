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
 * Created Date: 2023-09-07
 * Author: xuchaojie
 */

#ifndef TEST_MDS_NAMESERVER2_MOCK_MOCK_COPYSET_CLIENT_H_
#define TEST_MDS_NAMESERVER2_MOCK_MOCK_COPYSET_CLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "src/mds/chunkserverclient/copyset_client.h"

namespace curve {
namespace mds {
namespace chunkserverclient {

class MockCopysetClient : public CopysetClientInterface {
 public:
    MOCK_METHOD4(DeleteChunkSnapshotOrCorrectSn,
        int(LogicalPoolID logicalPoolId,
            CopysetID copysetId,
            ChunkID chunkId,
            uint64_t correctedSn));
    MOCK_METHOD8(DeleteChunkSnapshot,
        int(uint64_t fileId,
        uint64_t originFileId,
        uint64_t chunkIndex,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t snapSn,
        const std::vector<uint64_t>& snaps));
    MOCK_METHOD7(DeleteChunk,
        int(uint64_t fileId,
        uint64_t originFileId,
        uint64_t chunkIndex,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t sn));
    MOCK_METHOD2(FlattenChunk,
        int(const std::shared_ptr<FlattenChunkContext> &ctx,
        CopysetClientClosure* done));
};

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_NAMESERVER2_MOCK_MOCK_COPYSET_CLIENT_H_
