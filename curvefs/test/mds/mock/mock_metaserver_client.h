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
 * @Date: 2021-09-07
 * @Author: wanghai01
 */

#ifndef CURVEFS_TEST_MDS_MOCK_MOCK_METASERVER_CLIENT_H_
#define CURVEFS_TEST_MDS_MOCK_MOCK_METASERVER_CLIENT_H_

#include <gmock/gmock-generated-function-mockers.h>
#include <gmock/gmock.h>
#include <set>
#include <string>
#include "curvefs/src/mds/metaserverclient/metaserver_client.h"

namespace curvefs {
namespace mds {
class MockMetaserverClient : public MetaserverClient {
 public:
    explicit MockMetaserverClient(const MetaserverOptions& option) :
                                  MetaserverClient(option) {}
    MOCK_METHOD2(GetLeader, FSStatusCode(const LeaderCtx &ctx,
                                         std::string *leader));
    MOCK_METHOD2(DeleteInode, FSStatusCode(uint32_t fsId, uint64_t inodeId));
    MOCK_METHOD8(CreateRootInode, FSStatusCode(uint32_t fsId, uint32_t poolId,
        uint32_t copysetId, uint32_t partitionId, uint32_t uid,
        uint32_t gid, uint32_t mode, const std::set<std::string> &addrs));
    MOCK_METHOD7(CreatePartition, FSStatusCode(uint32_t fsId, uint32_t poolId,
                                 uint32_t copysetId, uint32_t partitionId,
                                 uint64_t idStart, uint64_t idEnd,
                                 const std::set<std::string> &addrs));
    MOCK_METHOD3(CreateCopySet, FSStatusCode(uint32_t poolId,
            uint32_t copysetId, const std::set<std::string> &addrs));
    MOCK_METHOD4(DeletePartition, FSStatusCode(uint32_t poolId,
        uint32_t copysetId, uint32_t partitionId,
        const std::set<std::string> &addrs));
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_MOCK_MOCK_METASERVER_CLIENT_H_
