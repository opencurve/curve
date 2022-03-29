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
 * Created Date: Thur Jun 16 2021
 * Author: lixiaocui
 */

#ifndef CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_BASE_CLIENT_H_
#define CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_BASE_CLIENT_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>
#include "curvefs/src/client/rpcclient/base_client.h"

namespace curvefs {
namespace client {
namespace rpcclient {
class MockMDSBaseClient : public MDSBaseClient {
 public:
    MockMDSBaseClient() : MDSBaseClient() {}
    ~MockMDSBaseClient() = default;

    MOCK_METHOD5(MountFs,
                 void(const std::string &fsName, const std::string &mountPt,
                      MountFsResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));

    MOCK_METHOD5(UmountFs,
                 void(const std::string &fsName, const std::string &mountPt,
                      UmountFsResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));

    MOCK_METHOD4(GetFsInfo,
                 void(const std::string &fsName, GetFsInfoResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD4(GetFsInfo,
                 void(uint32_t fsId, GetFsInfoResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD4(CommitTx, void(const std::vector<PartitionTxId>& txIds,
                                CommitTxResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel));

    MOCK_METHOD5(GetMetaServerInfo,
                 void(uint32_t port, std::string ip,
                      GetMetaServerInfoResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD5(GetMetaServerListInCopysets,
                 void(const LogicPoolID &logicalpooid,
                      const std::vector<CopysetID> &copysetidvec,
                      GetMetaServerListInCopySetsResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD5(CreatePartition,
                 void(uint32_t fsID, uint32_t count,
                      CreatePartitionResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));

    MOCK_METHOD4(GetCopysetOfPartitions,
                 void(const std::vector<uint32_t> &partitionIDList,
                      GetCopysetOfPartitionResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD4(ListPartition,
                 void(uint32_t fsID, ListPartitionResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD4(RefreshSession,
                 void(const std::vector<PartitionTxId> &txIds,
                      RefreshSessionResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));
};
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_BASE_CLIENT_H_
