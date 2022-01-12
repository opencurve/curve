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

#ifndef CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_CLIENT_H_
#define CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_CLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "curvefs/src/client/rpcclient/mds_client.h"

using ::testing::_;
using ::testing::Return;

namespace curvefs {
namespace client {
namespace rpcclient {

class MockMdsClient : public MdsClient {
 public:
    MockMdsClient() {}
    ~MockMdsClient() {}

    MOCK_METHOD2(Init,
                 FSStatusCode(const ::curve::client::MetaServerOption& mdsOpt,
                              MDSBaseClient* baseclient));

    MOCK_METHOD3(CreateFs,
                 FSStatusCode(const std::string& fsName, uint64_t blockSize,
                              const Volume& volume));

    MOCK_METHOD3(CreateFsS3,
                 FSStatusCode(const std::string& fsName, uint64_t blockSize,
                              const S3Info& s3Info));

    MOCK_METHOD1(DeleteFs, FSStatusCode(const std::string& fsName));

    MOCK_METHOD3(MountFs,
                 FSStatusCode(const std::string& fsName,
                              const std::string& mountPt, FsInfo* fsInfo));

    MOCK_METHOD2(UmountFs, FSStatusCode(const std::string& fsName,
                                        const std::string& mountPt));

    MOCK_METHOD2(GetFsInfo,
                 FSStatusCode(const std::string& fsName, FsInfo* fsInfo));

    MOCK_METHOD2(GetFsInfo, FSStatusCode(uint32_t fsId, FsInfo* fsInfo));

    MOCK_METHOD2(AllocS3ChunkId,
                 FSStatusCode(uint32_t fsId, uint64_t* chunkId));

    MOCK_METHOD1(CommitTx,
                 TopoStatusCode(const std::vector<PartitionTxId>& txIds));

    MOCK_METHOD2(GetMetaServerInfo,
                 bool(const PeerAddr& addr,
                      CopysetPeerInfo<MetaserverID>* metaserverInfo));

    MOCK_METHOD3(GetMetaServerListInCopysets,
                 bool(const LogicPoolID& logicalpooid,
                      const std::vector<CopysetID>& copysetidvec,
                      std::vector<CopysetInfo<MetaserverID>>* cpinfoVec));

    MOCK_METHOD3(CreatePartition,
                 bool(uint32_t fsID, uint32_t count,
                      std::vector<PartitionInfo>* partitionInfos));

    MOCK_METHOD2(GetCopysetOfPartitions,
                 bool(const std::vector<uint32_t>& partitionIDList,
                      std::map<uint32_t, Copyset>* copysetMap));

    MOCK_METHOD2(ListPartition,
                 bool(uint32_t fsID,
                      std::vector<PartitionInfo>* partitionInfos));
};
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_CLIENT_H_
