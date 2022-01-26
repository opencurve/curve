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

#ifndef CURVEFS_TEST_CLIENT_MOCK_METASERVER_CLIENT_H_
#define CURVEFS_TEST_CLIENT_MOCK_METASERVER_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <list>
#include <string>
#include <vector>
#include <memory>
#include <set>

#include "curvefs/src/client/rpcclient/metaserver_client.h"

using ::testing::Return;
using ::testing::_;

namespace curvefs {
namespace client {
namespace rpcclient {

class MockMetaServerClient : public MetaServerClient {
 public:
    MockMetaServerClient() {}
    ~MockMetaServerClient() {}

    MOCK_METHOD3(Init, MetaStatusCode(const ExcutorOpt &excutorOpt,
        std::shared_ptr<MetaCache> metaCache,
        std::shared_ptr<ChannelManager<MetaserverID>> channelManager));

    MOCK_METHOD4(GetTxId, MetaStatusCode(uint32_t fsId,
                                         uint64_t inodeId,
                                         uint32_t* partitionId,
                                         uint64_t* txId));

    MOCK_METHOD2(SetTxId, void(uint32_t partitionId, uint64_t txId));

    MOCK_METHOD4(GetDentry, MetaStatusCode(uint32_t fsId, uint64_t inodeid,
                  const std::string &name, Dentry *out));

    MOCK_METHOD6(ListDentry, MetaStatusCode(uint32_t fsId, uint64_t inodeid,
            const std::string &last, uint32_t count, bool onlyDir,
            std::list<Dentry> *dentryList));

    MOCK_METHOD1(CreateDentry, MetaStatusCode(const Dentry &dentry));

    MOCK_METHOD3(DeleteDentry, MetaStatusCode(
            uint32_t fsId, uint64_t inodeid, const std::string &name));

    MOCK_METHOD1(PrepareRenameTx,
                 MetaStatusCode(const std::vector<Dentry>& dentrys));

    MOCK_METHOD3(GetInode, MetaStatusCode(
            uint32_t fsId, uint64_t inodeid, Inode *out));

    MOCK_METHOD3(BatchGetInodeAttr, MetaStatusCode(
        uint32_t fsId, const std::set<uint64_t> &inodeIds,
        std::list<InodeAttr> *attr));

    MOCK_METHOD3(BatchGetXAttr, MetaStatusCode(
        uint32_t fsId, const std::set<uint64_t> &inodeIds,
        std::list<XAttr> *xattr));

    MOCK_METHOD2(UpdateInode,
                 MetaStatusCode(const Inode &inode,
                                InodeOpenStatusChange statusChange));

    MOCK_METHOD3(UpdateInodeAsync,
                 void(const Inode &inode, MetaServerClientDone *done,
                      InodeOpenStatusChange statusChange));

    MOCK_METHOD2(UpdateXattrAsync, void(const Inode &inode,
        MetaServerClientDone *done));

    MOCK_METHOD5(GetOrModifyS3ChunkInfo, MetaStatusCode(
        uint32_t fsId, uint64_t inodeId,
        const google::protobuf::Map<
            uint64_t, S3ChunkInfoList> &s3ChunkInfos,
        bool returnS3ChunkInfoMap,
        google::protobuf::Map<
            uint64_t, S3ChunkInfoList> *out));

    MOCK_METHOD4(GetOrModifyS3ChunkInfoAsync, void(
        uint32_t fsId, uint64_t inodeId,
        const google::protobuf::Map<
            uint64_t, S3ChunkInfoList> &s3ChunkInfos,
        MetaServerClientDone *done));

    MOCK_METHOD2(CreateInode, MetaStatusCode(
            const InodeParam &param, Inode *out));

    MOCK_METHOD2(DeleteInode, MetaStatusCode(uint32_t fsId, uint64_t inodeid));
};

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_METASERVER_CLIENT_H_
