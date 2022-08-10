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
 * Date: Sat Sep  4 14:08:04 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/raft_log_codec.h"

#include <butil/sys_byteorder.h>
#include <google/protobuf/message.h>
#include <gtest/gtest.h>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/copyset/meta_operator.h"
#include "curvefs/test/utils/protobuf_message_utils.h"
#include "src/common/macros.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curvefs::test::GenerateAnDefaultInitializedMessage;

TEST(RaftLogCodecTest, EncodeTest_RequestSerializeFailed) {
    OperatorType type = OperatorType::GetDentry;

    GetDentryRequest request;
    request.set_poolid(1);
    request.set_copysetid(1);
    request.set_partitionid(1);
    request.set_fsid(1);
    request.set_parentinodeid(1);
    request.set_txid(1);

    // NOTE: serialize will check whether message's size is bigger than INT_MAX
    auto usedSize = request.ByteSize();
    request.set_allocated_name(
        new std::string(static_cast<uint64_t>(INT_MAX) - usedSize + 1, 'x'));

    butil::IOBuf log;
    ASSERT_FALSE(RaftLogCodec::Encode(type, &request, &log));
}

TEST(RaftLogCodecTest, EncodeAndDecodeTest) {
    OperatorType type = OperatorType::GetDentry;

    GetDentryRequest request;
    request.set_poolid(1);
    request.set_copysetid(1);
    request.set_partitionid(1);
    request.set_fsid(1);
    request.set_parentinodeid(1);
    request.set_name("hello");
    request.set_txid(1);

    butil::IOBuf log;
    EXPECT_TRUE(RaftLogCodec::Encode(type, &request, &log));

    auto meta = RaftLogCodec::Decode(nullptr, log);
    EXPECT_NE(nullptr, meta.get());
    EXPECT_EQ(type, meta->GetOperatorType());

    auto* realtype = dynamic_cast<GetDentryOperator*>(meta.get());
    EXPECT_NE(nullptr, realtype);
}

TEST(RaftLogCodecTest, TypeAndRequestAreMismatch) {
    OperatorType type = OperatorType::CreateInode;

    GetDentryRequest request;
    request.set_poolid(1);
    request.set_copysetid(1);
    request.set_partitionid(1);
    request.set_fsid(1);
    request.set_parentinodeid(1);
    request.set_name("hello");
    request.set_txid(1);

    butil::IOBuf log;
    EXPECT_TRUE(RaftLogCodec::Encode(type, &request, &log));

    ASSERT_EQ(nullptr, RaftLogCodec::Decode(nullptr, log));
}

TEST(RaftLogCodecTest, DecodeFailedTest) {
#define DECODE_FAILED_TEST(TYPE)                                           \
    do {                                                                   \
        butil::IOBuf data;                                                 \
        const uint32_t networkType =                                       \
            butil::HostToNet32(static_cast<uint32_t>(OperatorType::TYPE)); \
        data.append(&networkType, sizeof(networkType));                    \
        const uint32_t networkRequestSize = butil::HostToNet32(1024);      \
        data.append(&networkRequestSize, sizeof(networkRequestSize));      \
        data.append(std::string(1024, 'x'));                               \
        EXPECT_EQ(nullptr, RaftLogCodec::Decode(nullptr, data));           \
    } while (0)

    DECODE_FAILED_TEST(GetDentry);
    DECODE_FAILED_TEST(ListDentry);
    DECODE_FAILED_TEST(CreateDentry);
    DECODE_FAILED_TEST(DeleteDentry);
    DECODE_FAILED_TEST(GetInode);
    DECODE_FAILED_TEST(CreateInode);
    DECODE_FAILED_TEST(UpdateInode);
    DECODE_FAILED_TEST(DeleteInode);
    DECODE_FAILED_TEST(CreateRootInode);
    DECODE_FAILED_TEST(CreateManageInode);
    DECODE_FAILED_TEST(CreatePartition);
    DECODE_FAILED_TEST(DeletePartition);
    DECODE_FAILED_TEST(PrepareRenameTx);

#undef DECODE_FAILED_TEST
}

TEST(RaftLogCodecTest, DecodeSuccessTest) {
#define ENCODE_DECODE_TEST(TYPE)                                             \
    do {                                                                     \
        auto request = GenerateAnDefaultInitializedMessage(                  \
            "curvefs.metaserver." STRINGIFY(TYPE##Request));                 \
        EXPECT_NE(nullptr, request);                                         \
        EXPECT_TRUE(request->IsInitialized());                               \
        butil::IOBuf data;                                                   \
        EXPECT_TRUE(                                                         \
            RaftLogCodec::Encode(OperatorType::TYPE, request.get(), &data)); \
        auto decodedRequest = RaftLogCodec::Decode(nullptr, data);           \
        EXPECT_NE(nullptr, decodedRequest);                                  \
        auto* real = dynamic_cast<TYPE##Operator*>(decodedRequest.get());    \
        EXPECT_NE(nullptr, real);                                            \
    } while (0)

    ENCODE_DECODE_TEST(GetDentry);
    ENCODE_DECODE_TEST(ListDentry);
    ENCODE_DECODE_TEST(CreateDentry);
    ENCODE_DECODE_TEST(DeleteDentry);
    ENCODE_DECODE_TEST(GetInode);
    ENCODE_DECODE_TEST(CreateInode);
    ENCODE_DECODE_TEST(UpdateInode);
    ENCODE_DECODE_TEST(DeleteInode);
    ENCODE_DECODE_TEST(CreateRootInode);
    ENCODE_DECODE_TEST(CreateManageInode);
    ENCODE_DECODE_TEST(CreatePartition);
    ENCODE_DECODE_TEST(DeletePartition);
    ENCODE_DECODE_TEST(PrepareRenameTx);

#undef ENCODE_DECODE_TEST
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
