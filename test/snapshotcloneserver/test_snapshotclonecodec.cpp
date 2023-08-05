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
 * Created Date: Wed Jul 01 2020
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <string>
#include <set>

#include "src/snapshotcloneserver/common/snapshotclonecodec.h"

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;

namespace curve {
namespace snapshotcloneserver {

static const char* kDefaultPoolset = "poolset";

TEST(TestSnapshotCloneServerCodec, TestSnapInfoEncodeDecodeEqual) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 4096, 8, kDefaultPoolset, 0,
                        Status::pending);
    SnapshotCloneCodec testObj;
    std::string value;
    ASSERT_TRUE(testObj.EncodeSnapshotData(snapInfo, &value));

    SnapshotInfo decodedSnapInfo;
    ASSERT_TRUE(testObj.DecodeSnapshotData(value, &decodedSnapInfo));

    ASSERT_EQ(snapInfo.GetUuid(), decodedSnapInfo.GetUuid());
    ASSERT_EQ(snapInfo.GetUser(), decodedSnapInfo.GetUser());
    ASSERT_EQ(snapInfo.GetFileName(), decodedSnapInfo.GetFileName());
    ASSERT_EQ(snapInfo.GetSnapshotName(), decodedSnapInfo.GetSnapshotName());
    ASSERT_EQ(snapInfo.GetSeqNum(), decodedSnapInfo.GetSeqNum());
    ASSERT_EQ(snapInfo.GetChunkSize(), decodedSnapInfo.GetChunkSize());
    ASSERT_EQ(snapInfo.GetSegmentSize(), decodedSnapInfo.GetSegmentSize());
    ASSERT_EQ(snapInfo.GetFileLength(), decodedSnapInfo.GetFileLength());
    ASSERT_EQ(snapInfo.GetStripeUnit(), decodedSnapInfo.GetStripeUnit());
    ASSERT_EQ(snapInfo.GetStripeCount(), decodedSnapInfo.GetStripeCount());
    ASSERT_EQ(snapInfo.GetPoolset(), decodedSnapInfo.GetPoolset());
    ASSERT_EQ(snapInfo.GetCreateTime(), decodedSnapInfo.GetCreateTime());
    ASSERT_EQ(snapInfo.GetStatus(), decodedSnapInfo.GetStatus());
}

TEST(TestSnapshotCloneServerCodec, TestCloneInfoEncodeDecodeEqual) {
    CloneInfo cloneInfo("cloneuuid", "cloneuser",
                     CloneTaskType::kRecover, "srcfile",
                     "dstfile", kDefaultPoolset, 1, 2, 3,
                     CloneFileType::kSnapshot, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::recovering);

    SnapshotCloneCodec testObj;
    std::string value;
    ASSERT_TRUE(testObj.EncodeCloneInfoData(cloneInfo, &value));

    CloneInfo decodeCloneInfo;
    ASSERT_TRUE(testObj.DecodeCloneInfoData(value, &decodeCloneInfo));

    ASSERT_EQ(cloneInfo.GetTaskId(), decodeCloneInfo.GetTaskId());
    ASSERT_EQ(cloneInfo.GetUser(), decodeCloneInfo.GetUser());
    ASSERT_EQ(cloneInfo.GetTaskType(), decodeCloneInfo.GetTaskType());
    ASSERT_EQ(cloneInfo.GetSrc(), decodeCloneInfo.GetSrc());
    ASSERT_EQ(cloneInfo.GetDest(), decodeCloneInfo.GetDest());
    ASSERT_EQ(cloneInfo.GetOriginId(), decodeCloneInfo.GetOriginId());
    ASSERT_EQ(cloneInfo.GetDestId(), decodeCloneInfo.GetDestId());
    ASSERT_EQ(cloneInfo.GetTime(), decodeCloneInfo.GetTime());
    ASSERT_EQ(cloneInfo.GetFileType(), decodeCloneInfo.GetFileType());
    ASSERT_EQ(cloneInfo.GetIsLazy(), decodeCloneInfo.GetIsLazy());
    ASSERT_EQ(cloneInfo.GetNextStep(), decodeCloneInfo.GetNextStep());
    ASSERT_EQ(cloneInfo.GetStatus(), decodeCloneInfo.GetStatus());
}

TEST(TestSnapshotCloneServerCodec, TestEncodeKeyNotEqual) {
    SnapshotCloneCodec testObj;
    std::string encodeKey;

    int keyNum = 10000;

    std::set<std::string> keySet;
    for (int i = 0; i < keyNum; i++) {
        encodeKey = testObj.EncodeSnapshotKey(std::to_string(i));
        keySet.insert(encodeKey);
    }

    for (int i = 0; i < keyNum; i++) {
        encodeKey = testObj.EncodeCloneInfoKey(std::to_string(i));
        keySet.insert(encodeKey);
    }

    ASSERT_EQ(keyNum * 2, keySet.size());
}



}  // namespace snapshotcloneserver
}  // namespace curve
