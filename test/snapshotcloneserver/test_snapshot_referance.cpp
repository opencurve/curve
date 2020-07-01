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
 * Created Date: Sun May 05 2019
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/snapshotcloneserver/common/snapshot_reference.h"


namespace curve {
namespace snapshotcloneserver {

TEST(TestSnapshotReference, TestSnapshotReferenceSuccess) {
    SnapshotReference referance;
    UUID snapshotId1 = "id1";
    referance.IncrementSnapshotRef(snapshotId1);

    int refcount = referance.GetSnapshotRef(snapshotId1);
    ASSERT_EQ(1, refcount);

    referance.IncrementSnapshotRef(snapshotId1);
    refcount = referance.GetSnapshotRef(snapshotId1);
    ASSERT_EQ(2, refcount);

    UUID snapshotId2 = "id2";
    referance.IncrementSnapshotRef(snapshotId2);
    int refcount1 = referance.GetSnapshotRef(snapshotId1);
    int refcount2 = referance.GetSnapshotRef(snapshotId2);
    ASSERT_EQ(2, refcount1);
    ASSERT_EQ(1, refcount2);

    referance.DecrementSnapshotRef(snapshotId1);
    refcount1 = referance.GetSnapshotRef(snapshotId1);
    refcount2 = referance.GetSnapshotRef(snapshotId2);
    ASSERT_EQ(1, refcount1);
    ASSERT_EQ(1, refcount2);

    referance.DecrementSnapshotRef(snapshotId1);
    refcount1 = referance.GetSnapshotRef(snapshotId1);
    refcount2 = referance.GetSnapshotRef(snapshotId2);
    ASSERT_EQ(0, refcount1);
    ASSERT_EQ(1, refcount2);

    referance.DecrementSnapshotRef(snapshotId1);
    refcount1 = referance.GetSnapshotRef(snapshotId1);
    refcount2 = referance.GetSnapshotRef(snapshotId2);
    ASSERT_EQ(0, refcount1);
    ASSERT_EQ(1, refcount2);


    UUID snapshotId3 = "id3";
    int refcount3 = referance.GetSnapshotRef(snapshotId3);
    ASSERT_EQ(0, refcount3);
}

}  // namespace snapshotcloneserver
}  // namespace curve
