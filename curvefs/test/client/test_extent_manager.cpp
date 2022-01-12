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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <glog/logging.h>

#include "curvefs/src/client/extent_manager.h"

namespace curvefs {
namespace client {

using ::testing::Return;
using ::testing::_;
using ::testing::Contains;
using ::testing::SetArgPointee;

class TestSimpleExtentManager : public ::testing::Test {
 protected:
    TestSimpleExtentManager() {}
    ~TestSimpleExtentManager() {}

    virtual void SetUp() {
        extent_manager_ = std::make_shared<SimpleExtentManager>();
        extManagerOption_.preAllocSize = 65536;
        extent_manager_->Init(extManagerOption_);
    }

    virtual void TearDown() {
    }

 protected:
    std::shared_ptr<SimpleExtentManager> extent_manager_;
    ExtentManagerOption extManagerOption_;
};

TEST_F(TestSimpleExtentManager, allocateExtentAndMerge) {
    VolumeExtentList extents;
    uint64_t offset = 0;
    uint64_t len = 4096;
    std::list<ExtentAllocInfo> toAllocExtents;

    CURVEFS_ERROR ret = extent_manager_->GetToAllocExtents(extents,
        offset, len, &toAllocExtents);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(1, toAllocExtents.size());
    ASSERT_EQ(0, toAllocExtents.front().lOffset);
    ASSERT_EQ(extManagerOption_.preAllocSize, toAllocExtents.front().len);
    ASSERT_EQ(false, toAllocExtents.front().leftHintAvailable);
    ASSERT_EQ(false, toAllocExtents.front().rightHintAvailable);

    uint64_t pOffset = 10240;
    std::list<Extent> allocatedExtents;
    Extent ext;
    ext.set_offset(pOffset);
    ext.set_length(extManagerOption_.preAllocSize);
    allocatedExtents.push_back(ext);

    ret = extent_manager_->MergeAllocedExtents(
        toAllocExtents, allocatedExtents, &extents);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(1, extents.volumeextents_size());
    ASSERT_EQ(0, extents.volumeextents(0).fsoffset());
    ASSERT_EQ(pOffset, extents.volumeextents(0).volumeoffset());
    ASSERT_EQ(extManagerOption_.preAllocSize,
        extents.volumeextents(0).length());
    ASSERT_EQ(false, extents.volumeextents(0).isused());

    for (int i = 0; i < extManagerOption_.preAllocSize/len; i++) {
        ret = extent_manager_->GetToAllocExtents(extents,
            offset, len, &toAllocExtents);
        ASSERT_EQ(CURVEFS_ERROR::OK, ret);
        ASSERT_EQ(0, toAllocExtents.size()) << "failed when i = " << i;
        offset += len;
    }

    ret = extent_manager_->GetToAllocExtents(extents,
        offset, len, &toAllocExtents);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(1, toAllocExtents.size());
    ASSERT_EQ(offset, toAllocExtents.front().lOffset);
    ASSERT_EQ(extManagerOption_.preAllocSize, toAllocExtents.front().len);
    ASSERT_EQ(true, toAllocExtents.front().leftHintAvailable);
    ASSERT_EQ(pOffset + extManagerOption_.preAllocSize,
        toAllocExtents.front().pOffsetLeft);
    ASSERT_EQ(false, toAllocExtents.front().rightHintAvailable);

    std::list<Extent> allocatedExtents2;
    Extent ext2;
    ext2.set_offset(pOffset + extManagerOption_.preAllocSize);
    ext2.set_length(extManagerOption_.preAllocSize);
    allocatedExtents2.push_back(ext2);
    ret = extent_manager_->MergeAllocedExtents(
        toAllocExtents, allocatedExtents2, &extents);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(1, extents.volumeextents_size());
    ASSERT_EQ(0, extents.volumeextents(0).fsoffset());
    ASSERT_EQ(pOffset, extents.volumeextents(0).volumeoffset());
    ASSERT_EQ(extManagerOption_.preAllocSize * 2,
        extents.volumeextents(0).length());
    ASSERT_EQ(false, extents.volumeextents(0).isused());
}

TEST_F(TestSimpleExtentManager, MarkExtentsWritten) {
    uint64_t pOffset = 10240;
    VolumeExtentList extents;
    VolumeExtent *vext = extents.add_volumeextents();
    vext->set_fsoffset(0);
    vext->set_volumeoffset(pOffset);
    vext->set_length(extManagerOption_.preAllocSize);
    vext->set_isused(false);

    uint64_t offset = 0;
    uint64_t len = 4096;
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;

    for (int i = 1; i < extManagerOption_.preAllocSize/len; i++) {
        LOG(INFO) << "MarkExtentsWritten i = " << i;
        ret = extent_manager_->MarkExtentsWritten(
            offset, len, &extents);
        ASSERT_EQ(CURVEFS_ERROR::OK, ret);
        ASSERT_EQ(2, extents.volumeextents_size());

        ASSERT_EQ(0, extents.volumeextents(0).fsoffset());
        ASSERT_EQ(pOffset, extents.volumeextents(0).volumeoffset());
        ASSERT_EQ(i * len, extents.volumeextents(0).length());
        ASSERT_EQ(true, extents.volumeextents(0).isused());

        ASSERT_EQ(i * len, extents.volumeextents(1).fsoffset());
        ASSERT_EQ(pOffset + i * len,
            extents.volumeextents(1).volumeoffset());
        ASSERT_EQ(extManagerOption_.preAllocSize - i * len,
            extents.volumeextents(1).length());
        ASSERT_EQ(false, extents.volumeextents(1).isused());
        offset += len;
    }

    ret = extent_manager_->MarkExtentsWritten(
        offset, len, &extents);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(1, extents.volumeextents_size());
    ASSERT_EQ(0, extents.volumeextents(0).fsoffset());
    ASSERT_EQ(pOffset, extents.volumeextents(0).volumeoffset());
    ASSERT_EQ(extManagerOption_.preAllocSize,
        extents.volumeextents(0).length());
    ASSERT_EQ(true, extents.volumeextents(0).isused());
}

TEST_F(TestSimpleExtentManager, DivideExtents) {
    uint64_t pOffset = 10240;
    VolumeExtentList extents;
    VolumeExtent *vext1 = extents.add_volumeextents();
    vext1->set_fsoffset(0);
    vext1->set_volumeoffset(pOffset);
    vext1->set_length(4096);
    vext1->set_isused(true);

    VolumeExtent *vext2 = extents.add_volumeextents();
    vext2->set_fsoffset(4096);
    vext2->set_volumeoffset(pOffset + 4096);
    vext2->set_length(4096);
    vext2->set_isused(false);

    uint64_t offset = 0;
    uint64_t len = 4096 * 2;
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;

    std::list<PExtent> pExtents;
    ret = extent_manager_->DivideExtents(extents, offset, len, &pExtents);

    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(2, pExtents.size());

    auto it = pExtents.begin();
    ASSERT_EQ(pOffset, it->pOffset);
    ASSERT_EQ(4096, it->len);
    ASSERT_EQ(false, it->UnWritten);

    it++;
    ASSERT_EQ(pOffset + 4096, it->pOffset);
    ASSERT_EQ(4096 , it->len);
    ASSERT_EQ(true, it->UnWritten);
}



}  // namespace client
}  // namespace curvefs
