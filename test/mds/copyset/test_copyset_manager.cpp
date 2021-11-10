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
 * Created Date: Mon May 13 2019
 * Author: xuchaojie
 */

#include <gtest/gtest.h>

#include "src/mds/copyset/copyset_manager.h"
#include "test/mds/copyset/test_helper.h"


namespace curve {
namespace mds {
namespace copyset {

TEST(TestCopysetManager, InitSuccess) {
    CopysetOption option;
    CopysetManager manager(option);

    CopysetConstrait constrait;
    constrait.zoneNum = 3;
    constrait.zoneChoseNum = 3;
    constrait.replicaNum = 3;

    ASSERT_TRUE(manager.Init(constrait));
}

TEST(TestCopysetManager, InitFail) {
    CopysetOption option;
    CopysetManager manager(option);

    CopysetConstrait constrait;
    constrait.zoneNum = 4;
    constrait.zoneChoseNum = 4;
    constrait.replicaNum = 3;

    ASSERT_FALSE(manager.Init(constrait));
}

TEST(TestCopysetManager, GenCopysetByCopysetNumSuccess) {
    CopysetOption option;
    CopysetManager manager(option);

    CopysetConstrait constrait;
    constrait.zoneNum = 3;
    constrait.zoneChoseNum = 3;
    constrait.replicaNum = 3;

    ASSERT_TRUE(manager.Init(constrait));

    TestCluster cluster;
    cluster.SetUniformCluster();

    std::vector<Copyset> out;
    uint32_t scatterWidth = 0;
    ASSERT_TRUE(manager.GenCopyset(cluster,
        3, &scatterWidth, &out));
    ASSERT_EQ(3, out.size());
}

TEST(TestCopysetManager, GenCopysetByScatterWidthSuccess) {
    CopysetOption option;
    CopysetManager manager(option);

    CopysetConstrait constrait;
    constrait.zoneNum = 3;
    constrait.zoneChoseNum = 3;
    constrait.replicaNum = 3;

    ASSERT_TRUE(manager.Init(constrait));

    TestCluster cluster;
    cluster.SetUniformCluster();

    std::vector<Copyset> out;
    uint32_t scatterWidth = 4;
    ASSERT_TRUE(manager.GenCopyset(cluster,
        0, &scatterWidth, &out));
}

TEST(TestCopysetManager, GenCopysetByCopysetNumAndValidSuccess) {
    CopysetOption option;
    option.copysetRetryTimes = 10;
    option.scatterWidthFloatingPercentage = 20;
    CopysetManager manager(option);

    CopysetConstrait constrait;
    constrait.zoneNum = 3;
    constrait.zoneChoseNum = 3;
    constrait.replicaNum = 3;

    ASSERT_TRUE(manager.Init(constrait));

    {
        // 180 chunkservers, 3 zones, 6000 copysets
        TestCluster cluster;
        cluster.SetMassiveCluster(180, 3);

        std::vector<Copyset> out;
        uint32_t scatterWidth = 0;
        ASSERT_TRUE(manager.GenCopyset(cluster,
            6000, &scatterWidth, &out));
        ASSERT_EQ(6000, out.size());
    }

    {
        // 180 chunkservers, 3 zones, 9000 copysets
        TestCluster cluster;
        cluster.SetMassiveCluster(180, 3);

        std::vector<Copyset> out;
        uint32_t scatterWidth = 0;
        ASSERT_TRUE(manager.GenCopyset(cluster,
            9000, &scatterWidth, &out));
        ASSERT_EQ(9000, out.size());
    }

    {
        // 240 chunkservers, 3 zones, 9000 copysets
        TestCluster cluster;
        cluster.SetMassiveCluster(240, 3);

        std::vector<Copyset> out;
        uint32_t scatterWidth = 0;
        ASSERT_TRUE(manager.GenCopyset(cluster,
            9000, &scatterWidth, &out));
        ASSERT_EQ(9000, out.size());
    }

    {
        // 240 chunkservers, 3 zones, 12000 copysets
        TestCluster cluster;
        cluster.SetMassiveCluster(240, 3);

        std::vector<Copyset> out;
        uint32_t scatterWidth = 0;
        ASSERT_TRUE(manager.GenCopyset(cluster,
            12000, &scatterWidth, &out));
        ASSERT_EQ(12000, out.size());
    }

    {
        // 3 chunkservers, 3 zone, 100 copysets
        TestCluster cluster;
        cluster.SetUniformCluster();

        std::vector<Copyset> out;
        uint32_t scatterWidth = 2;
        ASSERT_TRUE(manager.GenCopyset(cluster,
            100, &scatterWidth, &out));
        ASSERT_EQ(100, out.size());
    }

    {
        // 1 chunkserver, 1 zone, 100 copysets
        TestCluster cluster;
        cluster.SetMassiveCluster(1, 1);

        std::vector<Copyset> out;
        uint32_t scatterWidth = 0;

        CopysetOption option;
        option.copysetRetryTimes = 10;
        option.scatterWidthFloatingPercentage = 20;
        CopysetManager manager2(option);

        CopysetConstrait constrait;
        constrait.zoneNum = 1;
        constrait.zoneChoseNum = 1;
        constrait.replicaNum = 1;

        ASSERT_TRUE(manager2.Init(constrait));

        ASSERT_TRUE(manager2.GenCopyset(cluster,
            100, &scatterWidth, &out));
        ASSERT_EQ(100, out.size());
    }
}

}  // namespace copyset
}  // namespace mds
}  // namespace curve
