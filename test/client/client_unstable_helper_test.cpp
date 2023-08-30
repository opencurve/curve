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

/**
 * Project: curve
 * Created Date: 2020/03/19
 * Author: wuhanqing
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <butil/endpoint.h>
#include <utility>

#include "src/client/unstable_helper.h"

namespace curve {
namespace client {

TEST(UnstableHelperTest, normal_test) {
    UnstableHelper helper;

    ChunkServerUnstableOption opt;
    opt.maxStableChunkServerTimeoutTimes = 10;
    opt.serverUnstableThreshold = 3;

    helper.Init(opt);

    std::vector<std::pair<ChunkServerID, butil::EndPoint>> chunkservers;
    for (int i = 1; i <= opt.serverUnstableThreshold; ++i) {
        butil::EndPoint ep;
        std::string ipPort = "127.100.0.1:" + std::to_string(i + 60000);
        butil::str2endpoint(ipPort.c_str(), &ep);
        chunkservers.emplace_back(std::make_pair(i, ep));
    }

    //First, perform 10 consecutive timeouts on each chunkserver
    for (const auto& cs : chunkservers) {
        for (int i = 1; i <= opt.maxStableChunkServerTimeoutTimes; ++i) {
            helper.IncreTimeout(cs.first);

            ASSERT_EQ(UnstableState::NoUnstable,
                      helper.GetCurrentUnstableState(
                          cs.first, cs.second));
        }
    }

    //Add another timeout to each chunkserver
    //The first two are in the chunkserver unstable state, and the third is in the server unstable state
    helper.IncreTimeout(chunkservers[0].first);
    ASSERT_EQ(UnstableState::ChunkServerUnstable,
              helper.GetCurrentUnstableState(
                  chunkservers[0].first, chunkservers[0].second));

    helper.IncreTimeout(chunkservers[1].first);
    ASSERT_EQ(UnstableState::ChunkServerUnstable,
              helper.GetCurrentUnstableState(
                  chunkservers[1].first, chunkservers[1].second));

    helper.IncreTimeout(chunkservers[2].first);
    ASSERT_EQ(UnstableState::ServerUnstable,
              helper.GetCurrentUnstableState(
                  chunkservers[2].first, chunkservers[2].second));

    //Continue to increase the number of timeouts
    //In this case, it is always chunkserver unstable
    helper.IncreTimeout(chunkservers[0].first);
    ASSERT_EQ(UnstableState::ChunkServerUnstable,
              helper.GetCurrentUnstableState(
                  chunkservers[0].first, chunkservers[0].second));
    helper.IncreTimeout(chunkservers[1].first);
    ASSERT_EQ(UnstableState::ChunkServerUnstable,
              helper.GetCurrentUnstableState(
                  chunkservers[1].first, chunkservers[1].second));
    helper.IncreTimeout(chunkservers[2].first);
    ASSERT_EQ(UnstableState::ChunkServerUnstable,
              helper.GetCurrentUnstableState(
                  chunkservers[2].first, chunkservers[2].second));

    //The first timeout of a new chunkserver can be directly set to chunkserver unstable based on the IP address
    butil::EndPoint ep;
    butil::str2endpoint("127.100.0.1:60999", &ep);
    auto chunkserver4 = std::make_pair(4, ep);

    helper.IncreTimeout(chunkserver4.first);

    ASSERT_EQ(UnstableState::ChunkServerUnstable,
              helper.GetCurrentUnstableState(
                  chunkserver4.first, chunkserver4.second));

    //Chunkservers for other IPs
    butil::str2endpoint("127.200.0.1:60999", &ep);
    auto chunkserver5 = std::make_pair(5, ep);
    for (int i = 1; i <= opt.maxStableChunkServerTimeoutTimes; ++i) {
        helper.IncreTimeout(chunkserver5.first);
        ASSERT_EQ(UnstableState::NoUnstable,
                  helper.GetCurrentUnstableState(
                      chunkserver5.first, chunkserver5.second));
    }
    helper.IncreTimeout(chunkserver5.first);
    ASSERT_EQ(UnstableState::ChunkServerUnstable,
                  helper.GetCurrentUnstableState(
                      chunkserver5.first, chunkserver5.second));
}

}  // namespace client
}  // namespace curve
