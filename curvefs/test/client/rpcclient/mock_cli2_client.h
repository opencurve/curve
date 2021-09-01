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

#ifndef CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_CLI2_CLIENT_H_
#define CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_CLI2_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <string>

#include "curvefs/src/client/rpcclient/cli2_client.h"

using ::testing::_;
using ::testing::Return;

namespace curvefs {
namespace client {
namespace rpcclient {

class MockCli2Client : public Cli2Client {
 public:
    MockCli2Client() {}
    ~MockCli2Client() {}

    MOCK_METHOD6(GetLeader,
                 bool(const LogicPoolID &poolID, const CopysetID &copysetID,
                      const PeerInfoList &peerInfoList,
                      int16_t currentLeaderIndex, PeerAddr *peerAddr,
                      MetaserverID *metaserverID));
};
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_CLI2_CLIENT_H_
