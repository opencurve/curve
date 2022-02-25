/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-03-02
 * Author: chengyi01
 */

#ifndef CURVEFS_TEST_METASERVER_MOCK_MDS_CLIENT_H_
#define CURVEFS_TEST_METASERVER_MOCK_MDS_CLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "curvefs/src/metaserver/mdsclient/mds_client.h"

using ::testing::_;
using ::testing::Return;

namespace curvefs {
namespace metaserver {
namespace mdsclient {

class MockMdsClient : public MdsClient {
 public:
    MockMdsClient() {}
    ~MockMdsClient() {}
    MOCK_METHOD2(Init,
                 FSStatusCode(const ::curve::client::MetaServerOption& mdsOpt,
                              MDSBaseClient* baseclient));
    MOCK_METHOD2(GetFsInfo,
                 FSStatusCode(const std::string& fsName, FsInfo* fsInfo));

    MOCK_METHOD2(GetFsInfo, FSStatusCode(uint32_t fsId, FsInfo* fsInfo));
};

}  // namespace mdsclient
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_MOCK_MDS_CLIENT_H_
