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
 * Created Date: 2020/12/03
 * Author: hzchenwei7
 */

#ifndef  TEST_MDS_NAMESERVER2_MOCK_MOCK_SNAPSHOTCLONE_CLIENT_H_
#define  TEST_MDS_NAMESERVER2_MOCK_MOCK_SNAPSHOTCLONE_CLIENT_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>
#include "src/mds/snapshotcloneclient/snapshotclone_client.h"

namespace curve {
namespace mds {
namespace snapshotcloneclient {
class MockSnapshotCloneClient: public SnapshotCloneClient {
 public:
    ~MockSnapshotCloneClient() {}
    MOCK_METHOD1(Init, void(const SnapshotCloneClientOption &option));

    MOCK_METHOD4(GetCloneRefStatus, StatusCode(std::string, std::string,
      CloneRefStatus *, std::vector<DestFileInfo> *));
};
}  // namespace snapshotcloneclient
}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_NAMESERVER2_MOCK_MOCK_SNAPSHOTCLONE_CLIENT_H_
