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
 * Created Date: 2020/12/2
 * Author: hzchenwei7
 */

#ifndef TEST_MDS_MOCK_MOCK_SNAPSHOTCLONESERVER_H_
#define TEST_MDS_MOCK_MOCK_SNAPSHOTCLONESERVER_H_

#include "proto/snapshotcloneserver.pb.h"

namespace curve {
namespace snapshotcloneserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class MockSnapshotCloneService : public SnapshotCloneService {
 public:
    MOCK_METHOD4(default_method,
        void(RpcController *controller,
        const HttpRequest *request,
        HttpResponse *response,
        Closure *done));
};

}  // namespace snapshotcloneserver
}  // namespace curve


#endif  // TEST_MDS_MOCK_MOCK_SNAPSHOTCLONESERVER_H_
