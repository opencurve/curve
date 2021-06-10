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
 * Created Date: Thur Jun 16 2021
 * Author: lixiaocui
 */


#ifndef CURVEFS_TEST_CLIENT_MOCK_MDS_SERVICE_H_
#define CURVEFS_TEST_CLIENT_MOCK_MDS_SERVICE_H_

#include <gmock/gmock.h>
#include "curvefs/proto/mds.pb.h"

namespace curvefs {
namespace client {

class MockMdsService : public curvefs::mds::MdsService {
 public:
    MockMdsService() : MdsService() {}
    ~MockMdsService() = default;

    MOCK_METHOD4(CreateFs, void(::google::protobuf::RpcController *controller,
                                const ::curvefs::mds::CreateFsRequest *request,
                                ::curvefs::mds::CreateFsResponse *response,
                                ::google::protobuf::Closure *done));
    MOCK_METHOD4(MountFs, void(::google::protobuf::RpcController *controller,
                               const ::curvefs::mds::MountFsRequest *request,
                               ::curvefs::mds::MountFsResponse *response,
                               ::google::protobuf::Closure *done));
    MOCK_METHOD4(UmountFs, void(::google::protobuf::RpcController *controller,
                                const ::curvefs::mds::UmountFsRequest *request,
                                ::curvefs::mds::UmountFsResponse *response,
                                ::google::protobuf::Closure *done));
    MOCK_METHOD4(GetFsInfo,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::mds::GetFsInfoRequest *request,
                      ::curvefs::mds::GetFsInfoResponse *response,
                      ::google::protobuf::Closure *done));
    MOCK_METHOD4(DeleteFs, void(::google::protobuf::RpcController *controller,
                                const ::curvefs::mds::DeleteFsRequest *request,
                                ::curvefs::mds::DeleteFsResponse *response,
                                ::google::protobuf::Closure *done));
};
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_MDS_SERVICE_H_
