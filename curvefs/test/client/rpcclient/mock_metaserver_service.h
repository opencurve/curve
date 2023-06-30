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


#ifndef CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_METASERVER_SERVICE_H_
#define CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_METASERVER_SERVICE_H_

#include <gmock/gmock.h>
#include "curvefs/proto/metaserver.pb.h"

namespace curvefs {
namespace client {
namespace rpcclient {
class MockMetaServerService : public curvefs::metaserver::MetaServerService {
 public:
    MockMetaServerService() : MetaServerService() {}
    ~MockMetaServerService() = default;

    MOCK_METHOD4(GetDentry,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::metaserver::GetDentryRequest *request,
                      ::curvefs::metaserver::GetDentryResponse *response,
                      ::google::protobuf::Closure *done));

    MOCK_METHOD4(ListDentry,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::metaserver::ListDentryRequest *request,
                      ::curvefs::metaserver::ListDentryResponse *response,
                      ::google::protobuf::Closure *done));

    MOCK_METHOD4(CreateDentry,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::metaserver::CreateDentryRequest *request,
                      ::curvefs::metaserver::CreateDentryResponse *response,
                      ::google::protobuf::Closure *done));

    MOCK_METHOD4(DeleteDentry,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::metaserver::DeleteDentryRequest *request,
                      ::curvefs::metaserver::DeleteDentryResponse *response,
                      ::google::protobuf::Closure *done));

    MOCK_METHOD4(
        PrepareRenameTx,
        void(::google::protobuf::RpcController* controller,
             const ::curvefs::metaserver::PrepareRenameTxRequest* request,
             ::curvefs::metaserver::PrepareRenameTxResponse* response,
             ::google::protobuf::Closure* done));

    MOCK_METHOD4(GetInode,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::metaserver::GetInodeRequest *request,
                      ::curvefs::metaserver::GetInodeResponse *response,
                      ::google::protobuf::Closure *done));

     MOCK_METHOD4(BatchGetInodeAttr,
          void(::google::protobuf::RpcController *controller,
               const ::curvefs::metaserver::BatchGetInodeAttrRequest *request,
               ::curvefs::metaserver::BatchGetInodeAttrResponse *response,
               ::google::protobuf::Closure *done));

     MOCK_METHOD4(BatchGetXAttr,
               void(::google::protobuf::RpcController *controller,
                    const ::curvefs::metaserver::BatchGetXAttrRequest *request,
                    ::curvefs::metaserver::BatchGetXAttrResponse *response,
                    ::google::protobuf::Closure *done));

    MOCK_METHOD4(CreateInode,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::metaserver::CreateInodeRequest *request,
                      ::curvefs::metaserver::CreateInodeResponse *response,
                      ::google::protobuf::Closure *done));
    MOCK_METHOD4(UpdateInode,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::metaserver::UpdateInodeRequest *request,
                      ::curvefs::metaserver::UpdateInodeResponse *response,
                      ::google::protobuf::Closure *done));
    MOCK_METHOD4(DeleteInode,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::metaserver::DeleteInodeRequest *request,
                      ::curvefs::metaserver::DeleteInodeResponse *response,
                      ::google::protobuf::Closure *done));

    MOCK_METHOD4(GetOrModifyS3ChunkInfo,
        void(::google::protobuf::RpcController *controller,
            const ::curvefs::metaserver::GetOrModifyS3ChunkInfoRequest *request,
             ::curvefs::metaserver::GetOrModifyS3ChunkInfoResponse *response,
             ::google::protobuf::Closure *done));

    MOCK_METHOD4(
        GetVolumeExtent,
        void(::google::protobuf::RpcController *controller,
             const ::curvefs::metaserver::GetVolumeExtentRequest *request,
             ::curvefs::metaserver::GetVolumeExtentResponse *response,
             ::google::protobuf::Closure *done));

    MOCK_METHOD4(
        UpdateVolumeExtent,
        void(::google::protobuf::RpcController *controller,
             const ::curvefs::metaserver::UpdateVolumeExtentRequest *request,
             ::curvefs::metaserver::UpdateVolumeExtentResponse *response,
             ::google::protobuf::Closure *done));
};
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_METASERVER_SERVICE_H_
