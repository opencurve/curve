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
 * @Project: curve
 * @Date: 2021-06-24 16:26:02
 * @Author: chenwei
 */

#ifndef CURVEFS_TEST_MDS_MOCK_MOCK_METASERVER_H_
#define CURVEFS_TEST_MDS_MOCK_MOCK_METASERVER_H_
#include <gmock/gmock.h>
#include "curvefs/proto/metaserver.pb.h"

namespace curvefs {
namespace metaserver {
class MockMetaserverService : public MetaServerService {
 public:
    MOCK_METHOD4(GetDentry,
                 void(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::GetDentryRequest* request,
                      ::curvefs::metaserver::GetDentryResponse* response,
                      ::google::protobuf::Closure* done));
    MOCK_METHOD4(ListDentry,
                 void(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::ListDentryRequest* request,
                      ::curvefs::metaserver::ListDentryResponse* response,
                      ::google::protobuf::Closure* done));
    MOCK_METHOD4(CreateDentry,
                 void(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::CreateDentryRequest* request,
                      ::curvefs::metaserver::CreateDentryResponse* response,
                      ::google::protobuf::Closure* done));
    MOCK_METHOD4(DeleteDentry,
                 void(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::DeleteDentryRequest* request,
                      ::curvefs::metaserver::DeleteDentryResponse* response,
                      ::google::protobuf::Closure* done));
    MOCK_METHOD4(GetInode,
                 void(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::GetInodeRequest* request,
                      ::curvefs::metaserver::GetInodeResponse* response,
                      ::google::protobuf::Closure* done));
    MOCK_METHOD4(CreateInode,
                 void(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::CreateInodeRequest* request,
                      ::curvefs::metaserver::CreateInodeResponse* response,
                      ::google::protobuf::Closure* done));
    MOCK_METHOD4(CreateRootInode,
        void(::google::protobuf::RpcController* controller,
             const ::curvefs::metaserver::CreateRootInodeRequest* request,
             ::curvefs::metaserver::CreateRootInodeResponse* response,
             ::google::protobuf::Closure* done));
    MOCK_METHOD4(UpdateInode,
                 void(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::UpdateInodeRequest* request,
                      ::curvefs::metaserver::UpdateInodeResponse* response,
                      ::google::protobuf::Closure* done));
    MOCK_METHOD4(DeleteInode,
                 void(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::DeleteInodeRequest* request,
                      ::curvefs::metaserver::DeleteInodeResponse* response,
                      ::google::protobuf::Closure* done));
    MOCK_METHOD4(CreatePartition,
          void(::google::protobuf::RpcController* controller,
               const ::curvefs::metaserver::CreatePartitionRequest* request,
               ::curvefs::metaserver::CreatePartitionResponse* response,
               ::google::protobuf::Closure* done));
    MOCK_METHOD4(DeletePartition,
          void(::google::protobuf::RpcController* controller,
               const ::curvefs::metaserver::DeletePartitionRequest* request,
               ::curvefs::metaserver::DeletePartitionResponse* response,
               ::google::protobuf::Closure* done));
};

namespace copyset {
class MockCopysetService : public CopysetService {
 public:
     MOCK_METHOD4(CreateCopysetNode,
                 void(::google::protobuf::RpcController* controller,
                      const CreateCopysetRequest* request,
                      CreateCopysetResponse* response,
                      ::google::protobuf::Closure* done));
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_MOCK_MOCK_METASERVER_H_
