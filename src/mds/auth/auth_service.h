/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-05-24
 * Author: wanghai (SeanHai)
 */


#ifndef SRC_MDS_AUTH_AUTH_SERVICE_H_
#define SRC_MDS_AUTH_AUTH_SERVICE_H_

#include <glog/logging.h>
#include <brpc/server.h>
#include <memory>
#include "proto/auth.pb.h"
#include "src/mds/auth/auth_service_manager.h"

namespace curve {
namespace mds {
namespace auth {

class AuthServiceImpl : public AuthService {
 public:
    explicit AuthServiceImpl(std::shared_ptr<AuthServiceManager> authMgr)
        : authMgr_(authMgr) {}

    virtual ~AuthServiceImpl() = default;

    void AddKey(google::protobuf::RpcController* cntl_base,
                const AddKeyRequest *request,
                AddKeyResponse* response,
                google::protobuf::Closure* done) override;

    void DeleteKey(google::protobuf::RpcController* cntl_base,
                   const DeleteKeyRequest *request,
                   DeleteKeyResponse* response,
                   google::protobuf::Closure* done) override;

    void GetKey(google::protobuf::RpcController* cntl_base,
                const GetKeyRequest *request,
                GetKeyResponse* response,
                google::protobuf::Closure* done) override;

    void UpdateKey(google::protobuf::RpcController* cntl_base,
                   const UpdateKeyRequest *request,
                   UpdateKeyResponse* response,
                   google::protobuf::Closure* done) override;

    void GetTicket(google::protobuf::RpcController* cntl_base,
                   const GetTicketRequest *request,
                   GetTicketResponse* response,
                   google::protobuf::Closure* done) override;

 private:
    std::shared_ptr<AuthServiceManager> authMgr_;
};

}  // namespace auth
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_AUTH_AUTH_SERVICE_H_
