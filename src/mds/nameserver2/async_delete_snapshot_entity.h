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
 * Created Date: Saturday December 22nd 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_MDS_NAMESERVER2_ASYNC_DELETE_SNAPSHOT_ENTITY_H_
#define SRC_MDS_NAMESERVER2_ASYNC_DELETE_SNAPSHOT_ENTITY_H_

#include "proto/nameserver2.pb.h"

namespace curve {
namespace  mds {

using ::curve::mds::DeleteSnapShotResponse;
using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class AsyncDeleteSnapShotEntity {
 public:
    AsyncDeleteSnapShotEntity(DeleteSnapShotResponse* deleteResponse,
                    const DeleteSnapShotRequest* deleteRequest,
                    RpcController* controller,
                    Closure* closure) {
        deleteResponse_ = deleteResponse;
        deleteRequest_ = deleteRequest;
        controller_ = controller;
        closure_ = closure;
    }

    DeleteSnapShotResponse* GetDeleteResponse(void) {
        return deleteResponse_;
    }

    const DeleteSnapShotRequest* GetDeleteRequest(void) const {
        return deleteRequest_;
    }

    RpcController* GetController(void) {
        return controller_;
    }

    Closure* GetClosure(void) {
        return closure_;
    }

    void SetClosure(Closure *closure) {
        closure_ = closure;
    }

 private:
    // response set the response
    DeleteSnapShotResponse* deleteResponse_;
    const DeleteSnapShotRequest*  deleteRequest_;
    RpcController* controller_;
    Closure* closure_;
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_ASYNC_DELETE_SNAPSHOT_ENTITY_H_
