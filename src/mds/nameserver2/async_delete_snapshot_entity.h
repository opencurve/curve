/*
 * Project: curve
 * Created Date: Saturday December 22nd 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
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
