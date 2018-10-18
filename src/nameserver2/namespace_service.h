/*
 * Project: curve
 * Created Date: Tuesday September 25th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef  SRC_NAMESERVER2_NAMESPACE_SERVICE_H_
#define  SRC_NAMESERVER2_NAMESPACE_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include "proto/nameserver2.pb.h"


namespace curve {
namespace mds {

class NameSpaceService: public CurveFSService {
 public:
    NameSpaceService() {}

    virtual ~NameSpaceService() {}

    void CreateFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateFileRequest* request,
                       ::curve::mds::CreateFileResponse* response,
                       ::google::protobuf::Closure* done) override;

    void GetFileInfo(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetFileInfoRequest* request,
                       ::curve::mds::GetFileInfoResponse* response,
                       ::google::protobuf::Closure* done) override;

    void GetOrAllocateSegment(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetOrAllocateSegmentRequest* request,
                       ::curve::mds::GetOrAllocateSegmentResponse* response,
                       ::google::protobuf::Closure* done) override;
};
}  // namespace mds
}  // namespace curve
#endif   // SRC_NAMESERVER2_NAMESPACE_SERVICE_H_
