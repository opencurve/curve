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
 * Date: Wed Jan 13 09:48:12 CST 2021
 * Author: wuhanqing
 */

#ifndef TEST_CLIENT_MOCK_MOCK_NAMESPACE_SERVICE_H_
#define TEST_CLIENT_MOCK_MOCK_NAMESPACE_SERVICE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "proto/nameserver2.pb.h"

namespace curve {
namespace mds {

class MockNameService : public CurveFSService {
 public:
    MOCK_METHOD4(OpenFile, void(google::protobuf::RpcController* cntl,
                                const OpenFileRequest* request,
                                OpenFileResponse* response,
                                google::protobuf::Closure* done));

     MOCK_METHOD4(CloseFile, void(google::protobuf::RpcController* cntl,
                                    const CloseFileRequest* request,
                                    CloseFileResponse* response,
                                    google::protobuf::Closure* done));

    MOCK_METHOD4(CreateFile, void(google::protobuf::RpcController* cntl,
                                  const CreateFileRequest* request,
                                  CreateFileResponse* response,
                                  google::protobuf::Closure* done));

    MOCK_METHOD4(GetFileInfo, void(google::protobuf::RpcController* cntl,
                                   const GetFileInfoRequest* request,
                                   GetFileInfoResponse* response,
                                   google::protobuf::Closure* done));

    MOCK_METHOD4(RecoverFile, void(google::protobuf::RpcController* cntl,
                                   const RecoverFileRequest* request,
                                   RecoverFileResponse* response,
                                   google::protobuf::Closure* done));

    MOCK_METHOD4(DeleteFile, void(google::protobuf::RpcController* cntl,
                                  const DeleteFileRequest* request,
                                  DeleteFileResponse* response,
                                  google::protobuf::Closure* done));

    MOCK_METHOD4(RenameFile, void(google::protobuf::RpcController* cntl,
                                  const RenameFileRequest* request,
                                  RenameFileResponse* response,
                                  google::protobuf::Closure* done));

    MOCK_METHOD4(ChangeOwner, void(google::protobuf::RpcController* cntl,
                                  const ChangeOwnerRequest* request,
                                  ChangeOwnerResponse* response,
                                  google::protobuf::Closure* done));

    MOCK_METHOD4(CreateSnapShot, void(google::protobuf::RpcController* cntl,
                                       const CreateSnapShotRequest* request,
                                       CreateSnapShotResponse* response,
                                       google::protobuf::Closure* done));

    MOCK_METHOD4(DeleteSnapShot, void(google::protobuf::RpcController* cntl,
                                      const DeleteSnapShotRequest* request,
                                      DeleteSnapShotResponse* response,
                                      google::protobuf::Closure* done));

    MOCK_METHOD4(ListSnapShot, void(google::protobuf::RpcController* cntl,
                                    const ListSnapShotFileInfoRequest* request,
                                    ListSnapShotFileInfoResponse* response,
                                    google::protobuf::Closure* done));

    MOCK_METHOD4(GetSnapShotFileSegment,
                 void(google::protobuf::RpcController* cntl,
                      const GetOrAllocateSegmentRequest* request,
                      GetOrAllocateSegmentResponse* response,
                      google::protobuf::Closure* done));

    MOCK_METHOD4(CheckSnapShotStatus,
                 void(google::protobuf::RpcController* cntl,
                      const CheckSnapShotStatusRequest* request,
                      CheckSnapShotStatusResponse* response,
                      google::protobuf::Closure* done));

    MOCK_METHOD4(RefreshSession,
                 void(::google::protobuf::RpcController* controller,
                      const curve::mds::ReFreshSessionRequest* request,
                      curve::mds::ReFreshSessionResponse* response,
                      ::google::protobuf::Closure* done));

    MOCK_METHOD4(IncreaseFileEpoch,
                 void(::google::protobuf::RpcController* controller,
                      const curve::mds::IncreaseFileEpochRequest* request,
                      curve::mds::IncreaseFileEpochResponse* response,
                      ::google::protobuf::Closure* done));

    MOCK_METHOD4(CreateCloneFile,
                 void(::google::protobuf::RpcController* controller,
                      const curve::mds::CreateCloneFileRequest* request,
                      curve::mds::CreateCloneFileResponse* response,
                      ::google::protobuf::Closure* done));

    MOCK_METHOD4(SetCloneFileStatus,
                 void(::google::protobuf::RpcController* controller,
                      const curve::mds::SetCloneFileStatusRequest* request,
                      curve::mds::SetCloneFileStatusResponse* response,
                      ::google::protobuf::Closure* done));

    MOCK_METHOD4(GetOrAllocateSegment,
                 void(::google::protobuf::RpcController* controller,
                      const curve::mds::GetOrAllocateSegmentRequest* request,
                      curve::mds::GetOrAllocateSegmentResponse* response,
                      ::google::protobuf::Closure* done));

    MOCK_METHOD4(DeAllocateSegment,
                 void(::google::protobuf::RpcController* controller,
                      const curve::mds::DeAllocateSegmentRequest* request,
                      curve::mds::DeAllocateSegmentResponse* response,
                      ::google::protobuf::Closure* done));

    MOCK_METHOD4(ExtendFile,
                 void(::google::protobuf::RpcController* controller,
                      const curve::mds::ExtendFileRequest* request,
                      curve::mds::ExtendFileResponse* response,
                      ::google::protobuf::Closure* done));

    MOCK_METHOD4(ListDir,
                 void(::google::protobuf::RpcController* controller,
                      const curve::mds::ListDirRequest* request,
                      curve::mds::ListDirResponse* response,
                      ::google::protobuf::Closure* done));
};

}  // namespace mds
}  // namespace curve

#endif  // TEST_CLIENT_MOCK_MOCK_NAMESPACE_SERVICE_H_
