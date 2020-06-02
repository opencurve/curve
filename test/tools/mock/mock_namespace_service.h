/*
 * Project: curve
 * File Created: 2020-06-01
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#ifndef TEST_TOOLS_MOCK_MOCK_NAMESPACE_SERVICE_H_
#define TEST_TOOLS_MOCK_MOCK_NAMESPACE_SERVICE_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "proto/nameserver2.pb.h"

using google::protobuf::RpcController;
using google::protobuf::Closure;

namespace curve {
namespace mds {
class MockNameService: public CurveFSService {
 public:
    MOCK_METHOD4(CreateFile,
        void(RpcController *controller,
        const CreateFileRequest *request,
        CreateFileResponse *response,
        Closure *done));
    MOCK_METHOD4(DeleteFile,
        void(RpcController *controller,
        const DeleteFileRequest *request,
        DeleteFileResponse *response,
        Closure *done));
    MOCK_METHOD4(GetFileInfo,
        void(RpcController *controller,
        const GetFileInfoRequest *request,
        GetFileInfoResponse *response,
        Closure *done));
    MOCK_METHOD4(GetOrAllocateSegment,
        void(RpcController *controller,
        const GetOrAllocateSegmentRequest *request,
        GetOrAllocateSegmentResponse *response,
        Closure *done));
    MOCK_METHOD4(RenameFile,
        void(RpcController *controller,
        const RenameFileRequest *request,
        RenameFileResponse *response,
        Closure *done));
    MOCK_METHOD4(ExtendFile,
        void(RpcController *controller,
        const ExtendFileRequest *request,
        ExtendFileResponse *response,
        Closure *done));
    MOCK_METHOD4(ChangeOwner,
        void(RpcController *controller,
        const ChangeOwnerRequest *request,
        ChangeOwnerResponse *response,
        Closure *done));
    MOCK_METHOD4(ListDir,
        void(RpcController *controller,
        const ListDirRequest *request,
        ListDirResponse *response,
        Closure *done));
    MOCK_METHOD4(CreateSnapShot,
        void(RpcController *controller,
        const CreateSnapShotRequest *request,
        CreateSnapShotResponse *response,
        Closure *done));
    MOCK_METHOD4(ListSnapShot,
        void(RpcController *controller,
        const ListSnapShotFileInfoRequest *request,
        ListSnapShotFileInfoResponse *response,
        Closure *done));
    MOCK_METHOD4(DeleteSnapShot,
        void(RpcController *controller,
        const DeleteSnapShotRequest *request,
        DeleteSnapShotResponse *response,
        Closure *done));
    MOCK_METHOD4(CheckSnapShotStatus,
        void(RpcController *controller,
        const CheckSnapShotStatusRequest *request,
        CheckSnapShotStatusResponse *response,
        Closure *done));
    MOCK_METHOD4(GetSnapShotFileSegment,
        void(RpcController *controller,
        const GetOrAllocateSegmentRequest *request,
        GetOrAllocateSegmentResponse *response,
        Closure *done));
    MOCK_METHOD4(OpenFile,
        void(RpcController *controller,
        const OpenFileRequest *request,
        OpenFileResponse *response,
        Closure *done));
    MOCK_METHOD4(CloseFile,
        void(RpcController *controller,
        const CloseFileRequest *request,
        CloseFileResponse *response,
        Closure *done));
    MOCK_METHOD4(RefreshSession,
        void(RpcController *controller,
        const ReFreshSessionRequest *request,
        ReFreshSessionResponse *response,
        Closure *done));
    MOCK_METHOD4(CreateCloneFile,
        void(RpcController *controller,
        const CreateCloneFileRequest *request,
        CreateCloneFileResponse *response,
        Closure *done));
    MOCK_METHOD4(SetCloneFileStatus,
        void(RpcController *controller,
        const SetCloneFileStatusRequest *request,
        SetCloneFileStatusResponse *response,
        Closure *done));
    MOCK_METHOD4(RegistClient,
        void(RpcController *controller,
        const RegistClientRequest *request,
        RegistClientResponse *response,
        Closure *done));
    MOCK_METHOD4(GetAllocatedSize,
        void(RpcController *controller,
        const GetAllocatedSizeRequest *request,
        GetAllocatedSizeResponse *response,
        Closure *done));
    MOCK_METHOD4(GetFileSize,
        void(RpcController *controller,
        const GetFileSizeRequest *request,
        GetFileSizeResponse *response,
        Closure *done));
    MOCK_METHOD4(ListClient,
        void(RpcController *controller,
        const ListClientRequest *request,
        ListClientResponse *response,
        Closure *done));
    MOCK_METHOD4(FindFileMountPoint,
        void(RpcController *controller,
        const FindFileMountPointRequest *request,
        FindFileMountPointResponse *response,
        Closure *done));
};
}  // namespace mds
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_MOCK_NAMESPACE_SERVICE_H_
