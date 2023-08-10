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
 * Created Date: Tuesday September 25th 2018
 * Author: hzsunjianliang
 */

#ifndef  SRC_MDS_NAMESERVER2_NAMESPACE_SERVICE_H_
#define  SRC_MDS_NAMESERVER2_NAMESPACE_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <string>
#include "proto/nameserver2.pb.h"
#include "src/mds/nameserver2/file_lock.h"

namespace curve {
namespace mds {
/**
 *  @brief determine whether a path is legal by following rules：
 *         1. the path should be the full path start from the root directory
 *            starting with "/", and each level of directory should be separated
 *            by a single "/"
 *         2. except for the root directory, all paths must not end with "/"
 *  @param path the path to judge
 *  @return true if legal, false if not
 */
// isPathValid put here temporarily, used here only
bool isPathValid(const std::string path);

/**
 *  @brief determine whether a renamed path by following rules：
 *         1.renaming or renaming to the root directory are not allowed
 *         2.a path cannot contain another path
 *           e.g. rename /a to /a/b, or /a/b to /a
 *  @param oldFileName the path to judge, and should be the full path
 *  @param newFileName the path to judge, and should be the full path
 *  @return true if legal, false if not
 */
// IsRenamePathValidput here temporarily, used here only
bool IsRenamePathValid(const std::string& oldFileName,
                       const std::string& newFileName);

/**
 *  @brief determine the log level according to the error code
 *  @param code error code
 *  @return log level: google::INFO, google::WARNING, google::ERROR
 */
uint32_t GetMdsLogLevel(StatusCode code);

class NameSpaceService: public CurveFSService {
 public:
    explicit NameSpaceService(FileLockManager *fileLockManager) {
        fileLockManager_ = fileLockManager;
    }

    virtual ~NameSpaceService() {}

    void CreateFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateFileRequest* request,
                       ::curve::mds::CreateFileResponse* response,
                       ::google::protobuf::Closure* done) override;

    void DeleteFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::DeleteFileRequest* request,
                       ::curve::mds::DeleteFileResponse* response,
                       ::google::protobuf::Closure* done) override;

    void RecoverFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::RecoverFileRequest* request,
                       ::curve::mds::RecoverFileResponse* response,
                       ::google::protobuf::Closure* done) override;

    void GetFileInfo(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetFileInfoRequest* request,
                       ::curve::mds::GetFileInfoResponse* response,
                       ::google::protobuf::Closure* done) override;

    void GetOrAllocateSegment(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetOrAllocateSegmentRequest* request,
                       ::curve::mds::GetOrAllocateSegmentResponse* response,
                       ::google::protobuf::Closure* done) override;

    void RenameFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::RenameFileRequest* request,
                       ::curve::mds::RenameFileResponse* response,
                       ::google::protobuf::Closure* done) override;

    void ExtendFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::ExtendFileRequest* request,
                       ::curve::mds::ExtendFileResponse* response,
                       ::google::protobuf::Closure* done) override;

    void ChangeOwner(::google::protobuf::RpcController* controller,
                       const ::curve::mds::ChangeOwnerRequest* request,
                       ::curve::mds::ChangeOwnerResponse* response,
                       ::google::protobuf::Closure* done) override;

    void ListDir(::google::protobuf::RpcController* controller,
                       const ::curve::mds::ListDirRequest* request,
                       ::curve::mds::ListDirResponse* response,
                       ::google::protobuf::Closure* done) override;

    void IncreaseFileEpoch(::google::protobuf::RpcController* controller,
                       const ::curve::mds::IncreaseFileEpochRequest* request,
                       ::curve::mds::IncreaseFileEpochResponse* response,
                       ::google::protobuf::Closure* done) override;

    void CreateSnapShot(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateSnapShotRequest* request,
                       ::curve::mds::CreateSnapShotResponse* response,
                       ::google::protobuf::Closure* done) override;
    void ListSnapShot(::google::protobuf::RpcController* controller,
                       const ::curve::mds::ListSnapShotFileInfoRequest* request,
                       ::curve::mds::ListSnapShotFileInfoResponse* response,
                       ::google::protobuf::Closure* done) override;
    void DeleteSnapShot(::google::protobuf::RpcController* controller,
                       const ::curve::mds::DeleteSnapShotRequest* request,
                       ::curve::mds::DeleteSnapShotResponse* response,
                       ::google::protobuf::Closure* done) override;
    void CheckSnapShotStatus(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CheckSnapShotStatusRequest* request,
                       ::curve::mds::CheckSnapShotStatusResponse* response,
                       ::google::protobuf::Closure* done) override;
    void GetSnapShotFileSegment(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetOrAllocateSegmentRequest* request,
                       ::curve::mds::GetOrAllocateSegmentResponse* response,
                       ::google::protobuf::Closure* done) override;

    // ProtectSnapShot
    void ProtectSnapShot(::google::protobuf::RpcController* controller,
                       const ::curve::mds::ProtectSnapShotRequest* request,
                       ::curve::mds::ProtectSnapShotResponse* response,
                       ::google::protobuf::Closure* done) override;
    // UnprotectSnapShot
    void UnprotectSnapShot(::google::protobuf::RpcController* controller,
                       const ::curve::mds::UnprotectSnapShotRequest* request,
                       ::curve::mds::UnprotectSnapShotResponse* response,
                       ::google::protobuf::Closure* done) override;

    // clone
    void Clone(::google::protobuf::RpcController* controller,
        const ::curve::mds::CloneRequest* request,
        ::curve::mds::CloneResponse* response,
        ::google::protobuf::Closure* done) override;
    // flatten
    void Flatten(::google::protobuf::RpcController* controller,
        const ::curve::mds::FlattenRequest* request,
        ::curve::mds::FlattenResponse* response,
        ::google::protobuf::Closure* done) override;
    // query flatten status
    void QueryFlattenStatus(::google::protobuf::RpcController* controller,
        const ::curve::mds::QueryFlattenStatusRequest* request,
        ::curve::mds::QueryFlattenStatusResponse* response,
        ::google::protobuf::Closure* done) override;
    // children
    void Children(::google::protobuf::RpcController* controller,
        const ::curve::mds::ChildrenRequest* request,
        ::curve::mds::ChildrenResponse* response,
        ::google::protobuf::Closure* done) override;

    void OpenFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::OpenFileRequest* request,
                       ::curve::mds::OpenFileResponse* response,
                       ::google::protobuf::Closure* done) override;
    void CloseFile(::google::protobuf::RpcController* controller,
                        const ::curve::mds::CloseFileRequest* request,
                        ::curve::mds::CloseFileResponse* response,
                        ::google::protobuf::Closure* done) override;
    void RefreshSession(::google::protobuf::RpcController* controller,
                        const ::curve::mds::ReFreshSessionRequest* request,
                        ::curve::mds::ReFreshSessionResponse* response,
                        ::google::protobuf::Closure* done) override;
    void CreateCloneFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateCloneFileRequest* request,
                       ::curve::mds::CreateCloneFileResponse* response,
                       ::google::protobuf::Closure* done) override;
    void SetCloneFileStatus(::google::protobuf::RpcController* controller,
                       const ::curve::mds::SetCloneFileStatusRequest* request,
                       ::curve::mds::SetCloneFileStatusResponse* response,
                       ::google::protobuf::Closure* done) override;
    void GetAllocatedSize(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetAllocatedSizeRequest* request,
                       ::curve::mds::GetAllocatedSizeResponse* response,
                       ::google::protobuf::Closure* done) override;
    void GetFileSize(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetFileSizeRequest* request,
                       ::curve::mds::GetFileSizeResponse* response,
                       ::google::protobuf::Closure* done) override;
    void ListClient(::google::protobuf::RpcController* controller,
                       const ::curve::mds::ListClientRequest* request,
                       ::curve::mds::ListClientResponse* response,
                       ::google::protobuf::Closure* done) override;
    void FindFileMountPoint(
        ::google::protobuf::RpcController* controller,
        const ::curve::mds::FindFileMountPointRequest* request,
        ::curve::mds::FindFileMountPointResponse* response,
        ::google::protobuf::Closure* done) override;
    void ListVolumesOnCopysets(
                ::google::protobuf::RpcController* controller,
                const ::curve::mds::ListVolumesOnCopysetsRequest* request,
                ::curve::mds::ListVolumesOnCopysetsResponse* response,
                ::google::protobuf::Closure* done) override;

 private:
    FileLockManager *fileLockManager_;
};
}  // namespace mds
}  // namespace curve
#endif   // SRC_MDS_NAMESERVER2_NAMESPACE_SERVICE_H_
