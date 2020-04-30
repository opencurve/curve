/*
 * Project: curve
 * Created Date: Tuesday September 25th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
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
 *  @brief 判断一个路径是否是合法的，判断规则：
 *         路径应该是从根目录开始的全路径，以"/"开始，各层目录以单个"/"进行分隔，
 *         除根目录以外，所有的path不得以"/"结尾。
 *  @param path 用来判断的路径，路径应该是从根目录开始的全路径
 *  @return true表示合法，false表示不合法
 */
// isPathValid先放这里，目前就这里用
bool isPathValid(const std::string path);

/**
 *  @brief 判断一个rename的路径是否是合法的，判断规则：
 *         1、不允许对根目录rename或者rename到根目录
 *         2、一个路径不能包含另一个路径，
 *         比如不能rename /a 到 /a/b，或者rename /a/b 到 /a
 *  @param oldFileName 用来判断的路径，路径应该是从根目录开始的全路径
 *         newFileName 用来判断的路径，路径应该是从根目录开始的全路径
 *  @return true表示合法，false表示不合法
 */
// IsRenamePathValid先放这里，目前就这里用
bool IsRenamePathValid(const std::string& oldFileName,
                       const std::string& newFileName);

/**
 *  @brief 根据错误码，判断日志的打印级别
 *  @param code 错误吗
 *  @return 打印级别，google::INFO，google::WARNING，google::ERROR
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
                       ::google::protobuf::Closure* done);

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
    void RegistClient(::google::protobuf::RpcController* controller,
                       const ::curve::mds::RegistClientRequest* request,
                       ::curve::mds::RegistClientResponse* response,
                       ::google::protobuf::Closure* done) override;
    void GetAllocatedSize(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetAllocatedSizeRequest* request,
                       ::curve::mds::GetAllocatedSizeResponse* response,
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

 private:
    FileLockManager *fileLockManager_;
};
}  // namespace mds
}  // namespace curve
#endif   // SRC_MDS_NAMESERVER2_NAMESPACE_SERVICE_H_
