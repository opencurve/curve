/*
 * Project: curve
 * Created Date: Tuesday September 25th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include "src/mds/nameserver2/namespace_service.h"
#include <algorithm>
#include <vector>
#include <string>
#include "src/mds/nameserver2/curvefs.h"
#include "src/mds/nameserver2/file_lock.h"
#include "src/common/string_util.h"

namespace curve {
namespace mds {

void NameSpaceService::CreateFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateFileRequest* request,
                       ::curve::mds::CreateFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", CreateFile request path is invalid, filename = "
            << request->filename()
            << ", filetype = " << request->filetype()
            << ", filelength = " << request->filelength();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", CreateFile request, filename = " << request->filename()
        << ", filetype = " << request->filetype()
        << ", filelength = " << request->filelength();

    FileWriteLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckPathOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckPathOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckPathOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.CreateFile(request->filename(), request->owner(),
            request->filetype(), request->filelength());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        // TODO(hzsunjianliang): check if we should really print error here
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CreateFile fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CreateFile fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }

        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", CreateFile ok, filename = " << request->filename();
    }
    return;
}

void NameSpaceService::DeleteFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::DeleteFileRequest* request,
                       ::curve::mds::DeleteFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", DeleteFile request path is invalid, filename = "
            << request->filename();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", DeleteFile request, filename = " << request->filename()
        << " forDeleteFlag = " << request->forcedelete();

    FileWriteLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }

        return;
    }
    bool forceDeleteFlag = false;
    if (request->has_forcedelete()) {
        forceDeleteFlag = request->forcedelete();
    }

    uint64_t fileId = kUnitializedFileID;
    if (request->has_fileid()) {
        fileId = request->fileid();
    }

    retCode = kCurveFS.DeleteFile(request->filename(), fileId, forceDeleteFlag);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", DeleteFile fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", DeleteFile fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", DeleteFile ok, filename = " << request->filename();
    }
    return;
}

void NameSpaceService::GetFileInfo(
                        ::google::protobuf::RpcController* controller,
                        const ::curve::mds::GetFileInfoRequest* request,
                        ::curve::mds::GetFileInfoResponse* response,
                        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", GetFileInfo request path is invalid, filename = "
            << request->filename();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", GetFileInfo request, filename = " << request->filename();

    FileReadLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.GetFileInfo(request->filename(),
        response->mutable_fileinfo());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", GetFileInfo fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", GetFileInfo fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", GetFileInfo ok, filename = " << request->filename();
    }
    return;
}

void NameSpaceService::GetOrAllocateSegment(
                    ::google::protobuf::RpcController* controller,
                    const ::curve::mds::GetOrAllocateSegmentRequest* request,
                    ::curve::mds::GetOrAllocateSegmentResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", GetOrAllocateSegment request path is invalid, filename = "
            << request->filename()
            << ", offset = " << request->offset() << ", allocateTag = "
            << request->allocateifnotexist();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", GetOrAllocateSegment request, filename = " << request->filename()
        << ", offset = " << request->offset() << ", allocateTag = "
        << request->allocateifnotexist();

    FileWriteLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.GetOrAllocateSegment(request->filename(),
                request->offset(),
                request->allocateifnotexist(),
                response->mutable_pagefilesegment());

    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", GetOrAllocateSegment fail, filename = "
                <<  request->filename()
                << ", offset = " << request->offset()
                << ", allocateTag = " << request->allocateifnotexist()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", GetOrAllocateSegment fail, filename = "
                <<  request->filename()
                << ", offset = " << request->offset()
                << ", allocateTag = " << request->allocateifnotexist()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
        response->clear_pagefilesegment();
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", GetOrAllocateSegment ok, filename = " << request->filename()
            << ", offset = " << request->offset()
            << ", allocateTag = " << request->allocateifnotexist();
    }
    return;
}

void NameSpaceService::RenameFile(::google::protobuf::RpcController* controller,
                         const ::curve::mds::RenameFileRequest* request,
                         ::curve::mds::RenameFileResponse* response,
                         ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    // IsRenamePathValid判断rename的路径能够加锁。不能对这种情况进行rename，
    // rename /a/b -> /b或者/a/b -> /a，一个路径不能包含另一个路径。
    if (!isPathValid(request->oldfilename())
        || !isPathValid(request->newfilename())
        || !IsRenamePathValid(request->oldfilename(), request->newfilename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
                    << ", RenameFile request path is invalid, oldfilename = "
                    << request->oldfilename()
                    << ", newfilename = " << request->newfilename();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", RenameFile request, oldfilename = " << request->oldfilename()
        << ", newfilename = " << request->newfilename();

    FileWriteLockGuard guard(fileLockManager_, request->oldfilename(),
                                               request->newfilename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->oldfilename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = "
                << request->oldfilename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = "
                << request->oldfilename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.CheckDestinationOwner(request->newfilename(),
                                             request->owner(), signature,
                                             request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING)  << "logid = " << cntl->log_id()
                        << ", CheckDestinationOwner fail, filename = "
                        <<  request->newfilename()
                        << ", owner = " << request->owner()
                        << ", statusCode = " << retCode;
        } else {
            LOG(ERROR)  << "logid = " << cntl->log_id()
                        << ", CheckDestinationOwner fail, filename = "
                        <<  request->newfilename()
                        << ", owner = " << request->owner()
                        << ", statusCode = " << retCode;
        }
        return;
    }

    // oldFileID和newFileID如果未传入，使用默认值，表示不比较file id
    uint64_t oldFileId = kUnitializedFileID;
    uint64_t newFileId = kUnitializedFileID;
    if (request->has_oldfileid()) {
        oldFileId = request->oldfileid();
    }

    if (request->has_newfileid()) {
        newFileId = request->newfileid();
    }

    retCode = kCurveFS.RenameFile(request->oldfilename(),
            request->newfilename(), oldFileId, newFileId);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", RenameFile fail, oldfilename = " << request->oldfilename()
                << ", newfilename = " << request->newfilename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", RenameFile fail, oldfilename = " << request->oldfilename()
                << ", newfilename = " << request->newfilename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", RenameFile ok, oldFileName = " << request->oldfilename()
            << ", newFileName = " << request->newfilename();
    }
    return;
}

void NameSpaceService::ExtendFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::ExtendFileRequest* request,
                    ::curve::mds::ExtendFileResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
                << ", ExtendFile request path is invalid, filename = "
                << request->filename()
                << ", newsize = " << request->newsize();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
              << ", ExtendFile request, filename = " << request->filename()
              << ", newsize = " << request->newsize();

    FileWriteLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.ExtendFile(request->filename(),
           request->newsize());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                    << ", ExtendFile fail, filename = " << request->filename()
                    << ", newsize = " << request->newsize()
                    << ", statusCode = " << retCode
                    << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                    << ", ExtendFile fail, filename = " << request->filename()
                    << ", newsize = " << request->newsize()
                    << ", statusCode = " << retCode
                    << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
           << ", ExtendFile ok, filename = " << request->filename()
           << ", newsize = " << request->newsize();
    }

    return;
}

void NameSpaceService::ChangeOwner(
                        ::google::protobuf::RpcController* controller,
                       const ::curve::mds::ChangeOwnerRequest* request,
                       ::curve::mds::ChangeOwnerResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
                << ", ChangeOwner request path is invalid, filename = "
                << request->filename()
                << ", newOwner = " << request->newowner();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
              << ", ChangeOwner request, filename = " << request->filename()
              << ", newOwner = " << request->newowner();

    FileWriteLockGuard guard(fileLockManager_, request->filename());

    StatusCode retCode;
    // ChangeOwner()接口，只允许root用户调用
    retCode = kCurveFS.CheckRootOwner(request->filename(), request->rootowner(),
                                      request->signature(), request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->rootowner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->rootowner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.ChangeOwner(request->filename(), request->newowner());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                    << ", ChangeOwner fail, filename = " << request->filename()
                    << ", newOwner = " << request->newowner()
                    << ", statusCode = " << retCode
                    << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                    << ", ChangeOwner fail, filename = " << request->filename()
                    << ", newOwner = " << request->newowner()
                    << ", statusCode = " << retCode
                    << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
           << ", ChangeOwner ok, filename = " << request->filename()
           << ", newOwner = " << request->newowner();
    }

    return;
}

void NameSpaceService::ListDir(::google::protobuf::RpcController* controller,
                       const ::curve::mds::ListDirRequest* request,
                       ::curve::mds::ListDirResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", ListDir request path is invalid, filename = "
            << request->filename();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", ListDir request, filename = " << request->filename();

    FileReadLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    std::vector<FileInfo> fileInfoList;
    retCode = kCurveFS.ReadDir(request->filename(), &fileInfoList);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", ListDir fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", ListDir fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        for (auto iter = fileInfoList.begin();
                                iter != fileInfoList.end(); ++iter) {
            FileInfo *fileinfo = response->add_fileinfo();
            fileinfo->CopyFrom(*iter);
        }
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", ListDir ok, filename = " << request->filename();
    }
    return;
}

void NameSpaceService::CreateSnapShot(
                        ::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateSnapShotRequest* request,
                       ::curve::mds::CreateSnapShotResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
                  << ", CreateSnapShot request path is invalid, filename = "
                  << request->filename();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
              << ", CreateSnapShot request, filename = "
              << request->filename();

    FileWriteLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.CreateSnapShotFile(request->filename(),
                                    response->mutable_snapshotfileinfo());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                    << ", CreateSnapShot fail, filename = "
                    << request->filename()
                    << ", statusCode = " << retCode
                    << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                    << ", CreateSnapShot fail, filename = "
                    << request->filename()
                    << ", statusCode = " << retCode
                    << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }

    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
           << ", CreateSnapShot ok, filename = " << request->filename()
           << ", seq = " << response->snapshotfileinfo().seqnum();
    }
    return;
}

void NameSpaceService::ListSnapShot(
                    ::google::protobuf::RpcController* controller,
                    const ::curve::mds::ListSnapShotFileInfoRequest* request,
                    ::curve::mds::ListSnapShotFileInfoResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
                  << ", CreateSnapShot request path is invalid, filename = "
                  << request->filename();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
              << ", ListSnapShot request, filename = "
              << request->filename();

    FileReadLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }

        return;
    }

    std::vector<FileInfo> snapShotFiles;
    retCode = kCurveFS.ListSnapShotFile(request->filename(),
                                &snapShotFiles);

    if (retCode == StatusCode::kOK) {
        auto size =  request->seq_size();
        for (int i = 0; i != size; i++) {
            auto tofindseq = request->seq(i);
            LOG(INFO) << "tofindseq = " << tofindseq;
            auto iter =
                std::find_if(snapShotFiles.begin(), snapShotFiles.end(),
                [&](const FileInfo &val){ return val.seqnum() == tofindseq; });

            if (iter != snapShotFiles.end()) {
                FileInfo *fileinfo = response->add_fileinfo();
                fileinfo->CopyFrom(*iter);
            }
        }
    }

    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                    << ", ListSnapShot fail, filename = " << request->filename()
                    << ", statusCode = " << retCode
                    << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                    << ", ListSnapShot fail, filename = " << request->filename()
                    << ", statusCode = " << retCode
                    << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", ListSnapShot ok, filename = " << request->filename()
                  << ", statusCode = " << retCode;
    }
    return;
}

void NameSpaceService::DeleteSnapShot(
                    ::google::protobuf::RpcController* controller,
                    const ::curve::mds::DeleteSnapShotRequest* request,
                    ::curve::mds::DeleteSnapShotResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
                << ", DeleteSnapShot request path is invalid, filename = "
                << request->filename()
                << ", seq = " << request->seq();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
              << ", DeleteSnapShot request, filename = "
              << request->filename()
              << ", seq = " << request->seq();

    FileWriteLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode =  kCurveFS.DeleteFileSnapShotFile(request->filename(),
                                    request->seq(), nullptr);

    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                    << ", DeleteSnapShot fail, filename = "
                    << request->filename()
                    << ", seq = " << request->seq()
                    << ", statusCode = " << retCode
                    << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                    << ", DeleteSnapShot fail, filename = "
                    << request->filename()
                    << ", seq = " << request->seq()
                    << ", statusCode = " << retCode
                    << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
        return;
    }
    response->set_statuscode(StatusCode::kOK);
    return;
}

void NameSpaceService::CheckSnapShotStatus(
                    ::google::protobuf::RpcController* controller,
                    const ::curve::mds::CheckSnapShotStatusRequest* request,
                    ::curve::mds::CheckSnapShotStatusResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckSnapShotStatus request path is invalid, filename = "
                << request->filename()
                << ", seqnum" << request->seq();
        return;
    }

    FileReadLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    FileStatus fileStatus;
    uint32_t progress;
    retCode = kCurveFS.CheckSnapShotFileStatus(request->filename(),
                        request->seq(), &fileStatus, &progress);
    if (retCode  != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckSnapShotFileStatus fail, filename = "
                <<  request->filename()
                << ", seq = " << request->seq()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckSnapShotFileStatus fail, filename = "
                <<  request->filename()
                << ", seq = " << request->seq()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }

    } else {
        response->set_statuscode(StatusCode::kOK);
        response->set_filestatus(fileStatus);
        response->set_progress(progress);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", CheckSnapShotFileStatus ok, filename = "
            <<  request->filename()
            << ", seq = " << request->seq()
            << ", statusCode = " << retCode;
    }

    return;
}

void NameSpaceService::GetSnapShotFileSegment(
                    ::google::protobuf::RpcController* controller,
                    const ::curve::mds::GetOrAllocateSegmentRequest* request,
                    ::curve::mds::GetOrAllocateSegmentResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    if ( !request->has_seqnum() ) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
              << ", GetSnapShotFileSegment, filename = "
              << request->filename()
              << ", seqnum not found";
        return;
    }

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
              << ", GetSnapShotFileSegment request path is invalid, filename = "
              << request->filename()
              << " offset = " << request->offset()
              << ", seqnum = " << request->seqnum();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
              << ", GetSnapShotFileSegment request, filename = "
              << request->filename()
              << " offset = " << request->offset()
              << ", seqnum = " << request->seqnum();

    FileReadLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.GetSnapShotFileSegment(request->filename(),
                        request->seqnum(),
                        request->offset(),
                        response->mutable_pagefilesegment());
    if (retCode  != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", GetSnapShotFileSegment fail, filename = "
                <<  request->filename()
                << ", offset = " << request->offset()
                << ", seqnum = " << request->seqnum()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", GetSnapShotFileSegment fail, filename = "
                <<  request->filename()
                << ", offset = " << request->offset()
                << ", seqnum = " << request->seqnum()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
        response->clear_pagefilesegment();
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", GetSnapShotFileSegment ok, filename = "
            <<  request->filename()
            << ", offset = " << request->offset()
            << ", seqnum = " << request->seqnum()
            << ", statusCode = " << retCode;
    }

    return;
}

void NameSpaceService::OpenFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::OpenFileRequest* request,
                    ::curve::mds::OpenFileResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::string clientIP = butil::ip2str(cntl->remote_side().ip).c_str();
    uint32_t clientPort = cntl->remote_side().port;

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
                << ", OpenFile request path is invalid, filename = "
                << request->filename()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort;
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", OpenFile request, filename = " << request->filename()
        << ", clientip = " << clientIP
        << ", clientport = " << clientPort;

    FileWriteLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    ProtoSession *protoSession = new ProtoSession();
    FileInfo *fileInfo = new FileInfo();
    retCode = kCurveFS.OpenFile(request->filename(),
                                clientIP,
                                protoSession,
                                fileInfo);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", OpenFile fail, filename = "
                <<  request->filename()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", OpenFile fail, filename = "
                <<  request->filename()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
        delete protoSession;
        delete fileInfo;
        return;
    } else {
        response->set_allocated_protosession(protoSession);
        response->set_allocated_fileinfo(fileInfo);
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", OpenFile ok, filename = " << request->filename()
            << ", clientip = " << clientIP
            << ", clientport = " << clientPort;
    }
    return;
}

void NameSpaceService::CloseFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::CloseFileRequest* request,
                    ::curve::mds::CloseFileResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::string clientIP = butil::ip2str(cntl->remote_side().ip).c_str();
    uint32_t clientPort = cntl->remote_side().port;

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CloseFile request path is invalid, filename = "
                << request->filename()
                << ", sessionid = " << request->sessionid()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort;
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", CloseFile request, filename = " << request->filename()
        << ", sessionid = " << request->sessionid()
        << ", clientip = " << clientIP
        << ", clientport = " << clientPort;

    FileWriteLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.CloseFile(request->filename(), request->sessionid());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CloseFile fail, filename = " <<  request->filename()
                << ", sessionid = " << request->sessionid()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CloseFile fail, filename = " <<  request->filename()
                << ", sessionid = " << request->sessionid()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", CloseFile ok, filename = " << request->filename()
            << ", sessionid = " << request->sessionid()
            << ", clientip = " << clientIP
            << ", clientport = " << clientPort;
    }

    return;
}

void NameSpaceService::RefreshSession(
                    ::google::protobuf::RpcController* controller,
                    const ::curve::mds::ReFreshSessionRequest* request,
                    ::curve::mds::ReFreshSessionResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::string clientIP = butil::ip2str(cntl->remote_side().ip).c_str();
    uint32_t clientPort = cntl->remote_side().port;

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        response->set_sessionid(request->sessionid());
        LOG(ERROR) << "logid = " << cntl->log_id()
                << ", RefreshSession request path is invalid, filename = "
                << request->filename()
                << ", sessionid = " << request->sessionid()
                << ", date = " << request->date()
                << ", signature = " << request->signature()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort;
        return;
    }

    DVLOG(6) << "logid = " << cntl->log_id()
        << ", RefreshSession request, filename = " << request->filename()
        << ", sessionid = " << request->sessionid()
        << ", date = " << request->date()
        << ", signature = " << request->signature()
        << ", clientip = " << clientIP
        << ", clientport = " << clientPort;

    FileReadLockGuard guard(fileLockManager_, request->filename());

    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    StatusCode retCode;
    retCode = kCurveFS.CheckFileOwner(request->filename(), request->owner(),
                                      signature, request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        response->set_sessionid(request->sessionid());
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    FileInfo *fileInfo = new FileInfo();
    ProtoSession *protoSession = new ProtoSession();
    retCode = kCurveFS.RefreshSession(request->filename(),
                                      request->sessionid(),
                                      request->date(),
                                      request->signature(),
                                      clientIP,
                                      fileInfo,
                                      protoSession);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        response->set_sessionid(request->sessionid());
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", RefreshSession fail, filename = " <<  request->filename()
                << ", sessionid = " << request->sessionid()
                << ", date = " << request->date()
                << ", signature = " << request->signature()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", RefreshSession fail, filename = " <<  request->filename()
                << ", sessionid = " << request->sessionid()
                << ", date = " << request->date()
                << ", signature = " << request->signature()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode);
        }
        delete fileInfo;
        delete protoSession;
        return;
    } else {
        response->set_sessionid(request->sessionid());
        response->set_allocated_fileinfo(fileInfo);
        response->set_allocated_protosession(protoSession);
        response->set_statuscode(StatusCode::kOK);
        DVLOG(6) << "logid = " << cntl->log_id()
            << ", RefreshSession ok, filename = " << request->filename()
            << ", sessionid = " << request->sessionid()
            << ", date = " << request->date()
            << ", signature = " << request->signature()
            << ", clientip = " << clientIP
            << ", clientport = " << clientPort;
    }

    return;
}

bool isPathValid(const std::string path) {
    if (path.empty() || path[0] != '/') {
        return false;
    }

    if (path.size() > 1U && path[path.size() - 1] == '/') {
        return false;
    }

    bool slash = false;
    for (uint32_t i = 0; i < path.size(); i++) {
        if (path[i] == '/') {
            if (slash) {
                return false;
            }
            slash = true;
        } else {
            slash = false;
        }
    }

    // 将来如果有path有其他限制，可以在此处继续添加

    return true;
}

bool IsRenamePathValid(const std::string& oldFileName,
                       const std::string& newFileName) {
    std::vector<std::string> oldFilePaths;
    ::curve::common::SplitString(oldFileName, "/", &oldFilePaths);

    std::vector<std::string> newFilePaths;
    ::curve::common::SplitString(newFileName, "/", &newFilePaths);

    // 不允许对根目录rename或者rename到根目录
    if (oldFilePaths.size() == 0 || newFilePaths.size() == 0) {
        return false;
    }

    if (oldFileName == newFileName) {
        return true;
    }

    // 不允许一个fileName包含另一个fileName
    uint32_t minSize = oldFilePaths.size() > newFilePaths.size()
                        ? newFilePaths.size() : oldFilePaths.size();
    for (uint32_t i = 0; i < minSize; i++) {
        if (oldFilePaths[i] != newFilePaths[i]) {
            return true;
        }
    }

    return false;
}

void NameSpaceService::CreateCloneFile(
                        ::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateCloneFileRequest* request,
                       ::curve::mds::CreateCloneFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);


    LOG(INFO) << "logid = " << cntl->log_id()
            << ", CreateCloneFile request, filename = " << request->filename()
            << ", filetype = " << request->filetype()
            << ", filelength = " << request->filelength()
            << ", seq = " << request->seq()
            << ", owner = " << request->owner()
            << ", chunksize = " << request->chunksize();

    // chunksize 必须得设置
    if (!request->has_chunksize()) {
        LOG(INFO) << "logid = " << cntl->log_id()
            << "CreateCloneFile error, chunksize not setted"
            << ". filename = " << request->filename();
        response->set_statuscode(StatusCode::kParaError);
        return;
    }

    // TODO(hzsunjianliang): 只允许root用户进行创建cloneFile
    std::string signature = "";
    if (request->has_signature()) {
        signature = request->signature();
    }

    FileWriteLockGuard guard(fileLockManager_, request->filename());


    // 检查权限
    StatusCode ret = kCurveFS.CheckPathOwner(request->filename(),
                                             request->owner(),
                                             signature, request->date());

    if (ret != StatusCode::kOK) {
        response->set_statuscode(ret);
        if (google::ERROR != GetMdsLogLevel(ret)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CreateCloneFile CheckPathOwner fail, filename = "
                <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << ret;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CreateCloneFile CheckPathOwner fail, filename = "
                <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << ret;
        }
        return;
    }

    // 创建clone文件
    ret = kCurveFS.CreateCloneFile(request->filename(),
                            request->owner(),
                            request->filetype(),
                            request->filelength(),
                            request->seq(),
                            request->chunksize(),
                            response->mutable_fileinfo());
    response->set_statuscode(ret);
    if (ret != StatusCode::kOK) {
        if (google::ERROR != GetMdsLogLevel(ret)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CreateCloneFile fail, filename = " <<  request->filename()
                << ", statusCode = " << ret
                << ", StatusCode_Name = " << StatusCode_Name(ret);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CreateCloneFile fail, filename = " <<  request->filename()
                << ", statusCode = " << ret
                << ", StatusCode_Name = " << StatusCode_Name(ret);
        }
    } else {
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", CreateFile ok, filename = " << request->filename();
    }
    return;
}

void NameSpaceService::SetCloneFileStatus(
                        ::google::protobuf::RpcController* controller,
                       const ::curve::mds::SetCloneFileStatusRequest* request,
                       ::curve::mds::SetCloneFileStatusResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", SetCloneFileStatus request, filename = " << request->filename()
        << ", set filestatus = " << request->filestatus()
        << ", has_fileid = " << request->has_fileid();

    uint64_t fileID = request->fileid();
    if (!request->has_fileid()) {
        fileID = kUnitializedFileID;
    }

    // TODO(hzsunjianliang): lock the filepath&name

    // TODO(hzsunjianliang): 只允许root用户进行创建cloneFile
    std::string signature = "";
    if (request->has_signature()) {
        signature = request->signature();
    }


    FileWriteLockGuard guard(fileLockManager_, request->filename());

    StatusCode ret = kCurveFS.CheckPathOwner(request->filename(),
                                             request->owner(),
                                             signature, request->date());
    if (ret != StatusCode::kOK) {
        response->set_statuscode(ret);
        if (google::ERROR != GetMdsLogLevel(ret)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", SetCloneFileStatus CheckPathOwner fail, filename = "
                <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << ret;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", SetCloneFileStatus CheckPathOwner fail, filename = "
                <<  request->filename()
                << ", owner = " << request->owner()
                << ", statusCode = " << ret;
        }
        return;
    }


    ret = kCurveFS.SetCloneFileStatus(request->filename(),
                                fileID,
                                request->filestatus());
    response->set_statuscode(ret);
    if (ret != StatusCode::kOK) {
        if (google::ERROR != GetMdsLogLevel(ret)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", SetCloneFileStatus fail, filename = "
                << request->filename()
                << ", statusCode = " << ret
                << ", StatusCode_Name = " << StatusCode_Name(ret);
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", SetCloneFileStatus fail, filename = "
                << request->filename()
                << ", statusCode = " << ret
                << ", StatusCode_Name = " << StatusCode_Name(ret);
        }
    } else {
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", SetCloneFileStatus ok, filename = " << request->filename();
    }
}

void NameSpaceService::RegistClient(
                        ::google::protobuf::RpcController* controller,
                       const ::curve::mds::RegistClientRequest* request,
                       ::curve::mds::RegistClientResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);


    LOG(INFO) << "logid = " << cntl->log_id()
        << ", RegistClient request, ip = " << request->ip()
        << ", port = " << request->port();

    StatusCode retCode;
    retCode = kCurveFS.RegistClient(request->ip(), request->port());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", RegistClient fail, ip = " << request->ip()
            << ", port = " << request->port()
            << ", statusCode = " << retCode
            << ", StatusCode_Name = " << StatusCode_Name(retCode);
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", RegistClient ok, ip = " << request->ip()
            << ", port = " << request->port();
    }
    return;
}

void NameSpaceService::GetAllocatedSize(
                        ::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetAllocatedSizeRequest* request,
                       ::curve::mds::GetAllocatedSizeResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);


    LOG(INFO) << "logid = " << cntl->log_id()
        << ", GetAllocatedSize request, fileName = " << request->filename();

    StatusCode retCode;
    uint64_t allocatedSize;
    retCode = kCurveFS.GetAllocatedSize(request->filename(), &allocatedSize);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", GetAllocatedSize fail, fileName = " << request->filename()
            << ", statusCode = " << retCode
            << ", StatusCode_Name = " << StatusCode_Name(retCode);
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        response->set_allocatedsize(allocatedSize);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", GetAllocatedSize ok, fileName = " << request->filename()
            << ", allocatedSize = " << response->allocatedsize() / kGB << "GB";
    }
    return;
}

uint32_t GetMdsLogLevel(StatusCode code) {
    switch (code) {
        case StatusCode::kSegmentNotAllocated:
        case StatusCode::kSnapshotFileNotExists:
        case StatusCode::kOK:
            return google::INFO;

        case StatusCode::kNotDirectory:
        case StatusCode::kDirNotExist:
        case StatusCode::kDirNotEmpty:
        case StatusCode::kFileUnderSnapShot:
        case StatusCode::kFileIdNotMatch:
        case StatusCode::kSessionNotExist:
        case StatusCode::kFileOccupied:
        case StatusCode::kFileNotExists:
        case StatusCode::kParaError:
            return google::WARNING;

        default:
            return google::ERROR;
    }
}
}  // namespace mds
}  // namespace curve

