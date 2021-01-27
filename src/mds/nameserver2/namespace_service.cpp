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
#include "src/mds/nameserver2/namespace_service.h"
#include <algorithm>
#include <vector>
#include <string>
#include <memory>
#include "src/mds/nameserver2/curvefs.h"
#include "src/mds/nameserver2/file_lock.h"
#include "src/common/string_util.h"
#include "src/common/timeutility.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

namespace curve {
namespace mds {

using curve::common::ExpiredTime;

void NameSpaceService::CreateFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateFileRequest* request,
                       ::curve::mds::CreateFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

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
            request->filetype(), request->filelength(), request->stripeunit(),
                                              request->stripecount());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        // TODO(hzsunjianliang): check if we should really print error here
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CreateFile fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CreateFile fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }

        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", CreateFile ok, filename = " << request->filename()
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
    }
    return;
}

void NameSpaceService::DeleteFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::DeleteFileRequest* request,
                       ::curve::mds::DeleteFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

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
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", DeleteFile fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", DeleteFile ok, filename = " << request->filename()
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
    }
    return;
}

void NameSpaceService::RecoverFile(
                       ::google::protobuf::RpcController* controller,
                       const ::curve::mds::RecoverFileRequest* request,
                       ::curve::mds::RecoverFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

    // check the filename is valid
    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", RecoverFile request path is invalid, filename = "
            << request->filename();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", RecoverFile request, filename = " << request->filename();

    // find the recoverFile in the RecycleBin
    FileInfo recoverFileInfo;
    std::string recoverFileName;
    StatusCode retCode;
    uint64_t fileId = kUnitializedFileID;
    if (request->has_fileid()) {
        fileId = request->fileid();
    }

    {
        FileReadLockGuard guard(fileLockManager_, RECYCLEBINDIR);
        retCode = kCurveFS.GetRecoverFileInfo(request->filename(), fileId,
                                              &recoverFileInfo);
        if (retCode != StatusCode::kOK) {
            response->set_statuscode(retCode);
            if (google::ERROR != GetMdsLogLevel(retCode)) {
                LOG(WARNING) << "logid = " << cntl->log_id()
                    << ", GetRecoverFileInfo fail, filename = "
                    <<  request->filename()
                    << ", owner = " << request->owner()
                    << ", statusCode = " << retCode;
            } else {
                LOG(ERROR) << "logid = " << cntl->log_id()
                    << ", GetRecoverFileInfo fail, filename = "
                    <<  request->filename()
                    << ", owner = " << request->owner()
                    << ", statusCode = " << retCode;
            }
            return;
        }
    }

    recoverFileName = RECYCLEBINDIR + "/" + recoverFileInfo.filename();
    FileWriteLockGuard guard(fileLockManager_, recoverFileName,
                             recoverFileInfo.originalfullpathname());
    std::string signature;
    if (request->has_signature()) {
        signature = request->signature();
    }

    retCode = kCurveFS.CheckRecycleFileOwner(recoverFileName,
                                      request->owner(), signature,
                                      request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " << recoverFileName
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckFileOwner fail, filename = " << recoverFileName
                << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.CheckDestinationOwner(
                                        recoverFileInfo.originalfullpathname(),
                                        request->owner(), signature,
                                        request->date());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING)  << "logid = " << cntl->log_id()
                        << ", CheckOriginFileOwner fail, filename = "
                        <<  recoverFileInfo.originalfullpathname()
                        << ", owner = " << request->owner()
                        << ", statusCode = " << retCode;
        } else {
            LOG(ERROR)  << "logid = " << cntl->log_id()
                        << ", CheckOriginFileOwner fail, filename = "
                        <<  recoverFileInfo.originalfullpathname()
                        << ", owner = " << request->owner()
                        << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.RecoverFile(request->filename(),
                                   recoverFileName, fileId);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", RecoverFile fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", RecoverFile fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", RecoverFile ok, filename = " << request->filename()
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
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
    ExpiredTime expiredTime;

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
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", GetFileInfo fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", GetFileInfo ok, filename = " << request->filename()
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
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
    ExpiredTime expiredTime;

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
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", GetOrAllocateSegment fail, filename = "
                <<  request->filename()
                << ", offset = " << request->offset()
                << ", allocateTag = " << request->allocateifnotexist()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
        response->clear_pagefilesegment();
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", GetOrAllocateSegment ok, filename = "
                  << request->filename() << ", offset = " << request->offset()
                  << ", allocateTag = " << request->allocateifnotexist()
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
    }
    return;
}

void NameSpaceService::RenameFile(::google::protobuf::RpcController* controller,
                         const ::curve::mds::RenameFileRequest* request,
                         ::curve::mds::RenameFileResponse* response,
                         ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

    // IsRenamePathValid determines whether the rename path is able to lock
    // case like this is not allow: rename /a/b -> /b or rename /a/b -> /a.
    // A path cannot include another
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

    // use default value if not passed in
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
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", RenameFile fail, oldfilename = " << request->oldfilename()
                << ", newfilename = " << request->newfilename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", RenameFile ok, oldFileName = " << request->oldfilename()
                  << ", newFileName = " << request->newfilename()
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
    }
    return;
}

void NameSpaceService::ExtendFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::ExtendFileRequest* request,
                    ::curve::mds::ExtendFileResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

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
                         << ", ExtendFile fail, filename = "
                         << request->filename()
                         << ", newsize = " << request->newsize()
                         << ", statusCode = " << retCode
                         << ", StatusCode_Name = " << StatusCode_Name(retCode)
                         << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                       << ", ExtendFile fail, filename = "
                       << request->filename()
                       << ", newsize = " << request->newsize()
                       << ", statusCode = " << retCode
                       << ", StatusCode_Name = " << StatusCode_Name(retCode)
                       << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", ExtendFile ok, filename = " << request->filename()
                  << ", newsize = " << request->newsize() << ", cost "
                  << expiredTime.ExpiredMs() << " ms";
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
    ExpiredTime expiredTime;

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
    // interface ChangeOwner() is only capable for root user
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
                         << ", ChangeOwner fail, filename = "
                         << request->filename()
                         << ", newOwner = " << request->newowner()
                         << ", statusCode = " << retCode
                         << ", StatusCode_Name = " << StatusCode_Name(retCode)
                         << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                       << ", ChangeOwner fail, filename = "
                       << request->filename()
                       << ", newOwner = " << request->newowner()
                       << ", statusCode = " << retCode
                       << ", StatusCode_Name = " << StatusCode_Name(retCode)
                       << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", ChangeOwner ok, filename = " << request->filename()
                  << ", newOwner = " << request->newowner() << ", cost "
                  << expiredTime.ExpiredMs() << " ms";
    }

    return;
}

void NameSpaceService::ListDir(::google::protobuf::RpcController* controller,
                       const ::curve::mds::ListDirRequest* request,
                       ::curve::mds::ListDirResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

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
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", ListDir fail, filename = " <<  request->filename()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
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
                  << ", ListDir ok, filename = " << request->filename()
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
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
    ExpiredTime expiredTime;

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
                         << request->filename() << ", statusCode = " << retCode
                         << ", StatusCode_Name = " << StatusCode_Name(retCode)
                         << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                       << ", CreateSnapShot fail, filename = "
                       << request->filename() << ", statusCode = " << retCode
                       << ", StatusCode_Name = " << StatusCode_Name(retCode)
                       << ", cost " << expiredTime.ExpiredMs() << " ms";
        }

    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", CreateSnapShot ok, filename = " << request->filename()
                  << ", seq = " << response->snapshotfileinfo().seqnum()
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
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
    ExpiredTime expiredTime;

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
                         << ", ListSnapShot fail, filename = "
                         << request->filename() << ", statusCode = " << retCode
                         << ", StatusCode_Name = " << StatusCode_Name(retCode)
                         << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                       << ", ListSnapShot fail, filename = "
                       << request->filename() << ", statusCode = " << retCode
                       << ", StatusCode_Name = " << StatusCode_Name(retCode)
                       << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", ListSnapShot ok, filename = " << request->filename()
                  << ", statusCode = " << retCode << ", cost "
                  << expiredTime.ExpiredMs() << " ms";
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
    ExpiredTime expiredTime;

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
                         << request->filename() << ", seq = " << request->seq()
                         << ", statusCode = " << retCode
                         << ", StatusCode_Name = " << StatusCode_Name(retCode)
                         << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                       << ", DeleteSnapShot fail, filename = "
                       << request->filename() << ", seq = " << request->seq()
                       << ", statusCode = " << retCode
                       << ", StatusCode_Name = " << StatusCode_Name(retCode)
                       << ", cost " << expiredTime.ExpiredMs() << " ms";
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
    ExpiredTime expiredTime;

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
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CheckSnapShotFileStatus fail, filename = "
                <<  request->filename()
                << ", seq = " << request->seq()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }

    } else {
        response->set_statuscode(StatusCode::kOK);
        response->set_filestatus(fileStatus);
        response->set_progress(progress);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", CheckSnapShotFileStatus ok, filename = "
                  << request->filename() << ", seq = " << request->seq()
                  << ", statusCode = " << retCode << ", cost "
                  << expiredTime.ExpiredMs() << " ms";
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
    ExpiredTime expiredTime;

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
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", GetSnapShotFileSegment fail, filename = "
                <<  request->filename()
                << ", offset = " << request->offset()
                << ", seqnum = " << request->seqnum()
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
        response->clear_pagefilesegment();
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", GetSnapShotFileSegment ok, filename = "
                  << request->filename() << ", offset = " << request->offset()
                  << ", seqnum = " << request->seqnum()
                  << ", statusCode = " << retCode << ", cost "
                  << expiredTime.ExpiredMs() << " ms";
    }

    return;
}

void NameSpaceService::OpenFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::OpenFileRequest* request,
                    ::curve::mds::OpenFileResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

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
    CloneSourceSegment* cloneSourceSegment = new CloneSourceSegment();
    retCode = kCurveFS.OpenFile(request->filename(),
                                clientIP,
                                protoSession,
                                fileInfo,
                                cloneSourceSegment);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", OpenFile fail, filename = "
                <<  request->filename()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", OpenFile fail, filename = "
                <<  request->filename()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
        delete protoSession;
        delete fileInfo;
        delete cloneSourceSegment;
        return;
    } else {
        response->set_allocated_protosession(protoSession);
        response->set_allocated_fileinfo(fileInfo);
        response->set_statuscode(StatusCode::kOK);

        if (cloneSourceSegment->IsInitialized()) {
            response->set_allocated_clonesourcesegment(cloneSourceSegment);
        } else {
            delete cloneSourceSegment;
        }

        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", OpenFile ok, filename = " << request->filename()
                  << ", clientip = " << clientIP
                  << ", clientport = " << clientPort
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
    }
    return;
}

void NameSpaceService::CloseFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::CloseFileRequest* request,
                    ::curve::mds::CloseFileResponse* response,
                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

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
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CloseFile fail, filename = " <<  request->filename()
                << ", sessionid = " << request->sessionid()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", CloseFile ok, filename = " << request->filename()
                  << ", sessionid = " << request->sessionid()
                  << ", clientip = " << clientIP
                  << ", clientport = " << clientPort << ", cost "
                  << expiredTime.ExpiredMs() << " ms";
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
    ExpiredTime expiredTime;

    std::string clientIP = butil::ip2str(cntl->remote_side().ip).c_str();
    std::string clientVersion;
    if (request->has_clientversion()) {
        clientVersion = request->clientversion();
    }
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
    retCode = kCurveFS.RefreshSession(
        request->filename(),
        request->sessionid(),
        request->date(),
        request->signature(),
        request->has_clientip() ? request->clientip() : clientIP,
        request->has_clientport() ? request->clientport() : kInvalidPort,
        clientVersion,
        fileInfo);
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
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost = " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", RefreshSession fail, filename = " <<  request->filename()
                << ", sessionid = " << request->sessionid()
                << ", date = " << request->date()
                << ", signature = " << request->signature()
                << ", clientip = " << clientIP
                << ", clientport = " << clientPort
                << ", statusCode = " << retCode
                << ", StatusCode_Name = " << StatusCode_Name(retCode)
                << ", cost = " << expiredTime.ExpiredMs() << " ms";
        }
        delete fileInfo;
        return;
    } else {
        response->set_sessionid(request->sessionid());
        response->set_allocated_fileinfo(fileInfo);
        response->set_statuscode(StatusCode::kOK);
        DVLOG(6) << "logid = " << cntl->log_id()
            << ", RefreshSession ok, filename = " << request->filename()
            << ", sessionid = " << request->sessionid()
            << ", date = " << request->date()
            << ", signature = " << request->signature()
            << ", clientip = " << clientIP
            << ", clientport = " << clientPort
            << ", cost = " << expiredTime.ExpiredMs() << " ms";
    }

    return;
}

bool IsRenamePathValid(const std::string& oldFileName,
                       const std::string& newFileName) {
    std::vector<std::string> oldFilePaths;
    ::curve::common::SplitString(oldFileName, "/", &oldFilePaths);

    std::vector<std::string> newFilePaths;
    ::curve::common::SplitString(newFileName, "/", &newFilePaths);

    // rename to the root directory or rename to the root directory
    // is not allowed
    if (oldFilePaths.size() == 0 || newFilePaths.size() == 0) {
        return false;
    }

    if (oldFileName == newFileName) {
        return true;
    }

    // do not allow one fileName to contain another
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
    ExpiredTime expiredTime;

    LOG(INFO) << "logid = " << cntl->log_id()
            << ", CreateCloneFile request, filename = " << request->filename()
            << ", filetype = " << request->filetype()
            << ", filelength = " << request->filelength()
            << ", seq = " << request->seq()
            << ", owner = " << request->owner()
            << ", chunksize = " << request->chunksize();

    // chunksize must be set
    if (!request->has_chunksize()) {
        LOG(INFO) << "logid = " << cntl->log_id()
            << "CreateCloneFile error, chunksize not setted"
            << ". filename = " << request->filename();
        response->set_statuscode(StatusCode::kParaError);
        return;
    }

    // TODO(hzsunjianliang): only root user is allowed to create cloneFile
    std::string signature = "";
    if (request->has_signature()) {
        signature = request->signature();
    }

    FileWriteLockGuard guard(fileLockManager_, request->filename());


    // check authority
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

    // create clone file
    ret = kCurveFS.CreateCloneFile(request->filename(),
                            request->owner(),
                            request->filetype(),
                            request->filelength(),
                            request->seq(),
                            request->chunksize(),
                            response->mutable_fileinfo(),
                            request->clonesource(),
                            request->filelength());
    response->set_statuscode(ret);
    if (ret != StatusCode::kOK) {
        if (google::ERROR != GetMdsLogLevel(ret)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                << ", CreateCloneFile fail, filename = " <<  request->filename()
                << ", statusCode = " << ret
                << ", StatusCode_Name = " << StatusCode_Name(ret)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", CreateCloneFile fail, filename = " <<  request->filename()
                << ", statusCode = " << ret
                << ", StatusCode_Name = " << StatusCode_Name(ret)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
    } else {
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", CreateFile ok, filename = " << request->filename()
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
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
    ExpiredTime expiredTime;

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", SetCloneFileStatus request, filename = " << request->filename()
        << ", set filestatus = " << request->filestatus()
        << ", has_fileid = " << request->has_fileid();

    uint64_t fileID = request->fileid();
    if (!request->has_fileid()) {
        fileID = kUnitializedFileID;
    }

    // TODO(hzsunjianliang): lock the filepath&name
    // TODO(hzsunjianliang): only root user is allowed to create cloneFile
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
                << ", StatusCode_Name = " << StatusCode_Name(ret)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                << ", SetCloneFileStatus fail, filename = "
                << request->filename()
                << ", statusCode = " << ret
                << ", StatusCode_Name = " << StatusCode_Name(ret)
                << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
    } else {
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", SetCloneFileStatus ok, filename = "
                  << request->filename() << ", cost "
                  << expiredTime.ExpiredMs() << " ms";
    }
}

void NameSpaceService::GetAllocatedSize(
                        ::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetAllocatedSizeRequest* request,
                       ::curve::mds::GetAllocatedSizeResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", GetAllocatedSize request, fileName = " << request->filename();

    StatusCode retCode;
    AllocatedSize allocSize;
    retCode = kCurveFS.GetAllocatedSize(request->filename(), &allocSize);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
                   << ", GetAllocatedSize fail, fileName = "
                   << request->filename() << ", statusCode = " << retCode
                   << ", StatusCode_Name = " << StatusCode_Name(retCode)
                   << ", cost " << expiredTime.ExpiredMs() << " ms";
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        response->set_allocatedsize(allocSize.total);
        for (const auto& item : allocSize.allocSizeMap) {
            response->mutable_allocsizemap()->insert({item.first, item.second});
        }
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", GetAllocatedSize ok, fileName = " << request->filename()
                  << ", allocatedSize = " << response->allocatedsize() / kGB
                  << "GB"
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
    }
    return;
}

void NameSpaceService::GetFileSize(
                        ::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetFileSizeRequest* request,
                       ::curve::mds::GetFileSizeResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);


    LOG(INFO) << "logid = " << cntl->log_id()
        << ", GetFileSize request, fileName = " << request->filename();

    StatusCode retCode;
    uint64_t fileSize = 0;
    retCode = kCurveFS.GetFileSize(request->filename(), &fileSize);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", GetFileSize fail, fileName = " << request->filename()
            << ", statusCode = " << retCode
            << ", StatusCode_Name = " << StatusCode_Name(retCode);
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        response->set_filesize(fileSize);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", GetFileSize ok, fileName = " << request->filename()
            << ", fileSize = " << response->filesize() / kGB << "GB";
    }
    return;
}

void NameSpaceService::ListClient(
                        ::google::protobuf::RpcController* controller,
                        const ::curve::mds::ListClientRequest* request,
                        ::curve::mds::ListClientResponse* response,
                        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

    LOG(INFO) << "logid = " << cntl->log_id()
              << ", ListClient request = " << request->ShortDebugString();

    StatusCode retCode;
    std::vector<ClientInfo> clientInfos;
    bool listAllClient =
        request->has_listallclient() && request->listallclient();
    retCode = kCurveFS.ListClient(listAllClient, &clientInfos);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id() << ", ListClient fail, "
                   << ", statusCode = " << retCode
                   << ", StatusCode_Name = " << StatusCode_Name(retCode)
                   << ", cost " << expiredTime.ExpiredMs() << " ms";
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        for (const auto& info : clientInfos) {
            ClientInfo* clientInfo = response->add_clientinfos();
            *clientInfo = info;
        }
        LOG(INFO) << "logid = " << cntl->log_id() << ", ListClient ok, "
                  << ", return " << response->clientinfos_size()
                  << " client infos"
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
    }
}

void NameSpaceService::FindFileMountPoint(
    ::google::protobuf::RpcController* controller,
    const ::curve::mds::FindFileMountPointRequest* request,
    ::curve::mds::FindFileMountPointResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

    LOG(INFO) << "logid = " << cntl->log_id()
              << ", FindFileMountPoint request, fileName = "
              << request->filename();

    StatusCode retCode;
    std::unique_ptr<ClientInfo> clientInfo(new ClientInfo());
    retCode =
        kCurveFS.FindFileMountPoint(request->filename(), clientInfo.get());

    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
                   << ", FindFileMountPoint fail, fileName = "
                   << request->filename() << ", statusCode = " << retCode
                   << ", StatusCode_Name = " << StatusCode_Name(retCode)
                   << ", cost " << expiredTime.ExpiredMs() << " ms";
        return;
    } else {
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", FindFileMountPoint ok, fileName = "
                  << request->filename() << ", client info = "
                  << response->clientinfo().ShortDebugString()
                  << ", cost " << expiredTime.ExpiredMs() << " ms";
        response->set_statuscode(StatusCode::kOK);
        response->set_allocated_clientinfo(clientInfo.release());
    }
    return;
}

void NameSpaceService::ListVolumesOnCopysets(
                ::google::protobuf::RpcController* controller,
                const ::curve::mds::ListVolumesOnCopysetsRequest* request,
                ::curve::mds::ListVolumesOnCopysetsResponse* response,
                ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "logid = " << cntl->log_id()
              << ", ListAllVolumesOnCopysets request";

    std::vector<std::string> fileNames;
    std::vector<common::CopysetInfo> copysets;
    for (int i = 0; i < request->copysets_size(); i++) {
        copysets.emplace_back(request->copysets(i));
    }

    auto retCode = kCurveFS.ListVolumesOnCopyset(copysets, &fileNames);
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", ListAllVolumesOnCopysets fail, statusCode = "
            << retCode << ", StatusCode_Name = " << StatusCode_Name(retCode);
        return;
    }
    response->set_statuscode(StatusCode::kOK);
    for (const auto& fileName : fileNames) {
        response->add_filenames(fileName);
    }
    LOG(INFO) << "logid = " << cntl->log_id()
              << ", ListAllVolumesOnCopysets ok, volume num: "
              << fileNames.size();
}

void NameSpaceService::UpdateFileThrottleParams(
    ::google::protobuf::RpcController* controller,
    const ::curve::mds::UpdateFileThrottleParamsRequest* request,
    ::curve::mds::UpdateFileThrottleParamsResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    ExpiredTime expiredTime;

    if (!isPathValid(request->filename())) {
        response->set_statuscode(StatusCode::kParaError);
        LOG(ERROR)
            << "logid = " << cntl->log_id()
            << ", UpdateFileThrottleParams request path is invalid, filename = "
            << request->filename();
        return;
    }

    LOG(INFO) << "logid = " << cntl->log_id()
              << ", UpdateFileThrottleParams request, filename = "
              << request->filename() << ", update "
              << ThrottleType_Name(request->throttleparams().type()) << " to "
              << request->throttleparams().ShortDebugString();

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
            LOG(WARNING)
                << "logid = " << cntl->log_id()
                << ", UpdateFileThrottleParams CheckFileOwner fail, filename = "
                << request->filename() << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        } else {
            LOG(ERROR)
                << "logid = " << cntl->log_id()
                << ", UpdateFileThrottleParams CheckFileOwner fail, filename = "
                << request->filename() << ", owner = " << request->owner()
                << ", statusCode = " << retCode;
        }
        return;
    }

    retCode = kCurveFS.UpdateFileThrottleParams(request->filename(),
                                                request->throttleparams());
    if (retCode != StatusCode::kOK) {
        response->set_statuscode(retCode);
        if (google::ERROR != GetMdsLogLevel(retCode)) {
            LOG(WARNING) << "logid = " << cntl->log_id()
                         << ", UpdateFileThrottleParams fail, filename = "
                         << request->filename() << ", statusCode = " << retCode
                         << ", StatusCode_Name = " << StatusCode_Name(retCode)
                         << ", cost " << expiredTime.ExpiredMs() << " ms";
        } else {
            LOG(ERROR) << "logid = " << cntl->log_id()
                       << ", UpdateFileThrottleParams fail, filename = "
                       << request->filename() << ", statusCode = " << retCode
                       << ", StatusCode_Name = " << StatusCode_Name(retCode)
                       << ", cost " << expiredTime.ExpiredMs() << " ms";
        }
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
                  << ", UpdateFileThrottleParams ok, filename = "
                  << request->filename() << ", cost " << expiredTime.ExpiredMs()
                  << " ms";
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
        case StatusCode::kFileExists:
        case StatusCode::kFileNotExists:
        case StatusCode::kParaError:
        case StatusCode::kDeleteFileBeingCloned:
        case StatusCode::kFileUnderDeleting:
            return google::WARNING;

        default:
            return google::ERROR;
    }
}
}  // namespace mds
}  // namespace curve
