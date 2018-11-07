/*
 * Project: curve
 * Created Date: Tuesday September 25th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include "src/mds/nameserver2/namespace_service.h"
#include "src/mds/nameserver2/curvefs.h"

namespace curve {
namespace mds {

void NameSpaceService::CreateFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateFileRequest* request,
                       ::curve::mds::CreateFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", CreateFile request, filename = " << request->filename()
        << ", filetype = " << request->filetype()
        << ", filelength = " << request->filelength();

    // TODO(hzsunjialiang): lock the filepath&name and do check permission
    StatusCode retCode = kCurveFS.CreateFile(request->filename(),
            request->filetype(), request->filelength());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", CreateFile fail, filename = " <<  request->filename()
            << ", statusCode = " << retCode;
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", CreateFile ok, filename = " << request->filename();
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

    LOG(ERROR) << "logid = " << cntl->log_id()
        << ", GetFileInfo request, filename = " << request->filename();

    // TODO(hzsunjialiang): lock the filepath&name and do check permission
    StatusCode retCode;
    retCode = kCurveFS.GetFileInfo(request->filename(),
        response->mutable_fileinfo());

    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", GetFileInfo fail, filename = " <<  request->filename()
            << ", statusCode = " << retCode;
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

    LOG(ERROR) << "logid = " << cntl->log_id()
        << ", GetOrAllocateSegment request, filename = " << request->filename()
        << ", offset = " << request->offset() << ", allocateTag = "
        << request->allocateifnotexist();

    // TODO(hzsunjialiang): lock the filepath&name and do check permission

    StatusCode retCode;
    retCode = kCurveFS.GetOrAllocateSegment(request->filename(),
                request->offset(),
                request->allocateifnotexist(),
                response->mutable_pagefilesegment());

    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", GetOrAllocateSegment fail, filename = "
            <<  request->filename()
            << ", offset = " << request->offset()
            << ", allocateTag = " << request->allocateifnotexist()
            << ", statusCode = " << retCode;
        return;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", GetOrAllocateSegment ok, filename = " << request->filename()
            << ", offset = " << request->offset()
            << ", allocateTag = " << request->allocateifnotexist();
    }
    return;
}

void NameSpaceService::DeleteSegment(::google::protobuf::RpcController* controller,
                         const ::curve::mds::DeleteSegmentRequest* request,
                         ::curve::mds::DeleteSegmentResponse* response,
                         ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", DeleteSegment request, filename = " << request->filename()
        << ", offset = " << request->offset();

    // TODO(hzsunjialiang): lock the filepath&name and do check permission
    StatusCode retCode = kCurveFS.DeleteSegment(request->filename(),
            request->offset());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", DeleteSegment fail, filename = " << request->filename()
            << ", offset = " << request->offset()
            << ", statusCode = " << retCode;
    } else {
        response->set_statuscode(StatusCode::kOK);
        LOG(INFO) << "logid = " << cntl->log_id()
            << ", DeleteSegment ok, filename = " << request->filename()
            << ", offset = " << request->offset();
    }

    return;
}
void NameSpaceService::RenameFile(::google::protobuf::RpcController* controller,
                         const ::curve::mds::RenameFileRequest* request,
                         ::curve::mds::RenameFileResponse* response,
                         ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id()
        << ", RenameFile request, oldfilename = " << request->oldfilename()
        << ", newfilename = " << request->newfilename();

    // TODO(hzsunjialiang): lock the filepath&name and do check permission
    StatusCode retCode = kCurveFS.RenameFile(request->oldfilename(),
            request->newfilename());
    if (retCode != StatusCode::kOK)  {
        response->set_statuscode(retCode);
        LOG(ERROR) << "logid = " << cntl->log_id()
            << ", RenameFile fail, oldfilename = " << request->oldfilename()
            << ", newfilename = " << request->newfilename()
            << ", statusCode = " << retCode;
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

    LOG(INFO) << "logid = " << cntl->log_id()
              << ", ExtendFile request, filename = " << request->filename()
              << ", newsize = " << request->newsize();

    // TODO(hzsunjialiang): lock the filepath&name and do check permission
    StatusCode retCode = kCurveFS.ExtendFile(request->filename(),
           request->newsize());
    if (retCode != StatusCode::kOK)  {
       response->set_statuscode(retCode);
       LOG(ERROR) << "logid = " << cntl->log_id()
                  << ", ExtendFile fail, filename = " << request->filename()
                  << ", newsize = " << request->newsize()
                  << ", statusCode = " << retCode;
    } else {
       response->set_statuscode(StatusCode::kOK);
       LOG(INFO) << "logid = " << cntl->log_id()
           << ", ExtendFile ok, filename = " << request->filename()
           << ", newsize = " << request->newsize();
    }

    return;
}


}  // namespace mds
}  // namespace curve

