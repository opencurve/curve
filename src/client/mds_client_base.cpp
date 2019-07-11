/*
 * Project: curve
 * File Created: Friday, 21st June 2019 10:20:57 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include "src/client/mds_client_base.h"

const char* kRootUserName = "root";

namespace curve {
namespace client {
int MDSClientBase::Init(const MetaServerOption_t& metaServerOpt) {
    metaServerOpt_ = metaServerOpt;
    return 0;
}

void MDSClientBase::OpenFile(const std::string& filename,
                            const UserInfo_t& userinfo,
                            OpenFileResponse* response,
                            brpc::Controller* cntl,
                            brpc::Channel* channel) {
    OpenFileRequest request;
    request.set_filename(filename);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<OpenFileRequest>(&request, userinfo);

    LOG(INFO) << "OpenFile: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.OpenFile(cntl, &request, response, nullptr);
}

void MDSClientBase::CreateFile(const std::string& filename,
                            const UserInfo_t& userinfo,
                            size_t size,
                            bool normalFile,
                            CreateFileResponse* response,
                            brpc::Controller* cntl,
                            brpc::Channel* channel) {
    CreateFileRequest request;
    request.set_filename(filename);
    if (normalFile) {
        request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        request.set_filelength(size);
    } else {
        request.set_filetype(curve::mds::FileType::INODE_DIRECTORY);
    }

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<CreateFileRequest>(&request, userinfo);

    LOG(INFO) << "CreateFile: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", is nomalfile: " << normalFile
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.CreateFile(cntl, &request, response, NULL);
}

void MDSClientBase::CloseFile(const std::string& filename,
                            const UserInfo_t& userinfo,
                            const std::string& sessionid,
                            CloseFileResponse* response,
                            brpc::Controller* cntl,
                            brpc::Channel* channel) {
    CloseFileRequest request;
    request.set_filename(filename);
    request.set_sessionid(sessionid);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<curve::mds::CloseFileRequest>(&request, userinfo);

    LOG(INFO) << "CloseFile: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", sessionid = " << sessionid
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.CloseFile(cntl, &request, response, nullptr);
}

void MDSClientBase::GetFileInfo(const std::string& filename,
                                const UserInfo_t& userinfo,
                                GetFileInfoResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    GetFileInfoRequest request;
    request.set_filename(filename);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<curve::mds::GetFileInfoRequest>(&request, userinfo);

    LOG(INFO) << "GetFileInfo: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.GetFileInfo(cntl, &request, response, nullptr);
}

void MDSClientBase::CreateSnapShot(const std::string& filename,
                                const UserInfo_t& userinfo,
                                CreateSnapShotResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    CreateSnapShotRequest request;
    request.set_filename(filename);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<::curve::mds::CreateSnapShotRequest>(&request, userinfo);

    LOG(INFO) << "CreateSnapShot: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.CreateSnapShot(cntl, &request, response, nullptr);
}

void MDSClientBase::DeleteSnapShot(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t seq,
                                DeleteSnapShotResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    DeleteSnapShotRequest request;;
    request.set_seq(seq);
    request.set_filename(filename);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<::curve::mds::DeleteSnapShotRequest>(&request, userinfo);

    LOG(INFO) << "DeleteSnapShot: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", seqnum = " << seq
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.DeleteSnapShot(cntl, &request, response, nullptr);
}

void MDSClientBase::ListSnapShot(const std::string& filename,
                                const UserInfo_t& userinfo,
                                const std::vector<uint64_t>* seq,
                                ListSnapShotFileInfoResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    ListSnapShotFileInfoRequest request;
    for (unsigned int i = 0; i < (*seq).size(); i++) {
        request.add_seq((*seq)[i]);
    }
    request.set_filename(filename);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<ListSnapShotFileInfoRequest>(&request, userinfo);

    LOG(INFO) << "ListSnapShot: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", seqnum = " << seq
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.ListSnapShot(cntl, &request, response, nullptr);
}

void MDSClientBase::GetSnapshotSegmentInfo(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t seq,
                                uint64_t offset,
                                GetOrAllocateSegmentResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    GetOrAllocateSegmentRequest request;
    request.set_filename(filename);
    request.set_offset(offset);
    request.set_allocateifnotexist(false);
    request.set_seqnum(seq);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<GetOrAllocateSegmentRequest>(&request, userinfo);

    LOG(INFO) << "GetSnapshotSegmentInfo: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", offset = " << offset
                << ", seqnum = " << seq;

    curve::mds::CurveFSService_Stub stub(channel);
    stub.GetSnapShotFileSegment(cntl, &request, response, nullptr);
}

void MDSClientBase::RefreshSession(const std::string& filename,
                                const UserInfo_t& userinfo,
                                const std::string& sessionid,
                                ReFreshSessionResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    ReFreshSessionRequest request;
    request.set_filename(filename);
    request.set_sessionid(sessionid);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
    FillUserInfo<ReFreshSessionRequest>(&request, userinfo);

    LOG_EVERY_N(INFO, 10) << "RefreshSession: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", sessionid = " << sessionid;

    curve::mds::CurveFSService_Stub stub(channel);
    stub.RefreshSession(cntl, &request, response, nullptr);
}

void MDSClientBase::CheckSnapShotStatus(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t seq,
                                CheckSnapShotStatusResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    CheckSnapShotStatusRequest request;
    request.set_seq(seq);
    request.set_filename(filename);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<CheckSnapShotStatusRequest>(&request, userinfo);

    LOG(INFO) << "CheckSnapShotStatus: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", seqnum = " << seq;

    curve::mds::CurveFSService_Stub stub(channel);
    stub.CheckSnapShotStatus(cntl, &request, response, nullptr);
}

void MDSClientBase::GetServerList(const LogicPoolID& logicalpooid,
                                const std::vector<CopysetID>& copysetidvec,
                                GetChunkServerListInCopySetsResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    GetChunkServerListInCopySetsRequest request;
    request.set_logicalpoolid(logicalpooid);
    for (auto copysetid : copysetidvec) {
        request.add_copysetid(copysetid);
    }

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.rpcTimeoutMs);

    curve::mds::topology::TopologyService_Stub stub(channel);
    stub.GetChunkServerListInCopySets(cntl, &request, response, nullptr);
}

void MDSClientBase::CreateCloneFile(const std::string &destination,
                                const UserInfo_t& userinfo,
                                uint64_t size,
                                uint64_t sn,
                                uint32_t chunksize,
                                CreateCloneFileResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    CreateCloneFileRequest request;
    request.set_seq(sn);
    request.set_filelength(size);
    request.set_filename(destination);
    request.set_chunksize(chunksize);
    request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<CreateCloneFileRequest>(&request, userinfo);

    LOG(INFO) << "CreateCloneFile: destination = " << destination
              << ", owner = " << userinfo.owner.c_str()
              << ", seqnum = " << sn
              << ", size = " << size
              << ", chunksize = " << chunksize;

    curve::mds::CurveFSService_Stub stub(channel);
    stub.CreateCloneFile(cntl, &request, response, NULL);
}

void MDSClientBase::SetCloneFileStatus(const std::string &filename,
                                const FileStatus& filestatus,
                                const UserInfo_t& userinfo,
                                uint64_t fileID,
                                SetCloneFileStatusResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    SetCloneFileStatusRequest request;
    request.set_filename(filename);
    request.set_filestatus(static_cast<curve::mds::FileStatus>(filestatus));
    if (fileID > 0) {
        request.set_fileid(fileID);
    }

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<SetCloneFileStatusRequest>(&request, userinfo);

    LOG(INFO) << "CreateCloneFile: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner.c_str()
                << ", filestatus = " << static_cast<int>(filestatus)
                << ", fileID = " << fileID
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.SetCloneFileStatus(cntl, &request, response, NULL);
}

void MDSClientBase::GetOrAllocateSegment(bool allocate,
                                const UserInfo_t& userinfo,
                                uint64_t offset,
                                const FInfo_t* fi,
                                GetOrAllocateSegmentResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    GetOrAllocateSegmentRequest request;

    // convert the user offset to seg  offset
    uint64_t segmentsize = fi->segmentsize;
    uint64_t chunksize = fi->chunksize;
    uint64_t seg_offset = (offset / segmentsize) * segmentsize;

    request.set_filename(fi->fullPathName);
    request.set_offset(seg_offset);
    request.set_allocateifnotexist(allocate);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
    FillUserInfo<GetOrAllocateSegmentRequest>(&request, userinfo);

    LOG(INFO) << "GetOrAllocateSegment: allocate = " << allocate
                << ", owner = " << userinfo.owner.c_str()
                << ", offset = " << offset
                << ", segment offset = " << seg_offset
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.GetOrAllocateSegment(cntl, &request, response, NULL);
}

void MDSClientBase::RenameFile(const UserInfo_t& userinfo,
                                const std::string &origin,
                                const std::string &destination,
                                uint64_t originId,
                                uint64_t destinationId,
                                RenameFileResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    RenameFileRequest request;
    request.set_oldfilename(origin);
    request.set_newfilename(destination);
    if (originId > 0 && destinationId > 0) {
        request.set_oldfileid(originId);
        request.set_newfileid(destinationId);
    }

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<RenameFileRequest>(&request, userinfo);

    LOG(INFO) << "RenameFile: origin = " << origin.c_str()
              << ", destination = " << destination.c_str()
              << ", originId = " << originId
              << ", destinationId = " << destinationId
              << ", owner = " << userinfo.owner.c_str()
              << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.RenameFile(cntl, &request, response, NULL);
}

void MDSClientBase::Extend(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t newsize,
                                ExtendFileResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    ExtendFileRequest request;
    request.set_filename(filename);
    request.set_newsize(newsize);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<ExtendFileRequest>(&request, userinfo);

    LOG(INFO) << "Extend: filename = " << filename.c_str()
              << ", owner = " << userinfo.owner.c_str()
              << ", newsize = " << newsize
              << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.ExtendFile(cntl, &request, response, NULL);
}

void MDSClientBase::DeleteFile(const std::string& filename,
                                const UserInfo_t& userinfo,
                                bool deleteforce,
                                uint64_t fileid,
                                DeleteFileResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    DeleteFileRequest request;
    request.set_filename(filename);
    request.set_forcedelete(deleteforce);
    if (fileid > 0) {
        request.set_fileid(fileid);
    }

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<DeleteFileRequest>(&request, userinfo);

    LOG(INFO) << "DeleteFile: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner.c_str()
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.DeleteFile(cntl, &request, response, NULL);
}

void MDSClientBase::ChangeOwner(const std::string& filename,
                                const std::string& newOwner,
                                const UserInfo_t& userinfo,
                                ChangeOwnerResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    curve::mds::ChangeOwnerRequest request;
    uint64_t date = curve::common::TimeUtility::GetTimeofDayUs();
    request.set_date(date);
    request.set_filename(filename);
    request.set_newowner(newOwner);
    request.set_rootowner(userinfo.owner);
    if (!userinfo.owner.compare(kRootUserName)&&userinfo.password.compare("")) {
        std::string str2sig = Authenticator::GetString2Signature(date,
                                                    userinfo.owner);
        std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                    userinfo.password);
        request.set_signature(sig);
    }

    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    cntl->set_log_id(GetLogId());

    LOG(INFO) << "ChangeOwner: filename = " << filename.c_str()
                << ", operator owner = " << userinfo.owner.c_str()
                << ", new owner = " << newOwner.c_str()
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.ChangeOwner(cntl, &request, response, NULL);
}

void MDSClientBase::Listdir(const std::string& dirpath,
                                const UserInfo_t& userinfo,
                                ListDirResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel) {
    curve::mds::ListDirRequest request;
    request.set_filename(dirpath);

    cntl->set_log_id(GetLogId());
    cntl->set_timeout_ms(metaServerOpt_.synchronizeRPCTimeoutMS);
    FillUserInfo<::curve::mds::ListDirRequest>(&request, userinfo);

    LOG(INFO) << "Listdir: filename = " << dirpath.c_str()
                << ", owner = " << userinfo.owner.c_str()
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.ListDir(cntl, &request, response, NULL);
}


}   // namespace client
}   // namespace curve
