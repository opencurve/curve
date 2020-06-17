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
 * File Created: Friday, 21st June 2019 10:20:57 am
 * Author: tongguangxun
 */

#include "src/client/mds_client_base.h"
#include "src/common/curve_version.h"

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
    request.set_clientversion(curve::common::CurveVersion());
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
    FillUserInfo<ListSnapShotFileInfoRequest>(&request, userinfo);

    LOG(INFO) << "ListSnapShot: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", seqnum = " << [seq] () {
                    std::string data("[ ");
                    for (uint64_t v : *seq) {
                        data += std::to_string(v);
                        data += " ";
                    }
                    data += "]";
                    return data;
                } ()
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
    FillUserInfo<GetOrAllocateSegmentRequest>(&request, userinfo);

    LOG(INFO) << "GetSnapshotSegmentInfo: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", offset = " << offset
                << ", seqnum = " << seq
                << ", log id = " << cntl->log_id();

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
    request.set_clientversion(curve::common::CurveVersion());

    static ClientDummyServerInfo& clientInfo =
        ClientDummyServerInfo::GetInstance();

    if (clientInfo.GetRegister()) {
        request.set_clientip(clientInfo.GetIP());
        request.set_clientport(clientInfo.GetPort());
    }

    FillUserInfo<ReFreshSessionRequest>(&request, userinfo);

    LOG_EVERY_N(INFO, 10) << "RefreshSession: filename = " << filename.c_str()
                          << ", owner = " << userinfo.owner
                          << ", sessionid = " << sessionid
                          << ", log id = " << cntl->log_id();

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
    FillUserInfo<CheckSnapShotStatusRequest>(&request, userinfo);

    LOG(INFO) << "CheckSnapShotStatus: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", seqnum = " << seq
                << ", log id = " << cntl->log_id();

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
    std::string requestCopysets;
    for (auto copysetid : copysetidvec) {
        request.add_copysetid(copysetid);
        requestCopysets.append(std::to_string(copysetid)).append(" ");
    }

    curve::mds::topology::TopologyService_Stub stub(channel);
    stub.GetChunkServerListInCopySets(cntl, &request, response, nullptr);
}

void MDSClientBase::GetClusterInfo(GetClusterInfoResponse* response,
                                   brpc::Controller* cntl,
                                   brpc::Channel* channel) {
    GetClusterInfoRequest request;

    curve::mds::topology::TopologyService_Stub stub(channel);
    stub.GetClusterInfo(cntl, &request, response, nullptr);
}

void MDSClientBase::CreateCloneFile(const std::string& source,
                                    const std::string& destination,
                                    const UserInfo_t& userinfo, uint64_t size,
                                    uint64_t sn, uint32_t chunksize,
                                    CreateCloneFileResponse* response,
                                    brpc::Controller* cntl,
                                    brpc::Channel* channel) {
    CreateCloneFileRequest request;
    request.set_seq(sn);
    request.set_filelength(size);
    request.set_filename(destination);
    request.set_chunksize(chunksize);
    request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    request.set_clonesource(source);
    FillUserInfo<CreateCloneFileRequest>(&request, userinfo);

    LOG(INFO) << "CreateCloneFile: source = " << source
              << ", destination = " << destination
              << ", owner = " << userinfo.owner.c_str() << ", seqnum = " << sn
              << ", size = " << size << ", chunksize = " << chunksize
              << ", log id = " << cntl->log_id();

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
    FillUserInfo<SetCloneFileStatusRequest>(&request, userinfo);

    LOG(INFO) << "SetCloneFileStatus: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner.c_str()
                << ", filestatus = " << static_cast<int>(filestatus)
                << ", fileID = " << fileID
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.SetCloneFileStatus(cntl, &request, response, NULL);
}

void MDSClientBase::GetOrAllocateSegment(bool allocate,
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
    FillUserInfo<GetOrAllocateSegmentRequest>(&request, fi->userinfo);

    LOG(INFO) << "GetOrAllocateSegment: allocate = " << allocate
                << ", owner = " << fi->owner.c_str()
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
    request.set_signature(CalcSignature(userinfo, date));

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

    FillUserInfo<::curve::mds::ListDirRequest>(&request, userinfo);

    LOG(INFO) << "Listdir: filename = " << dirpath.c_str()
                << ", owner = " << userinfo.owner.c_str()
                << ", log id = " << cntl->log_id();

    curve::mds::CurveFSService_Stub stub(channel);
    stub.ListDir(cntl, &request, response, NULL);
}

void MDSClientBase::GetChunkServerInfo(const std::string& ip,
    uint16_t port, GetChunkServerInfoResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
    curve::mds::topology::GetChunkServerInfoRequest request;
    request.set_hostip(ip);
    request.set_port(port);
    LOG(INFO) << "GetChunkServerInfo from mds: "
              << "ip = " << ip
              << ", port = " << port
              << ", log id = " << cntl->log_id();

    curve::mds::topology::TopologyService_Stub stub(channel);
    stub.GetChunkServer(cntl, &request, response, NULL);
}

void MDSClientBase::ListChunkServerInServer(
    const std::string& ip, ListChunkServerResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
    curve::mds::topology::ListChunkServerRequest request;
    request.set_ip(ip);
    LOG(INFO) << "ListChunkServerInServer from mds: "
        << "ip = " << ip
        << ", log id = " << cntl->log_id();

    curve::mds::topology::TopologyService_Stub stub(channel);
    stub.ListChunkServer(cntl, &request, response, NULL);
}

std::string MDSClientBase::CalcSignature(const UserInfo& userinfo,
                                         uint64_t date) const {
    if (IsRootUserAndHasPassword(userinfo)) {
        std::string str2sig = Authenticator::GetString2Signature(
            date, userinfo.owner);
        std::string sig = Authenticator::CalcString2Signature(
            str2sig, userinfo.password);
        return sig;
    }

    return "";
}

}   // namespace client
}   // namespace curve
