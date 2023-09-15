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
 * File Created: Friday, 21st June 2019 10:20:49 am
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_MDS_CLIENT_BASE_H_
#define SRC_CLIENT_MDS_CLIENT_BASE_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <string>
#include <vector>

#include "include/client/libcurve.h"
#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/client/client_common.h"
#include "src/common/timeutility.h"

namespace curve {
namespace client {

using curve::mds::OpenFileRequest;
using curve::mds::OpenFileResponse;
using curve::mds::CreateFileRequest;
using curve::mds::CreateFileResponse;
using curve::mds::CloseFileRequest;
using curve::mds::CloseFileResponse;
using curve::mds::RenameFileRequest;
using curve::mds::RenameFileResponse;
using curve::mds::ExtendFileRequest;
using curve::mds::ExtendFileResponse;
using curve::mds::DeleteFileRequest;
using curve::mds::DeleteFileResponse;
using curve::mds::RecoverFileRequest;
using curve::mds::RecoverFileResponse;
using curve::mds::GetFileInfoRequest;
using curve::mds::GetFileInfoResponse;
using curve::mds::IncreaseFileEpochResponse;
using curve::mds::DeleteSnapShotRequest;
using curve::mds::DeleteSnapShotResponse;
using curve::mds::ReFreshSessionRequest;
using curve::mds::ReFreshSessionResponse;
using curve::mds::ListDirRequest;
using curve::mds::ListDirResponse;
using curve::mds::ChangeOwnerRequest;
using curve::mds::ChangeOwnerResponse;
using curve::mds::CreateSnapShotRequest;
using curve::mds::CreateSnapShotResponse;
using curve::mds::CreateCloneFileRequest;
using curve::mds::CreateCloneFileResponse;
using curve::mds::SetCloneFileStatusRequest;
using curve::mds::SetCloneFileStatusResponse;
using curve::mds::GetOrAllocateSegmentRequest;
using curve::mds::GetOrAllocateSegmentResponse;
using curve::mds::DeAllocateSegmentRequest;
using curve::mds::DeAllocateSegmentResponse;
using curve::mds::CheckSnapShotStatusRequest;
using curve::mds::CheckSnapShotStatusResponse;
using curve::mds::ListSnapShotFileInfoRequest;
using curve::mds::ListSnapShotFileInfoResponse;
using curve::mds::GetOrAllocateSegmentRequest;
using curve::mds::GetOrAllocateSegmentResponse;
using curve::mds::topology::GetChunkServerListInCopySetsRequest;
using curve::mds::topology::GetChunkServerListInCopySetsResponse;
using curve::mds::topology::GetClusterInfoRequest;
using curve::mds::topology::GetClusterInfoResponse;
using curve::mds::topology::GetChunkServerInfoResponse;
using curve::mds::topology::ListChunkServerResponse;
using curve::mds::IncreaseFileEpochRequest;
using curve::mds::IncreaseFileEpochResponse;
using curve::mds::topology::ListPoolsetRequest;
using curve::mds::topology::ListPoolsetResponse;

extern const char* kRootUserName;

// MDSClientBase abstracts all RPC interfaces with the MDS, decoupling them from business logic. 
// Here, it is responsible only for sending RPC requests, while the specific business logic processing is returned to the caller through responses and controllers, 
// which are handled by the caller.
class MDSClientBase {
 public:
    /**
     * Open File
     * @param: filename is the file name
     * @param: userinfo is the user information
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void OpenFile(const std::string& filename,
                  const UserInfo_t& userinfo,
                  OpenFileResponse* response,
                  brpc::Controller* cntl,
                  brpc::Channel* channel);

    /**
     * Create File
     * @param: filename The file name used to create the file
     * @param: userinfo is the user information
     * @param: size File length
     * @param: normalFile indicates whether the created file is a regular file or a directory file. If it is a directory, size is ignored
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void CreateFile(const CreateFileContext& context,
                    CreateFileResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * To close the file, it is necessary to carry the sessionid, so that the mds side will delete the session information in the database
     * @param: filename is the file name to be renewed
     * @param: userinfo is the user information
     * @param: sessionid is the session information of the file
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void CloseFile(const std::string& filename,
                   const UserInfo_t& userinfo,
                   const std::string& sessionid,
                   CloseFileResponse* response,
                   brpc::Controller* cntl,
                   brpc::Channel* channel);
    /**
     * Obtain file information, where fi is the output parameter
     * @param: filename is the file name
     * @param: userinfo is the user information
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void GetFileInfo(const std::string& filename,
                     const UserInfo_t& userinfo,
                     GetFileInfoResponse* response,
                     brpc::Controller* cntl,
                     brpc::Channel* channel);

    /**
     * @brief Increase epoch and return chunkserver locations
     *
     * @param[in] filename  file name
     * @param[in] userinfo  user info
     * @param[out] response  rpc response
     * @param[in,out] cntl  rpc cntl
     * @param[in] channel  rpc channel
     *
     */
     void IncreaseEpoch(const std::string& filename,
                        const UserInfo_t& userinfo,
                        IncreaseFileEpochResponse* response,
                        brpc::Controller* cntl,
                        brpc::Channel* channel);

    /**
     * Create a snapshot with version number seq
     * @param: userinfo is the user information
     * @param: filename is the file name to create the snapshot
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void CreateSnapShot(const std::string& filename,
                        const UserInfo_t& userinfo,
                        CreateSnapShotResponse* response,
                        brpc::Controller* cntl,
                        brpc::Channel* channel);
    /**
     * Delete snapshot with version number seq
     * @param: userinfo is the user information
     * @param: filename is the file name to be snapshot
     * @param: seq is the version information of the file when creating the snapshot
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void DeleteSnapShot(const std::string& filename,
                        const UserInfo_t& userinfo,
                        uint64_t seq,
                        DeleteSnapShotResponse* response,
                        brpc::Controller* cntl,
                        brpc::Channel* channel);
    /**
     * Obtain snapshot file information with version number seq in the form of a list, where snapif is the output parameter
     * @param: filename is the file name to be snapshot
     * @param: userinfo is the user information
     * @param: seq is the version information of the file when creating the snapshot
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void ListSnapShot(const std::string& filename,
                      const UserInfo_t& userinfo,
                      const std::vector<uint64_t>* seq,
                      ListSnapShotFileInfoResponse* response,
                      brpc::Controller* cntl,
                      brpc::Channel* channel);
    /**
     * Obtain the chunk information of the snapshot and update it to the metacache, where segInfo is the output parameter
     * @param: filename is the file name to be snapshot
     * @param: userinfo is the user information
     * @param: seq is the version information of the file when creating the snapshot
     * @param: offset is the offset within the file
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void GetSnapshotSegmentInfo(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t seq,
                                uint64_t offset,
                                GetOrAllocateSegmentResponse* response,
                                brpc::Controller* cntl,
                                brpc::Channel* channel);

    /**
     * The file interface needs to maintain a heartbeat with MDS when opening files, and refresh is used to renew the contract
     * The renewal result will be returned to the calling layer through LeaseRefreshResult* resp
     * @param: filename is the file name to be renewed
     * @param: sessionid is the session information of the file
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void RefreshSession(const std::string& filename,
                        const UserInfo_t& userinfo,
                        const std::string& sessionid,
                        ReFreshSessionResponse* response,
                        brpc::Controller* cntl,
                        brpc::Channel* channel);
    /**
     * Get snapshot status
     * @param: filenam file name
     * @param: userinfo is the user information
     * @param: seq is the file version number information
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void CheckSnapShotStatus(const std::string& filename,
                             const UserInfo_t& userinfo,
                             uint64_t seq,
                             CheckSnapShotStatusResponse* response,
                             brpc::Controller* cntl,
                             brpc::Channel* channel);
    /**
     * Obtain the serverlist information corresponding to the copysetid and update it to the metacache
     * @param: logicPoolId Logical Pool Information
     * @param: copysetidvec is the list of copysets to obtain
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void GetServerList(const LogicPoolID& logicalpooid,
                       const std::vector<CopysetID>& copysetidvec,
                       GetChunkServerListInCopySetsResponse* response,
                       brpc::Controller* cntl,
                       brpc::Channel* channel);

    /**
     * Obtain the cluster ID corresponding to the mds
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void GetClusterInfo(GetClusterInfoResponse* response,
                        brpc::Controller* cntl,
                        brpc::Channel* channel);

    void ListPoolset(ListPoolsetResponse* response,
                     brpc::Controller* cntl,
                     brpc::Channel* channel);

    /**
     * Create clone file
     * @param source Clone source file name
     * @param: destination clone Destination file name
     * @param: userinfo User Information
     * @param: size File size
     * @param: sn version number
     * @param: chunksize is the chunk size of the created file
     * @param stripeUnit stripe size
     * @param stripeCount stripe count
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void CreateCloneFile(const std::string& source,
                         const std::string& destination,
                         const UserInfo_t& userinfo,
                         uint64_t size,
                         uint64_t sn,
                         uint32_t chunksize,
                         uint64_t stripeUnit,
                         uint64_t stripeCount,
                         const std::string& poolset,
                         CreateCloneFileResponse* response,
                         brpc::Controller* cntl,
                         brpc::Channel* channel);

    /**
     * @brief Notify mds to complete Clone Meta
     * @param: filename Target file
     * @param: filestatus is the target state to be set
     * @param: userinfo User information
     * @param: fileId is the file ID information, not required
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void SetCloneFileStatus(const std::string& filename,
                            const FileStatus& filestatus,
                            const UserInfo_t& userinfo,
                            uint64_t fileID,
                            SetCloneFileStatusResponse* response,
                            brpc::Controller* cntl,
                            brpc::Channel* channel);

    /**
     * Get or Alloc SegmentInfo，and update to Metacache
     * @param: allocate  ture for allocate, false for get only
     * @param: offset  segment start offset
     * @param: fi file info
     * @param: fEpoch  file epoch info
     * @param[out]: reponse  rpc response
     * @param[in|out]: cntl  rpc controller
     * @param[in]:channel  rpc channel
     */
    void GetOrAllocateSegment(bool allocate,
                              uint64_t offset,
                              const FInfo_t* fi,
                              const FileEpoch_t *fEpoch,
                              GetOrAllocateSegmentResponse* response,
                              brpc::Controller* cntl,
                              brpc::Channel* channel);

    void DeAllocateSegment(const FInfo* fileInfo, uint64_t segmentOffset,
                           DeAllocateSegmentResponse* response,
                           brpc::Controller* cntl, brpc::Channel* channel);

    /**
     * @brief duplicate file
     * @param: userinfo User Information
     * @param: originId The original file ID that was restored
     * @param: destinationId The cloned target file ID
     * @param: origin The original file name of the recovered file
     * @param: destination The cloned target file
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void RenameFile(const UserInfo_t& userinfo,
                    const std::string &origin,
                    const std::string &destination,
                    uint64_t originId,
                    uint64_t destinationId,
                    RenameFileResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * Extension file
     * @param: userinfo is the user information
     * @param: filename File name
     * @param: newsize New size
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void Extend(const std::string& filename,
                const UserInfo_t& userinfo,
                uint64_t newsize,
                ExtendFileResponse* response,
                brpc::Controller* cntl,
                brpc::Channel* channel);
    /**
     * Delete files
     * @param: userinfo is the user information
     * @param: filename The file name to be deleted
     * @param: Does deleteforce force deletion without placing it in the garbage bin
     * @param: id is the file id, with a default value of 0. If the user does not specify this value, the id will not be passed to mds
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void DeleteFile(const std::string& filename,
                    const UserInfo_t& userinfo,
                    bool deleteforce,
                    uint64_t fileid,
                    DeleteFileResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);

    /**
     * recover file
     * @param: userinfo
     * @param: filename
     * @param: fileid default 0
     * @param[out]: response, is the rpc response
     * @param[in|out]: cntl, return RPC status
     * @param[in]:channel
     */
    void RecoverFile(const std::string& filename,
                    const UserInfo_t& userinfo,
                    uint64_t fileid,
                    RecoverFileResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);

    /**
     * Change owner
     * @param: filename The file name to be changed
     * @param: newOwner New owner information
     * @param: userinfo The user information for performing this operation, only the root user can perform changes
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void ChangeOwner(const std::string& filename,
                     const std::string& newOwner,
                     const UserInfo_t& userinfo,
                     ChangeOwnerResponse* response,
                     brpc::Controller* cntl,
                     brpc::Channel* channel);
    /**
     * Enumerate directory contents
     * @param: userinfo is the user information
     * @param: dirpath is the directory path
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
      */
    void Listdir(const std::string& dirpath,
                 const UserInfo_t& userinfo,
                 ListDirResponse* response,
                 brpc::Controller* cntl,
                 brpc::Channel* channel);
    /**
     * Obtain chunkserverID information
     * @param[in]: IP is the listening address of the current client
     * @param[in]: port is the listening port
     * @param[out]: response is the response of the rpc, provided for external processing
     * @param[in|out]: cntl is both an input and output parameter, returning RPC status
     * @param[in]: channel is the current channel established with MDS
     */
    void GetChunkServerInfo(const std::string& ip,
                            uint16_t port,
                            GetChunkServerInfoResponse* reponse,
                            brpc::Controller* cntl,
                            brpc::Channel* channel);

    /**
     * Obtain the IDs of all chunkservers on the server
     * @param[in]: IP is the address of the current server
     * @param[out]: response is the response of the current rpc call, returned to external processing
     * @param[in|out]: cntl is both an input and output parameter
     * @param[in]: channel is the current channel established with MDS
     */
    void ListChunkServerInServer(const std::string& ip,
                                 ListChunkServerResponse* response,
                                 brpc::Controller* cntl,
                                 brpc::Channel* channel);

 private:
    /**
     * Fill in user information for different requests
     * @param: request is the pointer to the variable to be filled in
     */
    template <typename T>
    void FillUserInfo(T* request, const UserInfo_t& userinfo) {
        uint64_t date = curve::common::TimeUtility::GetTimeofDayUs();
        request->set_owner(userinfo.owner);
        request->set_date(date);
        request->set_signature(CalcSignature(userinfo, date));
    }

 private:
    bool IsRootUserAndHasPassword(const UserInfo& userinfo) const {
        return userinfo.owner == kRootUserName && !userinfo.password.empty();
    }

    std::string CalcSignature(const UserInfo& userinfo, uint64_t date) const;
};

}   //  namespace client
}   //  namespace curve

#endif  // SRC_CLIENT_MDS_CLIENT_BASE_H_
