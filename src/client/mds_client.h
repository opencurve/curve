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
 * File Created: Monday, 18th February 2019 6:25:17 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_MDS_CLIENT_H_
#define SRC_CLIENT_MDS_CLIENT_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <map>
#include <string>
#include <vector>
#include <list>

#include "include/client/libcurve.h"
#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/client_metric.h"
#include "src/client/mds_client_base.h"
#include "src/client/metacache_struct.h"

namespace curve {
namespace client {

class RPCExcutorRetryPolicy {
 public:
    RPCExcutorRetryPolicy()
        : retryOpt_(), currentWorkingMDSAddrIndex_(0), cntlID_(1) {}

    void SetOption(const MetaServerOption::RpcRetryOption &option) {
        retryOpt_ = option;
    }
    using RPCFunc = std::function<int(int addrindex, uint64_t rpctimeoutMS,
                                      brpc::Channel *, brpc::Controller *)>;
    /**
     *Detach the retry related logic between client and mds
     * @param: task is the specific rpc task to be carried out currently
     * @param: maxRetryTimeMS is the maximum retry time currently executed
     * @return: Returns the result of the current RPC
     */
    int DoRPCTask(RPCFunc task, uint64_t maxRetryTimeMS);

    /**
     *Test usage: Set the currently serving mdsindex
     */
    void SetCurrentWorkIndex(int index) {
        currentWorkingMDSAddrIndex_.store(index);
    }

    /**
     *Test usage: Obtain the currently serving mdsindex
     */
    int GetCurrentWorkIndex() const {
        return currentWorkingMDSAddrIndex_.load();
    }

 private:
    /**
     *RPC failed and needs to be retried. Based on the different states returned by CNTL, determine what kind of preprocessing should be done.
     *I mainly did the following things:
     *1 If the previous RPC was a timeout return, execute the rpc timeout index backoff logic
     *2 If the previous RPC returned values such as' not connect ', it will actively trigger the switching of the mds address and retry
     *3 Update retry information, such as the number of consecutive retries on the current mds
     * @param[in]: status is the status returned by the current RPC failure
     * @param normalRetryCount The total count of normal retry
     * @param[in][out]: curMDSRetryCount The number of retries on the current mds node, if switching mds
     *                  The value will be reset to 1
     * @param[in]: curRetryMDSIndex represents the mds index currently being retried
     * @param[out]: lastWorkingMDSIndex The mds index of the last service being provided
     * @param[out]: timeOutMS adjusts rpctimeout based on status
     *
     * @return: Returns the mds index for the next retry
     */
    int PreProcessBeforeRetry(int status, bool retryUnlimit,
                              uint64_t *normalRetryCount,
                              uint64_t *curMDSRetryCount, int curRetryMDSIndex,
                              int *lastWorkingMDSIndex, uint64_t *timeOutMS);
    /**
     *Execute rpc send task
     * @param[in]: mdsindex is the address index corresponding to mds
     * @param[in]: rpcTimeOutMS is the rpc timeout time
     * @param[in]: task is the task to be executed
     * @return: If the channel is successfully obtained, 0 will be returned. Otherwise, -1
     */
    int ExcuteTask(int mdsindex, uint64_t rpcTimeOutMS,
                   RPCExcutorRetryPolicy::RPCFunc task);
    /**
     *Retrieve the next mds index that needs to be retried based on the input status, and switch the mds logic:
     *Record three states: curRetryMDSIndex, lastWorkingMDSIndex
     *CurrentWorkingMDSIndex
     *1. At the beginning, curRetryMDSIndex=currentWorkingMDSIndex
     *                      lastWorkingMDSIndex=currentWorkingMDSIndex
     *2.
     *If rpc fails, it will trigger the switch curRetryMDSIndex. If the lastWorkingMDSIndex
     *Equal to the currentWorkingMDSIndex, it will switch sequentially to the next mds index,
     *If the lastWorkingMDSIndex and currentWorkingMDSIndex are not equal, then
     *Indicates that other interfaces have updated currentWorkingMDSAddrIndex_, So this switch
     *Switch directly to currentWorkingMDSAddrIndex_
     * @param[in]: needChangeMDS indicates whether the current peripheral needs to switch to MDS. This value is determined by
     *              PreProcessBeforeRetry function determination
     * @param[in]: currentRetryIndex is the mds index currently being retried
     * @param[in][out]:
     * lastWorkingindex is the index of the last mds being serviced and the mds being retried
     *              The index may be different from the mds being serviced.
     * @return: Returns the mds index for the next retry
     */

    int GetNextMDSIndex(bool needChangeMDS, int currentRetryIndex,
                        int *lastWorkingindex);
    /**
     *Based on the input parameters, decide whether to continue retry. The condition for retry exit is that the retry time exceeds the maximum allowed time
     *The retry time on IO paths is different from that on non IO paths, and the retry time on non IO paths is determined by the configuration file
     *The mdsMaxRetryMS parameter specifies that the IO path is an infinite loop retry.
     * @param[in]: startTimeMS
     * @param[in]: maxRetryTimeMS is the maximum retry time
     * @return: Need to continue retrying and return true, otherwise return false
     */
    bool GoOnRetry(uint64_t startTimeMS, uint64_t maxRetryTimeMS);

    /**
     *Increment controller id and return id
     */
    uint64_t GetLogId() {
        return cntlID_.fetch_add(1, std::memory_order_relaxed);
    }

 private:
    //Necessary configuration information for executing rpc
    MetaServerOption::RpcRetryOption retryOpt_;

    //Record the leader information from the last retry
    std::atomic<int> currentWorkingMDSAddrIndex_;

    //Controller ID, used to trace the entire RPC IO link
    //Simply use uint64 here, within a predictable range, without overflow
    std::atomic<uint64_t> cntlID_;
};


struct LeaseRefreshResult;

//MDSClient is the only window where the client communicates with MDS

class MDSClient : public MDSClientBase,
                  public std::enable_shared_from_this<MDSClient> {
 public:
    explicit MDSClient(const std::string &metricPrefix = "");

    virtual ~MDSClient();

    LIBCURVE_ERROR Initialize(const MetaServerOption &metaopt);

    /**
     *Create File
     * @param: context Create file information
     * @return: Successfully returned LIBCURVE_ERROR::OK
     *File already exists Return LIBCURVE_ERROR::EXIST
     *Otherwise, return LIBCURVE_ERROR::FAILED
     *If authentication fails, return LIBCURVE_ERROR::AUTHFAIL,
     */
    LIBCURVE_ERROR CreateFile(const CreateFileContext& context);
    /**
     * open file
     * @param: filename  file name
     * @param: userinfo  user info
     * @param[out]: fi  file info returned
     * @param[out]: fEpoch  file epoch info returned
     * @param[out]: lease lease of file returned
     * @return:
     * return LIBCURVE_ERROR::OK for success,
     * return LIBCURVE_ERROR::AUTHFAIL for auth fail,
     * otherwise return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR OpenFile(const std::string &filename,
                            const UserInfo_t &userinfo, FInfo_t *fi,
                            FileEpoch_t *fEpoch,
                            LeaseSession *lease);

    /**
     *Obtain the serverlist information corresponding to the copysetid and update it to the metacache
     * @param: logicPoolId Logical Pool Information
     * @param: csid is the list of copysets to obtain
     * @param: cpinfoVec saves the obtained server information
     * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise will be returned LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR
    GetServerList(const LogicPoolID &logicPoolId,
                  const std::vector<CopysetID> &csid,
                  std::vector<CopysetInfo<ChunkServerID>> *cpinfoVec);

    /**
     *Obtain the cluster information to which the current mds belongs
     * @param[out]: clsctx is the cluster information to be obtained
     * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise will be returned LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetClusterInfo(ClusterContext *clsctx);

    LIBCURVE_ERROR ListPoolset(std::vector<std::string>* out);

    /**
     * Get or Alloc SegmentInfo，and update to Metacache
     * @param: allocate  ture for allocate, false for get only
     * @param: offset  segment start offset
     * @param: fi file info
     * @param: fEpoch  file epoch info
     * @param[out]: segInfo segment info returned
     * @return:
     * return LIBCURVE_ERROR::OK for success,
     * return LIBCURVE_ERROR::AUTHFAIL for auth fail,
     * otherwise return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetOrAllocateSegment(bool allocate, uint64_t offset,
                                        const FInfo_t *fi,
                                        const FileEpoch_t *fEpoch,
                                        SegmentInfo *segInfo);

    /**
     * @brief Send DeAllocateSegment request to current working MDS
     * @param fileInfo current file info
     * @param offset segment start offset
     * @return LIBCURVE_ERROR::OK means success, other value means fail
     */
    virtual LIBCURVE_ERROR DeAllocateSegment(const FInfo *fileInfo,
                                             uint64_t offset);

    /**
     * Get File Info
     * @param: filename  file name
     * @param: userinfo  user info
     * @param[out]: fi  file info returned
     * @param[out]: fEpoch  file epoch info returned
     * @return:
     * return LIBCURVE_ERROR::OK for success,
     * return LIBCURVE_ERROR::AUTHFAIL for auth fail,
     * otherwise return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetFileInfo(const std::string &filename,
                               const UserInfo_t &userinfo,
                               FInfo_t *fi,
                               FileEpoch_t *fEpoch);

    /**
     * @brief Increase epoch and return chunkserver locations
     *
     * @param[in] filename  file name
     * @param[in] userinfo  user info
     * @param[out] fi  file info
     * @param[out] fEpoch  file epoch info
     * @param[out] csLocs  chunkserver locations
     *
     * @return LIBCURVE_ERROR::OK for success, LIBCURVE_ERROR::FAILED for fail.
     */
    LIBCURVE_ERROR IncreaseEpoch(const std::string& filename,
         const UserInfo_t& userinfo,
         FInfo_t* fi,
         FileEpoch_t *fEpoch,
         std::list<CopysetPeerInfo<ChunkServerID>> *csLocs);

    /**
     *Extension file
     * @param: userinfo is the user information
     * @param: filename File name
     * @param: newsize New size
     */
    LIBCURVE_ERROR Extend(const std::string &filename,
                          const UserInfo_t &userinfo, uint64_t newsize);
    /**
     *Delete files
     * @param: userinfo is the user information
     * @param: filename The file name to be deleted
     * @param: deleteforce  Does it force deletion without placing it in the garbage bin
     * @param: id is the file id, with a default value of 0. If the user does not specify this value, the id will not be passed to mds
     */
    LIBCURVE_ERROR DeleteFile(const std::string &filename,
                              const UserInfo_t &userinfo,
                              bool deleteforce = false, uint64_t id = 0);

    /**
     * recover file
     * @param: userinfo
     * @param: filename
     * @param: fileId is inodeid，default 0
     */
    LIBCURVE_ERROR RecoverFile(const std::string &filename,
                               const UserInfo_t &userinfo, uint64_t fileId);

    /**
     *Create a snapshot with version number seq
     * @param: userinfo is the user information
     * @param: filename is the file name to create the snapshot
     * @param: seq is an output parameter that returns the version information of the file when creating the snapshot
     * @return:
     *Successfully returned LIBCURVE_ERROR::OK, if authentication fails, return LIBCURVE_ERROR::AUTHFAIL,
     *Otherwise, return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR CreateSnapShot(const std::string &filename,
                                  const UserInfo_t &userinfo, uint64_t *seq);
    /**
     *Delete snapshot with version number seq
     * @param: userinfo is the user information
     * @param: filename is the file name to be snapshot
     * @param: seq is the version information of the file when creating the snapshot
     * @return:
     *Successfully returned LIBCURVE_ERROR::OK, if authentication fails, return LIBCURVE_ERROR::AUTHFAIL,
     *Otherwise, return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR DeleteSnapShot(const std::string &filename,
                                  const UserInfo_t &userinfo, uint64_t seq);

    /**
     *Obtain snapshot file information with version number seq in the form of a list, where snapif is the output parameter
     * @param: filename is the file name to be snapshot
     * @param: userinfo is the user information
     * @param: seq is the version information of the file when creating the snapshot
     * @param: snapif is a parameter that saves the basic information of the file
     * @return:
     *Successfully returned LIBCURVE_ERROR::OK, if authentication fails, return LIBCURVE_ERROR::AUTHFAIL,
     *Otherwise, return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR ListSnapShot(const std::string &filename,
                                const UserInfo_t &userinfo,
                                const std::vector<uint64_t> *seq,
                                std::map<uint64_t, FInfo> *snapif);
    /**
     *Obtain the chunk information of the snapshot and update it to the metacache, where segInfo is the output parameter
     * @param: filename is the file name to be snapshot
     * @param: userinfo is the user information
     * @param: seq is the version information of the file when creating the snapshot
     * @param: offset is the offset within the file
     * @param: segInfo is the output parameter, saving chunk information
     * @return:
     *Successfully returned LIBCURVE_ERROR::OK, if authentication fails, return LIBCURVE_ERROR::AUTHFAIL,
     *Otherwise, return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetSnapshotSegmentInfo(const std::string &filename,
                                          const UserInfo_t &userinfo,
                                          uint64_t seq, uint64_t offset,
                                          SegmentInfo *segInfo);
    /**
     *Get snapshot status
     * @param: filenam file name
     * @param: userinfo is the user information
     * @param: seq is the file version number information
     * @param[out]: filestatus is the snapshot status
     */
    LIBCURVE_ERROR CheckSnapShotStatus(const std::string &filename,
                                       const UserInfo_t &userinfo, uint64_t seq,
                                       FileStatus *filestatus);

    /**
     *The file interface needs to maintain a heartbeat with MDS when opening files, and refresh is used to renew the contract
     *The renewal result will be returned to the calling layer through LeaseRefreshResult * resp
     * @param: filename is the file name to be renewed
     * @param: sessionid is the session information of the file
     * @param: resp is the release information passed from the mds end
     * @param[out]: lease the session information of the current file
     * @return:
     *Successfully returned LIBCURVE_ERROR::OK, if authentication fails, return LIBCURVE_ERROR::AUTHFAIL,
     *Otherwise, return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR RefreshSession(const std::string &filename,
                                  const UserInfo_t &userinfo,
                                  const std::string &sessionid,
                                  LeaseRefreshResult *resp,
                                  LeaseSession *lease = nullptr);
    /**
     *To close the file, it is necessary to carry the session ID, so that the mds side will delete the session information in the database
     * @param: filename is the file name to be renewed
     * @param: sessionid is the session information of the file
     * @return:
     *Successfully returned LIBCURVE_ERROR::OK, if authentication fails, return LIBCURVE_ERROR::AUTHFAIL,
     *Otherwise, return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR CloseFile(const std::string &filename,
                             const UserInfo_t &userinfo,
                             const std::string &sessionid);

    /**
     * @brief Create clone file
     * @detail
     *- If clone, reset sn to initial value
     *- If recover, sn remains unchanged
     *
     * @param source Clone source file name
     * @param: destination clone Destination file name
     * @param: userinfo User Information
     * @param: size File size
     * @param: sn version number
     * @param: chunksize is the chunk size of the created file
     * @param stripeUnit stripe size
     * @param stripeCount stripe count
     * @param[out] destFileId The ID of the target file created
     *
     * @return error code
     */
    LIBCURVE_ERROR CreateCloneFile(const std::string &source,
                                   const std::string &destination,
                                   const UserInfo_t &userinfo, uint64_t size,
                                   uint64_t sn, uint32_t chunksize,
                                   uint64_t stripeUnit, uint64_t stripeCount,
                                   const std::string& poolset,
                                   FInfo *fileinfo);

    /**
     * @brief Notify mds to complete Clone Meta
     *
     * @param: destination target file
     * @param: userinfo User Information
     *
     * @return error code
     */
    LIBCURVE_ERROR CompleteCloneMeta(const std::string &destination,
                                     const UserInfo_t &userinfo);

    /**
     * @brief Notify mds to complete Clone Chunk
     *
     * @param: destination target file
     * @param: userinfo User Information
     *
     * @return error code
     */
    LIBCURVE_ERROR CompleteCloneFile(const std::string &destination,
                                     const UserInfo_t &userinfo);

    /**
     * @brief Notify mds to complete Clone Meta
     *
     * @param: filename Target file
     * @param: filestatus is the target state to be set
     * @param: userinfo User information
     * @param: FileId is the file ID information, not required
     *
     * @return error code
     */
    LIBCURVE_ERROR SetCloneFileStatus(const std::string &filename,
                                      const FileStatus &filestatus,
                                      const UserInfo_t &userinfo,
                                      uint64_t fileID = 0);

    /**
     * @brief duplicate file
     *
     * @param: userinfo User Information
     * @param: originId The original file ID that was restored
     * @param: destinationId The cloned target file ID
     * @param: origin The original file name of the recovered file
     * @param: destination The cloned target file
     *
     * @return error code
     */
    LIBCURVE_ERROR RenameFile(const UserInfo_t &userinfo,
                              const std::string &origin,
                              const std::string &destination,
                              uint64_t originId = 0,
                              uint64_t destinationId = 0);

    /**
     *Change owner
     * @param: filename The file name to be changed
     * @param: newOwner New owner information
     * @param: userinfo The user information for performing this operation, only the root user can perform changes
     * @return: Successfully returned 0,
     *Otherwise, return LIBCURVE_ERROR::FAILED, LIBCURVE_ERROR::AUTHFAILED, etc
     */
    LIBCURVE_ERROR ChangeOwner(const std::string &filename,
                               const std::string &newOwner,
                               const UserInfo_t &userinfo);

    /**
     *Enumerate directory contents
     * @param: userinfo is the user information
     * @param: dirpath is the directory path
     * @param[out]: filestatVec File information in the current folder
     */
    LIBCURVE_ERROR Listdir(const std::string &dirpath,
                           const UserInfo_t &userinfo,
                           std::vector<FileStatInfo> *filestatVec);

    /**
     *Register the address and port for client metric listening with mds
     * @param: IP client IP
     * @param: dummyServerPort is the listening port
     * @return: Successfully returned 0,
     *Otherwise, return LIBCURVE_ERROR::FAILED, LIBCURVE_ERROR::AUTHFAILED, etc
     */
    LIBCURVE_ERROR Register(const std::string &ip, uint16_t port);

    /**
     *Obtain chunkserver information
     * @param[in] addr chunkserver address information
     * @param[out] chunkserverInfo Information to be obtained
     * @return: Successfully returned OK
     */
    LIBCURVE_ERROR
    GetChunkServerInfo(const PeerAddr &addr,
                       CopysetPeerInfo<ChunkServerID> *chunkserverInfo);

    /**
     *Obtain the IDs of all chunkservers on the server
     * @param[in]: IP is the IP address of the server
     * @param[out]: csIds is used to save the id of the chunkserver
     * @return: Successfully returned LIBCURVE_ERROR::OK, failure returns LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR ListChunkServerInServer(const std::string &ip,
                                           std::vector<ChunkServerID> *csIds);

    /**
     *Deconstruct and recycle resources
     */
    void UnInitialize();

    /**
     *Map the mds side error code to the libcurve error code
     * @param: statecode is the error code on the mds side
     * @param[out]: The error code of the output parameter is the error code on the side of libcurve
     */
    void MDSStatusCode2LibcurveError(const ::curve::mds::StatusCode &statcode,
                                     LIBCURVE_ERROR *errcode);

    LIBCURVE_ERROR ReturnError(int retcode);

 private:
    //Initialization flag, placing duplicate initialization
    bool inited_ = false;

    //Initialization option configuration for the current module
    MetaServerOption metaServerOpt_;

    //Metric statistics of communication between client and mds
    MDSClientMetric mdsClientMetric_;

    RPCExcutorRetryPolicy rpcExcutor_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_MDS_CLIENT_H_
