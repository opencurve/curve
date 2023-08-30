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
 * File Created: Monday, 13th February 2019 9:47:08 am
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_LIBCURVE_SNAPSHOT_H_
#define SRC_CLIENT_LIBCURVE_SNAPSHOT_H_

#include <map>
#include <string>
#include <vector>

#include "src/client/mds_client.h"
#include "src/client/config_info.h"
#include "src/client/client_common.h"
#include "src/client/iomanager4chunk.h"

namespace curve {
namespace client {
//SnapshotClient is the exit for peripheral snapshot systems to communicate with MDS and Chunkserver
class SnapshotClient {
 public:
  SnapshotClient();
  ~SnapshotClient() = default;
  /**
   *Initialization function, peripheral system directly passes in configuration options
   * @param: opt is the peripheral configuration option
   * @return: 0 indicates success, -1 indicates failure
   */
  int Init(const ClientConfigOption& opt);

  /**
   *File object initialization function
   * @param: Configuration file path
   */
  int Init(const std::string& configpath);

  /**
   *Create a snapshot
   * @param: userinfo is the user information
   * @param: filename is the file name to create the snapshot
   * @param: seq is the output parameter to obtain the version information of the file
   * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise LIBCURVE_ERROR::FAILED
   */
  int CreateSnapShot(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t* seq);
  /**
   *Delete snapshot
   * @param: userinfo is the user information
   * @param: filename is the file name to be deleted
   * @param: seq The version information of this file
   * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise LIBCURVE_ERROR::FAILED
   */
  int DeleteSnapShot(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t seq);
  /**
   *Obtain file information corresponding to the snapshot
   * @param: userinfo is the user information
   * @param: filename is the corresponding file name
   * @param: seq corresponds to the version information when taking a snapshot of the file
   * @param: snapinfo is a parameter that saves the basic information of the current file
   * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise LIBCURVE_ERROR::FAILED
   */
  int GetSnapShot(const std::string& fname,
                             const UserInfo_t& userinfo,
                             uint64_t seq,
                             FInfo* snapinfo);
  /**
   *List the file information corresponding to the version list of the current file
   * @param: userinfo is the user information
   * @param: filenam file name
   * @param: seqvec is the version list of the current file
   * @param: snapif is a parameter that obtains file information for multiple seq numbers
   * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise LIBCURVE_ERROR::FAILED
   */
  int ListSnapShot(const std::string& filename,
                            const UserInfo_t& userinfo,
                            const std::vector<uint64_t>* seqvec,
                            std::map<uint64_t, FInfo>* snapif);
  /**
   *Obtain snapshot data segment information
   * @param: userinfo is the user information
   * @param: filenam file name
   * @param: seq is the file version number information
   * @param: offset is the offset of the file
   * @param: segInfo is a parameter that saves the snapshot segment information of the current file
   * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise LIBCURVE_ERROR::FAILED
   */
  int GetSnapshotSegmentInfo(const std::string& filename,
                            const UserInfo_t& userinfo,
                            uint64_t seq,
                            uint64_t offset,
                            SegmentInfo *segInfo);

  /**
   *Read snapshot data of seq version number
   * @param: cininfo is the ID information corresponding to the current chunk
   * @param: seq is the snapshot version number
   * @param: offset is the offset within the snapshot
   * @param: len is the length to be read
   * @param: buf is a read buffer
   * @param: scc is an asynchronous callback
   * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise LIBCURVE_ERROR::FAILED
   */
  int ReadChunkSnapshot(ChunkIDInfo cidinfo, uint64_t seq, uint64_t offset,
                        uint64_t len, char *buf, SnapCloneClosure* scc);
  /**
   *Delete snapshots generated during this dump or left over from history
   *If no snapshot is generated during the dump process, modify the correntSn of the chunk
   * @param: cininfo is the ID information corresponding to the current chunk
   * @param: correctedSeq is the version of chunk that needs to be corrected
   */
  int DeleteChunkSnapshotOrCorrectSn(ChunkIDInfo cidinfo,
                                                uint64_t correctedSeq);
  /**
   *Obtain the version information of the chunk, where chunkInfo is the output parameter
   * @param: cininfo is the ID information corresponding to the current chunk
   * @param: chunkInfo is the detailed information of the snapshot
   */
  int GetChunkInfo(ChunkIDInfo cidinfo, ChunkInfoDetail *chunkInfo);
  /**
   *Get snapshot status
   * @param: userinfo is the user information
   * @param: filenam file name
   * @param: seq is the file version number information
   */
  int CheckSnapShotStatus(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t seq,
                                FileStatus* filestatus);
  /**
   * @brief Create clone file
   * @detail
   *- If clone, reset sn to initial value
   *- If recover, sn remains unchanged
   *
   * @param source clone Source file name
   * @param: destination clone Destination file name
   * @param: userinfo User information
   * @param: size File size
   * @param: sn version number
   * @param: chunksize is the chunk size of the file to be created
   * @param stripeUnit stripe size
   * @param stripeCount stripe count
   * @param  poolset poolset of destination file
   * @param[out] fileinfo The file information of the target file created
   *
   * @return error code
   */
  int CreateCloneFile(const std::string& source,
                      const std::string& destination,
                      const UserInfo_t& userinfo,
                      uint64_t size,
                      uint64_t sn,
                      uint32_t chunksize,
                      uint64_t stripeUnit,
                      uint64_t stripeCount,
                      const std::string& poolset,
                      FInfo* fileinfo);

  /**
   * @brief lazy Create clone chunk
   * @param: URL of the location data source
   * @param: chunkidinfo target chunk
   * @param: SN chunk's serial number
   * @param: chunkSize Chunk size
   * @param: correntSn used to modify the chunk when creating CloneChunk
   * @param: scc is an asynchronous callback
   *
   * @return error code
   */
  int CreateCloneChunk(const std::string &location,
                       const ChunkIDInfo &chunkidinfo, uint64_t sn,
                       uint64_t correntSn, uint64_t chunkSize,
                       SnapCloneClosure* scc);

  /**
   * @brief Actual recovery chunk data
   *
   * @param: chunkidinfo chunkidinfo
   * @param: offset offset
   * @param: len length
   * @param: scc is an asynchronous callback
   *
   * @return error code
   */
  int RecoverChunk(const ChunkIDInfo &chunkidinfo,
                   uint64_t offset, uint64_t len,
                   SnapCloneClosure* scc);

  /**
   * @brief Notify mds to complete Clone Meta
   *
   * @param: destination target file
   * @param: userinfo User Information
   *
   * @return error code
   */
  int CompleteCloneMeta(const std::string &destination,
                                const UserInfo_t& userinfo);

  /**
   * @brief Notify mds to complete Clone Chunk
   *
   * @param: destination target file
   * @param: userinfo User Information
   *
   * @return error code
   */
  int CompleteCloneFile(const std::string &destination,
                                const UserInfo_t& userinfo);

  /**
   *Set clone file status
   * @param: filename Target file
   * @param: filestatus is the target state to be set
   * @param: userinfo User information
   * @param: FileId is the file ID information, not required
   *
   * @return error code
   */
  int SetCloneFileStatus(const std::string &filename,
                          const FileStatus& filestatus,
                          const UserInfo_t& userinfo,
                          uint64_t fileID = 0);

  /**
   * @brief Get file information
   *
   * @param: filename File name
   * @param: userinfo User Information
   * @param[out] fileInfo file information
   *
   * @return error code
   */
  int GetFileInfo(const std::string &filename,
                                const UserInfo_t& userinfo,
                                FInfo* fileInfo);

  /**
   * @brief Query or allocate file segment information
   *
   * @param: userinfo User Information
   * @param: offset offset value
   * @param: segInfo segment information
   *
   * @return error code
   */
  int GetOrAllocateSegmentInfo(bool allocate,
                                uint64_t offset,
                                const FInfo_t* fi,
                                SegmentInfo *segInfo);

  /**
   * @brief is the file copied for recover rename
   *
   * @param: userinfo User Information
   * @param: originId The original file ID that was restored
   * @param: destinationId The cloned target file ID
   * @param: origin The original file name of the recovered file
   * @param: destination The cloned target file
   *
   * @return error code
   */
  int RenameCloneFile(const UserInfo_t& userinfo,
                              uint64_t originId,
                              uint64_t destinationId,
                              const std::string &origin,
                              const std::string &destination);

  /**
   *Delete files
   * @param: userinfo is the user information
   * @param: filename The file name to be deleted
   * @param: id is the file id, with a default value of 0. If the user does not specify this value, the id will not be passed to mds
   */
  int DeleteFile(const std::string& filename,
                            const UserInfo_t& userinfo,
                            uint64_t id = 0);

  /**
   *Deconstruct and recycle resources
   */
  void UnInit();
  /**
   *Obtain iomanager information and test code usage
   */
  IOManager4Chunk* GetIOManager4Chunk() {return &iomanager4chunk_;}

 private:
  /**
   *Obtain the serverlist of copyset in the logicalpool
   * @param: lpid is the logical pool id
   * @param: csid is the copysetid dataset in the logical pool
   * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise LIBCURVE_ERROR::FAILED
   */
  int GetServerList(const LogicPoolID& lpid,
                            const std::vector<CopysetID>& csid);

 private:
  //MDSClient is responsible for communicating with Metaserver, and all communication goes through this interface
  MDSClient               mdsclient_;

  //IOManager4Chunk is used to manage IO sent to the chunkserver end
  IOManager4Chunk         iomanager4chunk_;

  //Used for client configuration reading
  ClientConfig clientconfig_;
};
}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_LIBCURVE_SNAPSHOT_H_
