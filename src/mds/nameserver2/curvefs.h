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
 * Created Date: Friday September 7th 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_MDS_NAMESERVER2_CURVEFS_H_
#define SRC_MDS_NAMESERVER2_CURVEFS_H_

#include <bvar/bvar.h>
#include <vector>
#include <string>
#include <memory>
#include <thread>  //NOLINT
#include <chrono>  //NOLINT
#include <unordered_map>
#include "proto/nameserver2.pb.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/chunk_allocator.h"
#include "src/mds/nameserver2/clean_manager.h"
#include "src/mds/nameserver2/async_delete_snapshot_entity.h"
#include "src/mds/nameserver2/file_record.h"
#include "src/mds/nameserver2/idgenerator/inode_id_generator.h"
#include "src/common/authenticator.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"
#include "src/mds/snapshotcloneclient/snapshotclone_client.h"
using curve::common::Authenticator;
using curve::mds::snapshotcloneclient::SnapshotCloneClient;

namespace curve {
namespace mds {

struct RootAuthOption {
    std::string rootOwner;
    std::string rootPassword;
};

struct ThrottleOption {
    uint64_t iopsMin;
    uint64_t iopsMax;
    double iopsPerGB;

    uint64_t bpsMin;
    uint64_t bpsMax;
    double bpsPerGB;
};

struct CurveFSOption {
    uint64_t defaultChunkSize;
    uint64_t defaultSegmentSize;
    uint64_t minFileLength;
    uint64_t maxFileLength;
    RootAuthOption authOptions;
    FileRecordOptions fileRecordOptions;
    ThrottleOption throttleOption;
};

struct AllocatedSize {
    // The size of the segment allocated by MDS to the file
    uint64_t total;
    // alloc size in each pool
    std::unordered_map<PoolIdType, uint64_t> allocSizeMap;
    AllocatedSize() : total(0) {}
    AllocatedSize& operator+=(const AllocatedSize& rhs);
};

using ::curve::mds::DeleteSnapShotResponse;

class CurveFS {
 public:
    // singleton, supported in c++11
    static CurveFS &GetInstance() {
        static CurveFS curvefs;
        return curvefs;
    }

    /**
     *  @brief CurveFS initialization
     *  @param NameServerStorage:
     *         InodeIDGenerator:
     *         ChunkSegmentAllocator:
     *         CleanManagerInterface:
     *         fileRecordManager
     *         allocStatistic: alloc statistic module
     *         CurveFSOption : Initialization parameters
     *  @return whether the initialization was successful
     */
    bool Init(std::shared_ptr<NameServerStorage>,
              std::shared_ptr<InodeIDGenerator>,
              std::shared_ptr<ChunkSegmentAllocator>,
              std::shared_ptr<CleanManagerInterface>,
              std::shared_ptr<FileRecordManager> fileRecordManager,
              std::shared_ptr<AllocStatistic> allocStatistic,
              const struct CurveFSOption &curveFSOptions,
              std::shared_ptr<Topology> topology,
              std::shared_ptr<SnapshotCloneClient> snapshotCloneClient);

    /**
     *  @brief Run session manager
     *  @param
     *  @return
     */
    void Run();

    /**
     *  @brief CurveFS Uninit
     *  @param
     *  @return
     */
    void Uninit();

    // namespace ops
    /**
     *  @brief create file
     *  @param fileName
     *         owner: the owner of the file
     *         filetype：the type of the file
     *         length：file length
     *         stripeUnit: the smallest unit of stripe
     *         stripeCount: stripe width
     *  @return return StatusCode::kOK if succeeded
     */
    StatusCode CreateFile(const std::string & fileName,
                          const std::string& owner,
                          FileType filetype,
                          uint64_t length,
                          uint64_t stripeUnit,
                          uint64_t stripeCount);
    /**
     *  @brief get file information
     *  @param filename
     *  @param inode: return the obtained file system
     *  @return status codes below:
     *          StatusCode::kOK                if succeeded
     *          StatusCode::kFileNotExists     if target file doesn't exist
     *          StatusCode::kStorageError      if failed to get file metadata
     */
    StatusCode GetFileInfo(const std::string & filename,
                           FileInfo * inode) const;

    /**
     * @brief get the fileInfo in recycleBin based on original filename
     * @param originFilename
     * @param fileId
     * @param recoverFileInfo: return the obtained fileInfo
     * @return status codes below:
     *          StatusCode::kOK                if succeeded
     *          StatusCode::kFileNotExists     if target file doesn't exist
     *          StatusCode::kStorageError      if failed to get file metadata
     */
    StatusCode GetRecoverFileInfo(const std::string& originFileName,
                                  const uint64_t fileId,
                                  FileInfo* recoverFileInfo);

     /**
     *  @brief get the allocated file size
     *  @param: fileName
     *  @param[out]: allocatedSize
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetAllocatedSize(const std::string& fileName,
                                AllocatedSize* allocatedSize);

    /**
     *  @brief get size of the file or directory
     *  @brief fileName
     *  @param[out]: size: the fileLength of the file or directory
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetFileSize(const std::string& fileName, uint64_t* size);

    /**
     *  @brief delete file
     *  @param[in] filename
     *  @param[in] fileId: there will be inodeID verification on deleted files,
     *                     except when kUnitializedFileID is passed.
     *  @param[in] deleteForce: whether to perform a force deletion. Deleted
     *                          files will be placed in recycle bin by default.
     *                          root user can prefer a force deletion.
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode DeleteFile(const std::string & filename, uint64_t fileId,
        bool deleteForce = false);

    /**
     *  @brief recover file
     *  @param[in] originFilename: filename before delete
     *  @param[in] recycleFilename: filename after delete
     *  @param[in] fileId: there will be inodeID verification on recovered files
     *                     except when kUnitializedFileID is passed.
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode RecoverFile(const std::string & originFileName,
                           const std::string & recycleFileName,
                           uint64_t fileId);

    /**
     *  @brief get information of all files in the directory
     *  @param dirname
     *  @param files: results found
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode ReadDir(const std::string & dirname,
                       std::vector<FileInfo> * files) const;

    /**
     *  @brief rename file
     *  @param oldFileName
     *  @param newFileName
     *  @param oldFileId: there will be inodeID verification, except when
     *                    kUnitializedFileID is passed.
     *  @param newFileId: there will be inodeID verification, except when
     *                    kUnitializedFileID is passed.
     *  @return StatusCode::kOK if succeeded
     */
    // TODO(hzsunjianliang): Add inode parameters of the source file for checking //NOLINT
    StatusCode RenameFile(const std::string & oldFileName,
                          const std::string & newFileName,
                          uint64_t oldFileId,
                          uint64_t newFileId);

    /**
     *  @brief extend file
     *  @param filename
     *  @param newSize
     *  @return StatusCode::kOK if succeeded
     */
    // extent size minimum unit 1GB (segement as a unit)
    StatusCode ExtendFile(const std::string &filename,
                          uint64_t newSize);

    /**
     *  @brief modify file owner information
     *  @param fileName
        @param newOwner
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode ChangeOwner(const std::string &filename,
                           const std::string &newOwner);

    // segment(chunk) ops

    /**
     *  @brief query segment information, if the segment does not exist, decide
     *         whether to create a new segment according to allocateIfNoExist
     *
     *  @param filename
     *  @param offset
     *  @param allocateIfNoExist: If the segment does not exist,
     *                            whether or not creating a new one
     *  @param segment: Return the queried segment information
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetOrAllocateSegment(
        const std::string & filename,
        offset_t offset,
        bool allocateIfNoExist, PageFileSegment *segment);

    /**
     *  @brief get the root file info
     *  @param
     *  @return return the root file info obtained
     */
    FileInfo GetRootFileInfo(void) const {
        return rootFileInfo_;
    }

    /**
     *  @brief Create a snapshot, return the created snapshot info if succeeded
     *  @param filename
     *  @param snapshotFileInfo: Info of snapshot created
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode CreateSnapShotFile(const std::string &fileName,
                            FileInfo *snapshotFileInfo);

    /**
     *  @brief get all the snapshot info of the file
     *  @param filename
     *         snapshotFileInfos: returned result
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode ListSnapShotFile(const std::string & fileName,
                            std::vector<FileInfo> *snapshotFileInfos) const;
    // async interface
    /**
     *  @brief Delete the snapshot file of the file specified by seq
     *  @param filename
     *  @param seq: Seq of snapshot
     *  @param entity: Delete snapshot entities asynchronously
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode DeleteFileSnapShotFile(const std::string &fileName,
                            FileSeqType seq,
                            std::shared_ptr<AsyncDeleteSnapShotEntity> entity);

    /**
     *  @brief Get the status of the snapshot, if the status is kFileDeleting
     *         , return the deletion progress additionally
     *  @param fileName
     *  @param seq: The sequence of the snapshot
     *  @param[out] status: File status (kFileCreated, kFileDeleting)
     *  @param[out] progress: Additional deletion progress when the status is kFileDeleting //NOLINT
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode CheckSnapShotFileStatus(const std::string &fileName,
                            FileSeqType seq, FileStatus * status,
                            uint32_t * progress) const;

    /**
     *  @brief Get snapshot info
     *  @param filename
     *  @param seq: sequence of the snapshot
     *  @param[out] snapshotFileInfo: snapshot info fetched
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetSnapShotFileInfo(const std::string &fileName,
                            FileSeqType seq, FileInfo *snapshotFileInfo) const;

    /**
     *  @brief get the segments info of the snapshot
     *  @param filename
     *  @param seq: sequence of the snapshot
     *  @param offset
     *  @param[out] segment
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetSnapShotFileSegment(
            const std::string & filename,
            FileSeqType seq,
            offset_t offset,
            PageFileSegment *segment);

    // session ops
    /**
     *  @brief open file
     *  @param filename
     *  @param clientIP
     *  @param[out] session: session information created
     *  @param[out] fileInfo: opened file information
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode OpenFile(const std::string &fileName,
                        const std::string &clientIP,
                        ProtoSession *protoSession,
                        FileInfo  *fileInfo,
                        CloneSourceSegment* cloneSourceSegment = nullptr);

    /**
     *  @brief close file
     *  @param fileName
     *  @param sessionID
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode CloseFile(const std::string &fileName,
                         const std::string &sessionID);

    /**
     *  @brief update the valid period of the session
     *  @param filename
     *  @param sessionid
     *  @param date: request time for preventing replay attacks
     *  @param signature: for authentication
     *  @param clientPort
     *  @param clientVersion
     *  @param clientIP
     *  @param[out] fileInfo: info of opened file
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode RefreshSession(const std::string &filename,
                              const std::string &sessionid,
                              const uint64_t date,
                              const std::string &signature,
                              const std::string &clientIP,
                              uint32_t clientPort,
                              const std::string &clientVersion,
                              FileInfo  *fileInfo);

    /**
     * @brief Clone a file. Clone file can only be created by the root user currently //NOLINT
     * @param filename
     * @param owner: Info of the owner who calls the interface
     * @param filetype
     * @param length
     * @param seq: version number
     * @param ChunkSizeType: The chunk size of the clone file
     * @param stripeUnit: stripe size
     * @param stripeCount: stripe count
     * @param cloneSource: Source file address, only supports CurveFS currently
     * @param cloneLength: Length of source file
     * @param[out] fileInfo: fileInfo of the clone file created
     * @return return StatusCode:kOK if succeeded
     */
    StatusCode CreateCloneFile(const std::string &filename,
                            const std::string& owner,
                            FileType filetype,
                            uint64_t length,
                            FileSeqType seq,
                            ChunkSizeType chunksize,
                            uint64_t stripeUnit,
                            uint64_t stripeCount,
                            FileInfo *fileInfo,
                            const std::string & cloneSource = "",
                            uint64_t cloneLength = 0);

    /**
     * @brief set file status to clone file
     * @param filename
     * @param fileID: set the file inodeid
     * @param fileStatus: the file status to set
     *
     * @return  return StatusCode:kOK if succeeded
     *
     */
    StatusCode SetCloneFileStatus(const std::string &filename,
                            uint64_t fileID,
                            FileStatus fileStatus);

    /**
     *  @brief check the owner of the file
     *  @param filename
     *  @param owner: file owner to check
     *  @param signature: signature for verification from user
     *  @param date: indicates the time that the request arrives
     *  @return StatusCode::kOK if succeeded, StatusCode::kOwnerAuthFail if failed //NOLINT
     */
    StatusCode CheckFileOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date);

    /**
     *  @brief Check the owner of all the files in the input path
     *  @param filename
     *  @param owner: file owner to check
     *  @param signature: signature for verification from user
     *  @param date: indicates the time that the request arrives
     *  @return StatusCode::kOK if succeeded, other code for other errors
     */
    StatusCode CheckPathOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date);

    /**
     *  @brief Check whether the owner passed in match the actual owner of the
     *         destination. This function is basically the same as
     *         CheckFileOwner, the only different is that CheckFileOwner will
     *         return a not exist error when the file corresponding to the
     *         fileName doesn't exist, but this function will return kOK.
     *  @param filename
     *  @param owner: file owner to check
     *  @param signature: signature for verification from user
     *  @param date: indicates the time that the request arrives
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode CheckDestinationOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date);

    /**
     *  @brief Check whether an owner is root
     *  @param filename
     *  @param owner
     *  @param signature: signature for verification from user
     *  @param date: indicates the time that the request arrives
     *  @return StatusCode::kOK if succeeded, StatusCode::kOwnerAuthFail if failed //NOLINT
     */
    StatusCode CheckRootOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date);

    /**
     *  @brief check the owner of the file in recycleBin
     *  @param filename
     *  @param owner: file owner to check
     *  @param signature: signature for verification from user
     *  @param date: indicates the time that the request arrives
     *  @return StatusCode::kOK if succeeded, StatusCode::kOwnerAuthFail if failed //NOLINT
     */
    StatusCode CheckRecycleFileOwner(const std::string &filename,
                                     const std::string &owner,
                                     const std::string &signature,
                                     uint64_t date);

    /**
     *  @brief Get the client information in fileRecord
     *  @param listAllClient: Whether to list all client information
     *  @param[out]: List of client info
     *  @return StatusCode::kOK if succeeded, StatusCode::KInternalError if failed //NOLINT
     */
    StatusCode ListClient(bool listAllClient,
                          std::vector<ClientInfo>* clientInfos);

    /**
     * @brief Query where a file is mounted
     * @param fileName
     * @param clientInfo: the node where the file is mounted
     * @return StatusCode::kOK if succeeded, StatusCode::kFileNotExists if failed //NOLINT
     */
    StatusCode FindFileMountPoint(const std::string& fileName,
                                  ClientInfo* clientInfo);

    /**
     * @brief List volumes on copysets
     * @param copysets
     * @param[fileNames] volumes on copysets
     * @return StatusCode::kOK if succeeded, StatusCode::kFileNotExists if failed //NOLINT
     */
    StatusCode ListVolumesOnCopyset(
                        const std::vector<common::CopysetInfo>& copysets,
                        std::vector<std::string>* fileNames);

    /**
     * @brief Update file throttle params
     * @param filename
     * @param param throttle params
     * @return StatusCode::kOK if succeeded
     */
    StatusCode UpdateFileThrottleParams(const std::string& fileName,
                                        ThrottleParams params);

    /**
     *  @brief Get the number of opened files
     *  @param
     *  @return return 0 of CurveFS has not been initialized
     */
    uint64_t GetOpenFileNum();

    /**
     *  @brief get the defaultChunkSize info of curvefs
     *  @param:
     *  @return return defaultChunkSize info obtained
     */
    uint64_t GetDefaultChunkSize();

    /**
     *  @brief get the defaultSegmentSize info of curvefs
     *  @param:
     *  @return return defaultSegmentSize info obtained
     */
    uint64_t GetDefaultSegmentSize();

    /**
     *  @brief get the minFileLength info of curvefs
     *  @param:
     *  @return return minFileLength info obtained
     */
    uint64_t GetMinFileLength();

    /**
     *  @brief get the maxFileLength info of curvefs
     *  @param:
     *  @return return maxFileLength info obtained
     */
    uint64_t GetMaxFileLength();

 private:
    CurveFS() = default;

    void InitRootFile(void);

    bool InitRecycleBinDir();

    StatusCode WalkPath(const std::string &fileName,
                        FileInfo *fileInfo, std::string  *lastEntry) const;

    StatusCode LookUpFile(const FileInfo & parentFileInfo,
                          const std::string & fileName,
                          FileInfo *fileInfo) const;

    StatusCode PutFile(const FileInfo & fileInfo);

    /**
     * @brief Execute a snapshot transaction of a fileinfo
     * @param originalFileInfo: fileInfo of the original file
     * @param SnapShotFile: snapshot file info generated
     * @return StatusCode: success or fail
     */
    StatusCode SnapShotFile(const FileInfo * originalFileInfo,
        const FileInfo * SnapShotFile) const;

    std::string GetRootOwner() {
        return rootAuthOptions_.rootOwner;
    }

    /**
     * @brief Check whether the current request date is legal. It should be
     *        within 15 minutes before and after the current time
     * @param date: request datetime
     * @return true if legal, false if not
     */
    bool CheckDate(uint64_t date);

    /**
     *  @brief Check whether the signature of the request is legal
     *  @param owner
     *  @param signature: signature for verification from user
     *  @param date: indicates the time that the request arrives
     *  @return true if legal, false if not
     */
    bool CheckSignature(const std::string& owner,
                        const std::string& signature,
                        uint64_t date);

    StatusCode CheckPathOwnerInternal(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              std::string *lastEntry,
                              uint64_t *parentID);

    /**
     *  @brief is some files in the directory
     *  @param: fileInfo：the fileInfo of directory
     *  @param: result: true(empty), false(not empty)
     *  @return: @return StatusCode::kOK if succeeded
     */
    StatusCode isDirectoryEmpty(const FileInfo &fileInfo, bool *result);

    /**
     * @brief determine whether taking snapshot is allowed for a file
     *        conditions:
     *        1.version number in filerecord is not empty and >= "0.0.6"
     *        2.no corresponding filerecord
     * @param fileName
     * @return three cases：
     *         StatusCode::kOK: snapshot allowed
     *         StatusCode::kSnapshotFrozen: snapshot function enabled
     *         StatusCode::kClientVersionNotMatch: snapshot not allowed due to
     *                                             the client version
     */
    StatusCode IsSnapshotAllowed(const std::string &fileName);

    /**
     *  @brief check whether file has changed, it need to check when
     *         deleting rename and changeowner
     *  @param: fileName
     *  @param: fileInfo
     *  @return: StatusCode::kOK if succeeded
     */
    StatusCode CheckFileCanChange(const std::string &fileName,
        const FileInfo &fileInfo);

    /**
     *  @brief Get allocated size, for both directory and file
     *  @param fileName
     *  @param fileInfo
     *  @param[out]: allocSize
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetAllocatedSize(const std::string& fileName,
                                const FileInfo& fileInfo,
                                AllocatedSize* allocSize);

    /**
     *  @brief Get allocated size for a file
     *  @param fileName
     *  @param fileInfo
     *  @param[out]: allocSize
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetFileAllocSize(const std::string& fileName,
                                const FileInfo& fileInfo,
                                AllocatedSize* allocSize);

    /**
     *  @brief Get allocated size for a directory
     *  @param dirName: directory name
     *  @param fileInfo: file info
     *  @param[out]: allocSize: allocSize to the directory
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetDirAllocSize(const std::string& fileName,
                                const FileInfo& fileInfo,
                                AllocatedSize* allocSize);

    /**
     *  @brief get the size of file or directory
     *  @param: fileName
     *  @param: fileInfo
     *  @param[out]: fileSize: the size of file or directory
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetFileSize(const std::string& fileName,
                           const FileInfo& fileInfo,
                           uint64_t* fileSize);

    /**
     *  @brief check file has rely dest file
     *  @param: fileName
     *  @param: owner
     *  @param[out]: isCloneHasRely:  is clone has rely
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode CheckHasCloneRely(const std::string & filename,
                                 const std::string &owner,
                                 bool *isHasCloneRely);


    /**
     *  @brief List all files recursively
     *  @param: inodeId The inode id of directory
     *  @param[out]: files All file info
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode ListAllFiles(uint64_t inodeId, std::vector<FileInfo>* files);

    /**
     * @brief check whether mds has started for enough time, based on the
     *        file record expiration time(mds.file.expiredTimeUs)
     * @param times multiple of file record expiration time
     * @return return true if ok, otherwise return false
     */
    bool IsStartEnoughTime(int times) const {
        std::chrono::steady_clock::duration timePass =
            std::chrono::steady_clock::now() - startTime_;
        uint32_t expiredUs = fileRecordManager_->GetFileRecordExpiredTimeUs();
        return timePass >= times * std::chrono::microseconds(expiredUs);
    }

    /**
     * @brief list clone source file's segment,
     *        if current file status is in kFileCloneMetaInstalled
     * @param fileInfo current file info
     * @param[out] cloneSourceSegment source file allocated segments
     */
    StatusCode ListCloneSourceFileSegments(
        const FileInfo* fileInfo, CloneSourceSegment* cloneSourceSegment) const;

    StatusCode CheckStripeParam(uint64_t stripeUnit,
                           uint64_t stripeCount);

    FileThrottleParams GenerateThrottleParams(uint64_t length) const;

 private:
    FileInfo rootFileInfo_;
    std::shared_ptr<NameServerStorage> storage_;
    std::shared_ptr<InodeIDGenerator> InodeIDGenerator_;
    std::shared_ptr<ChunkSegmentAllocator> chunkSegAllocator_;
    std::shared_ptr<FileRecordManager> fileRecordManager_;
    std::shared_ptr<CleanManagerInterface> cleanManager_;
    std::shared_ptr<AllocStatistic> allocStatistic_;
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<SnapshotCloneClient> snapshotCloneClient_;
    struct RootAuthOption       rootAuthOptions_;
    ThrottleOption throttleOption_;

    uint64_t defaultChunkSize_;
    uint64_t defaultSegmentSize_;
    uint64_t minFileLength_;
    uint64_t maxFileLength_;
    std::chrono::steady_clock::time_point startTime_;
};
extern CurveFS &kCurveFS;
}   // namespace mds
}   // namespace curve
#endif   // SRC_MDS_NAMESERVER2_CURVEFS_H_
