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
#include <chrono>
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
using curve::common::Authenticator;

namespace curve {
namespace mds {

struct RootAuthOption {
    std::string rootOwner;
    std::string rootPassword;
};

struct CurveFSOption {
    uint64_t defaultChunkSize;
    RootAuthOption authOptions;
    FileRecordOptions fileRecordOptions;
};

struct AllocatedSize {
    // The size of the segment allocated by MDS to the file
    uint64_t allocatedSize;
    // Actual occupied space in chunk server
    uint64_t physicalAllocatedSize;
    AllocatedSize() : allocatedSize(0), physicalAllocatedSize(0) {}
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

    bool Init(std::shared_ptr<NameServerStorage>,
              std::shared_ptr<InodeIDGenerator>,
              std::shared_ptr<ChunkSegmentAllocator>,
              std::shared_ptr<CleanManagerInterface>,
              std::shared_ptr<FileRecordManager> fileRecordManager,
              std::shared_ptr<AllocStatistic> allocStatistic,
              const struct CurveFSOption &curveFSOptions,
              std::shared_ptr<Topology> topology);

    // run session manager
    void Run();

    // CurveFS uninit
    void Uninit();

    // namespace ops

    // create files, return StatusCode::kOK if succeeded
    StatusCode CreateFile(const std::string & fileName,
                          const std::string& owner,
                          FileType filetype,
                          uint64_t length);
    /**
     *  @brief get file information
     *
     *  @param filename: just what you're expecting, file name (< _ <)
     *  @param inode: return the obtained file system
     *  @return status codes below:
     *          StatusCode::kOK                if succeeded
     *          StatusCode::kFileNotExists     if target file doesn't exist
     *          StatusCode::kStorageError      if failed to get file metadata
     */
    StatusCode GetFileInfo(const std::string & filename,
                           FileInfo * inode) const;

     /**
     *  @param: fileName
     *  @param[out]: allocatedSize
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetAllocatedSize(const std::string& fileName,
                                AllocatedSize* allocatedSize);

    /**
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
     *  @brief get information of all files in the directory
     *  @param dirname
     *  @param files: results found
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode ReadDir(const std::string & dirname,
                       std::vector<FileInfo> * files) const;

    /**
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
     *  @param filename
     *  @param clientIP
     *  @param[out] session: session information created
     *  @param[out] fileInfo: opened file information
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode OpenFile(const std::string &fileName,
                        const std::string &clientIP,
                        ProtoSession *protoSession,
                        FileInfo  *fileInfo);

    /**
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
                            FileInfo *fileInfo,
                            const std::string & cloneSource = "",
                            uint64_t cloneLength = 0);

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
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode CheckRootOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date);

    /**
     *  @brief Get the client information in fileRecord
     *  @param listAllClient: Whether to list all client information
     *  @param[out]: List of client info
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode ListClient(bool listAllClient,
                          std::vector<ClientInfo>* clientInfos);

    /**
     * @brief Query where a file is mounted
     * @param fileName
     * @param clientInfo: the node where the file is mounted
     * @return StatusCode::kOK if succeeded
     */
    StatusCode FindFileMountPoint(const std::string& fileName,
                                  ClientInfo* clientInfo);

    /**
     *  @brief Get the number of opened files
     *  @return return 0 of CurveFS has not been initialized
     */
    uint64_t GetOpenFileNum();

    uint64_t GetDefaultChunkSize();

 private:
    CurveFS() = default;

    void InitRootFile(void);

    bool InitRecycleBinDir();

    /**
     * @brief given a normal path like "a/b/c/d", traverse this path and get
     *        the parent directory 'c' of target file 'd' in Etcd storage.
     * @param[in] fileName: The complete path of a file
     * @param[out] fileInfo: file info of the parent directory of target file
     * @param[out] lastEntry: name of the target file ('d' in our example)
     * @return StatusCode
     */
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
     * @return StatusCode
     */
    StatusCode SnapShotFile(const FileInfo * originalFileInfo,
        const FileInfo * SnapShotFile) const;

    std::string GetRootOwner() {
        return rootAuthOptions_.rootOwner;
    }

    /**
     * @brief Check whether the current request date is legal. It should be
     *        within 15 minutes before and after the current time
     * @param date
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
     *  @param dirName
     *  @param fileInfo
     *  @param[out]: allocSize
     *  @return StatusCode::kOK if succeeded
     */
    StatusCode GetDirAllocSize(const std::string& fileName,
                                const FileInfo& fileInfo,
                                AllocatedSize* allocSize);

 private:
    FileInfo rootFileInfo_;
    std::shared_ptr<NameServerStorage> storage_;
    std::shared_ptr<InodeIDGenerator> InodeIDGenerator_;
    std::shared_ptr<ChunkSegmentAllocator> chunkSegAllocator_;
    std::shared_ptr<FileRecordManager> fileRecordManager_;
    std::shared_ptr<CleanManagerInterface> cleanManager_;
    std::shared_ptr<AllocStatistic> allocStatistic_;
    std::shared_ptr<Topology> topology_;
    struct RootAuthOption       rootAuthOptions_;

    uint64_t defaultChunkSize_;
    std::chrono::steady_clock::time_point startTime_;
};
extern CurveFS &kCurveFS;
}   // namespace mds
}   // namespace curve
#endif   // SRC_MDS_NAMESERVER2_CURVEFS_H_

