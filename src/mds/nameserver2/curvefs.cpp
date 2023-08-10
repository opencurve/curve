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

#include "src/mds/nameserver2/curvefs.h"
#include <glog/logging.h>
#include <memory>
#include <chrono>    //NOLINT
#include <set>
#include <utility>
#include <map>
#include "src/common/string_util.h"
#include "src/common/encode.h"
#include "src/common/timeutility.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

#include "absl/strings/match.h"

using curve::common::TimeUtility;
using curve::common::kDefaultPoolsetName;
using curve::common::StringToUll;
using curve::mds::topology::LogicalPool;
using curve::mds::topology::LogicalPoolIdType;
using curve::mds::topology::PhysicalPool;
using curve::mds::topology::PhysicalPoolIdType;
using curve::mds::topology::CopySetIdType;
using curve::mds::topology::ChunkServer;
using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::OnlineState;
using curve::mds::topology::Poolset;

namespace curve {
namespace mds {

ClientInfo EndPointToClientInfo(const butil::EndPoint& ep) {
    ClientInfo info;
    info.set_ip(butil::ip2str(ep.ip).c_str());
    info.set_port(ep.port);

    return info;
}

const std::string kSnapshotPathSeprator = "/";
const std::string kSnapshotSeqSeprator = "-";

std::string MakeSnapshotName(const std::string &fileName, FileSeqType seq) {
    return fileName + kSnapshotSeqSeprator + std::to_string(seq);
}

bool SplitSnapshotPath(const std::string &snapFilePath,
    std::string *filePath, FileSeqType *seq) {
    auto pos = snapFilePath.find_last_of(kSnapshotPathSeprator);
    if (snapFilePath.npos == pos) {
        return false;
    }
    *filePath = snapFilePath.substr(0, pos);
    std::string snapFileName = snapFilePath.substr(pos + 1);
    pos = snapFileName.find_last_of(kSnapshotSeqSeprator);
    if (snapFileName.npos == pos) {
        return false;
    }
    std::string seqStr = snapFileName.substr(pos + 1);
    return StringToUll(seqStr, seq);
}

bool CurveFS::InitRecycleBinDir() {
    FileInfo recyclebinFileInfo;

    StoreStatus ret = storage_->GetFile(ROOTINODEID, RECYCLEBINDIR,
        &recyclebinFileInfo);
    if (ret == StoreStatus::OK) {
        if (recyclebinFileInfo.parentid() != ROOTINODEID
            ||  recyclebinFileInfo.id() != RECYCLEBININODEID
            ||  recyclebinFileInfo.filename().compare(RECYCLEBINDIRNAME) != 0
            ||  recyclebinFileInfo.filetype() != FileType::INODE_DIRECTORY
            ||  recyclebinFileInfo.owner() != rootAuthOptions_.rootOwner) {
            LOG(ERROR) << "Recyclebin info error, fileInfo = "
                << recyclebinFileInfo.DebugString();
            return false;
        } else {
            LOG(INFO) << "Recycle Bin dir exist, Path = " << RECYCLEBINDIR;
            return true;
        }
    } else if ( ret == StoreStatus::KeyNotExist ) {
        // store the dir
        recyclebinFileInfo.set_parentid(ROOTINODEID);
        recyclebinFileInfo.set_id(RECYCLEBININODEID);
        recyclebinFileInfo.set_filename(RECYCLEBINDIRNAME);
        recyclebinFileInfo.set_filetype(FileType::INODE_DIRECTORY);
        recyclebinFileInfo.set_ctime(
            ::curve::common::TimeUtility::GetTimeofDayUs());
        recyclebinFileInfo.set_owner(rootAuthOptions_.rootOwner);

        StoreStatus ret2 = storage_->PutFile(recyclebinFileInfo);
        if ( ret2 != StoreStatus::OK ) {
            LOG(ERROR) << "RecycleBin dir create error, Path = "
                << RECYCLEBINDIR;
            return false;
        }
        LOG(INFO) << "RecycleBin dir create ok, Path = " << RECYCLEBINDIR;
        return true;
    } else {
        // internal error
        LOG(INFO) << "InitRecycleBinDir error ,ret = " << ret;
        return false;
    }
}

bool CurveFS::Init(std::shared_ptr<NameServerStorage> storage,
                std::shared_ptr<InodeIDGenerator> InodeIDGenerator,
                std::shared_ptr<CloneIDGenerator> cloneIdGenerator,
                std::shared_ptr<ChunkSegmentAllocator> chunkSegAllocator,
                std::shared_ptr<CleanManagerInterface> cleanManager,
                std::shared_ptr<FileRecordManager> fileRecordManager,
                std::shared_ptr<AllocStatistic> allocStatistic,
                const struct CurveFSOption &curveFSOptions,
                std::shared_ptr<Topology> topology,
                std::shared_ptr<SnapshotCloneClient> snapshotCloneClient,
                const std::shared_ptr<FlattenManager> &flattenManager) {
    startTime_ = std::chrono::steady_clock::now();
    storage_ = storage;
    InodeIDGenerator_ = InodeIDGenerator;
    cloneIdGenerator_ = cloneIdGenerator;
    chunkSegAllocator_ = chunkSegAllocator;
    cleanManager_ = cleanManager;
    allocStatistic_ = allocStatistic;
    fileRecordManager_ = fileRecordManager;
    rootAuthOptions_ = curveFSOptions.authOptions;

    defaultChunkSize_ = curveFSOptions.defaultChunkSize;
    defaultSegmentSize_ = curveFSOptions.defaultSegmentSize;
    minFileLength_ = curveFSOptions.minFileLength;
    maxFileLength_ = curveFSOptions.maxFileLength;
    topology_ = topology;
    snapshotCloneClient_ = snapshotCloneClient;
    flattenManager_ = flattenManager;
    poolsetRules_ = curveFSOptions.poolsetRules;

    InitRootFile();
    bool ret = InitRecycleBinDir();
    if (!ret) {
        LOG(ERROR) << "Init RecycleBin fail!";
        return false;
    }

    fileRecordManager_->Init(curveFSOptions.fileRecordOptions);

    return true;
}

void CurveFS::Run() {
    fileRecordManager_->Start();
}

void CurveFS::Uninit() {
    fileRecordManager_->Stop();
    storage_ = nullptr;
    InodeIDGenerator_ = nullptr;
    chunkSegAllocator_ = nullptr;
    cleanManager_ = nullptr;
    allocStatistic_ = nullptr;
    fileRecordManager_ = nullptr;
    snapshotCloneClient_ = nullptr;
}

void CurveFS::InitRootFile(void) {
    rootFileInfo_.set_id(ROOTINODEID);
    rootFileInfo_.set_filename(ROOTFILENAME);
    rootFileInfo_.set_filetype(FileType::INODE_DIRECTORY);
    rootFileInfo_.set_owner(GetRootOwner());
}

StatusCode CurveFS::WalkPath(const std::string &fileName,
                        FileInfo *fileInfo, std::string  *lastEntry) const  {
    assert(lastEntry != nullptr);

    std::vector<std::string> paths;
    ::curve::common::SplitString(fileName, "/", &paths);

    if ( paths.size() == 0 ) {
        fileInfo->CopyFrom(rootFileInfo_);
        return StatusCode::kOK;
    }

    *lastEntry = paths.back();
    uint64_t parentID = rootFileInfo_.id();

    for (uint32_t i = 0; i < paths.size() - 1; i++) {
        auto ret = storage_->GetFile(parentID, paths[i], fileInfo);

        if (ret ==  StoreStatus::OK) {
            if (fileInfo->filetype() !=  FileType::INODE_DIRECTORY) {
                LOG(INFO) << fileInfo->filename() << " is not an directory";
                return StatusCode::kNotDirectory;
            }
        } else if (ret == StoreStatus::KeyNotExist) {
            return StatusCode::kFileNotExists;
        } else {
            LOG(ERROR) << "GetFile error, errcode = " << ret;
            return StatusCode::kStorageError;
        }
        // assert(fileInfo->parentid() != parentID);
        parentID =  fileInfo->id();
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::LookUpFile(const FileInfo & parentFileInfo,
                    const std::string &fileName, FileInfo *fileInfo) const {
    assert(fileInfo != nullptr);

    auto ret = storage_->GetFile(parentFileInfo.id(), fileName, fileInfo);

    if (ret == StoreStatus::OK) {
        return StatusCode::kOK;
    } else if  (ret == StoreStatus::KeyNotExist) {
        return StatusCode::kFileNotExists;
    } else {
        return StatusCode::kStorageError;
    }
}

StatusCode CurveFS::LookUpSnapFile(const FileInfo & parentFileInfo,
                    const std::string &fileName, FileInfo *fileInfo) const {
    assert(fileInfo != nullptr);

    auto ret = storage_->GetSnapFile(parentFileInfo.id(), fileName, fileInfo);

    if (ret == StoreStatus::OK) {
        return StatusCode::kOK;
    } else if  (ret == StoreStatus::KeyNotExist) {
        return StatusCode::kFileNotExists;
    } else {
        return StatusCode::kStorageError;
    }
}

// StatusCode PutFileInternal()

StatusCode CurveFS::PutFile(const FileInfo & fileInfo) {
    if (storage_->PutFile(fileInfo) != StoreStatus::OK) {
        return StatusCode::kStorageError;
    } else {
        return StatusCode::kOK;
    }
}

StatusCode CurveFS::SnapShotFile(const FileInfo * origFileInfo,
                                const FileInfo * snapshotFile) const {
    if (storage_->SnapShotFile(origFileInfo, snapshotFile) != StoreStatus::OK) {
        return StatusCode::kStorageError;
    } else {
        return StatusCode::kOK;
    }
}

StatusCode CurveFS::CreateFile(const std::string& fileName,
                               std::string poolset,
                               const std::string& owner,
                               FileType filetype,
                               uint64_t length,
                               uint64_t stripeUnit,
                               uint64_t stripeCount) {
    FileInfo parentFileInfo;
    std::string lastEntry;

    if (filetype != FileType::INODE_PAGEFILE
            && filetype !=  FileType::INODE_DIRECTORY) {
        LOG(ERROR) << "CreateFile not support create file type : " << filetype
                   << ", fileName = " << fileName;
        return StatusCode::kNotSupported;
    }

    // check param
    if (filetype == FileType::INODE_PAGEFILE) {
        StatusCode retCode = CheckOrAssignPoolset(fileName, &poolset);
        if (retCode != StatusCode::kOK) {
            return retCode;
        }

        if  (length < minFileLength_) {
            LOG(ERROR) << "file Length < MinFileLength " << minFileLength_
                       << ", length = " << length;
            return StatusCode::kFileLengthNotSupported;
        }

        if (length > maxFileLength_) {
            LOG(ERROR) << "CreateFile file length > maxFileLength, fileName = "
                       << fileName << ", length = " << length
                       << ", maxFileLength = " << maxFileLength_;
            return StatusCode::kFileLengthNotSupported;
        }

        if (length % defaultSegmentSize_ != 0) {
            LOG(ERROR) << "Create file length not align to segment size, "
                       << "fileName = " << fileName
                       << ", length = " << length
                       << ", segment size = " << defaultSegmentSize_;
            return StatusCode::kFileLengthNotSupported;
        }
    }

    std::vector<PoolIdType> logicalPools =
                topology_->GetLogicalPoolInCluster();
    std::map<PoolIdType, double> remianingSpace;
    uint64_t allRemianingSpace = 0;
    chunkSegAllocator_->GetRemainingSpaceInLogicalPool(
        logicalPools, &remianingSpace, poolset);

    for (auto it = remianingSpace.begin(); it != remianingSpace.end(); it++) {
        allRemianingSpace +=it->second;
    }
    if (allRemianingSpace < length) {
        LOG(ERROR) << "CreateFile file length > LeftSize, fileName = "
                    << fileName << ", length = " << length;
        return StatusCode::kFileLengthNotSupported;
    }

    auto ret = CheckStripeParam(defaultSegmentSize_, defaultChunkSize_,
                                stripeUnit, stripeCount);
    if (ret != StatusCode::kOK) {
        return ret;
    }
    ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        return ret;
    }
    if (lastEntry.empty()) {
        return StatusCode::kFileExists;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret == StatusCode::kOK) {
        return StatusCode::kFileExists;
    }

    if (ret != StatusCode::kFileNotExists) {
         return ret;
    } else {
        InodeID inodeID;
        if ( InodeIDGenerator_->GenInodeID(&inodeID) != true ) {
            LOG(ERROR) << "GenInodeID  error";
            return StatusCode::kStorageError;
        }

        fileInfo.set_id(inodeID);
        fileInfo.set_filename(lastEntry);
        fileInfo.set_poolset(poolset);
        fileInfo.set_parentid(parentFileInfo.id());
        fileInfo.set_filetype(filetype);
        fileInfo.set_owner(owner);
        fileInfo.set_chunksize(defaultChunkSize_);
        fileInfo.set_segmentsize(defaultSegmentSize_);
        fileInfo.set_length(length);
        fileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
        fileInfo.set_seqnum(kStartSeqNum);
        fileInfo.set_filestatus(FileStatus::kFileCreated);
        fileInfo.set_stripeunit(stripeUnit);
        fileInfo.set_stripecount(stripeCount);
        fileInfo.set_blocksize(g_block_size);

        ret = PutFile(fileInfo);
        return ret;
    }
}

StatusCode CurveFS::GetFileInfo(const std::string & filename,
                                FileInfo *fileInfo) const {
    assert(fileInfo != nullptr);
    std::string lastEntry;
    FileInfo parentFileInfo;
    auto ret = WalkPath(filename, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        if (ret == StatusCode::kNotDirectory) {
            return StatusCode::kFileNotExists;
        }
        return ret;
    } else {
        if (lastEntry.empty()) {
            fileInfo->CopyFrom(parentFileInfo);
            return StatusCode::kOK;
        }
        return LookUpFile(parentFileInfo, lastEntry, fileInfo);
    }
}

StatusCode CurveFS::GetRecoverFileInfo(const std::string& originFileName,
                                       const uint64_t fileId,
                                       FileInfo* recoverFileInfo) {
    std::vector<FileInfo> fileInfoList;
    recoverFileInfo->set_ctime(0);
    bool findRecoverFile = false;

    StatusCode retCode = ReadDir(RECYCLEBINDIR, &fileInfoList);
    if (retCode != StatusCode::kOK)  {
        LOG(ERROR) <<"ReadDir fail, filename = " <<  RECYCLEBINDIR
            << ", statusCode = " << retCode
            << ", StatusCode_Name = " << StatusCode_Name(retCode);
        return retCode;
    } else {
        if (fileId != kUnitializedFileID) {
            for (auto iter = fileInfoList.begin(); iter != fileInfoList.end();
                 ++iter) {
                // recover the specified file
                if (fileId == iter->id()) {
                    recoverFileInfo->CopyFrom(*iter);
                    return StatusCode::kOK;
                }
            }
        }
        for (auto iter = fileInfoList.begin(); iter != fileInfoList.end();
                ++iter) {
            // recover the newer file
            if (iter->originalfullpathname() == originFileName &&
                iter->ctime() > recoverFileInfo->ctime()) {
                recoverFileInfo->CopyFrom(*iter);
                findRecoverFile = true;
            }
        }
        if (findRecoverFile != true) {
            return StatusCode::kFileNotExists;
        }
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::GetFileInfoWithCloneChain(const std::string & filename,
                                FileInfo *fileInfo) const {
    assert(fileInfo != nullptr);
    std::string lastEntry;
    FileInfo parentFileInfo;
    auto ret = WalkPath(filename, &parentFileInfo, &lastEntry);
    if (ret != StatusCode::kOK) {
        if (ret == StatusCode::kNotDirectory) { return StatusCode::kFileNotExists;
        }
        return ret;
    } else {
        if (lastEntry.empty()) {
            fileInfo->CopyFrom(parentFileInfo);
            return StatusCode::kOK;
        }
        ret = LookUpFile(parentFileInfo, lastEntry, fileInfo);
        if (ret != StatusCode::kOK) {
            return ret;
        }
        if (fileInfo->filetype() == FileType::INODE_CLONE_PAGEFILE) {
            return LookUpCloneChain(*fileInfo, fileInfo);
        }
        return ret;
    }
}

AllocatedSize& AllocatedSize::operator+=(const AllocatedSize& rhs) {
    total += rhs.total;
    for (const auto& item : rhs.allocSizeMap) {
        allocSizeMap[item.first] += item.second;
    }
    return *this;
}

StatusCode CurveFS::GetAllocatedSize(const std::string& fileName,
                                     AllocatedSize* allocatedSize) {
    assert(allocatedSize != nullptr);
    allocatedSize->total = 0;
    allocatedSize->allocSizeMap.clear();
    FileInfo fileInfo;
    auto ret = GetFileInfo(fileName, &fileInfo);
    if (ret != StatusCode::kOK) {
        return ret;
    }

    if (fileInfo.filetype() != curve::mds::FileType::INODE_DIRECTORY &&
        fileInfo.filetype() != curve::mds::FileType::INODE_PAGEFILE &&
        fileInfo.filetype() != curve::mds::FileType::INODE_CLONE_PAGEFILE) {
        LOG(ERROR) << "GetAllocatedSize not support file type : "
                   << fileInfo.filetype() << ", fileName = " << fileName;
        return StatusCode::kNotSupported;
    }

    return GetAllocatedSize(fileName, fileInfo, allocatedSize);
}

StatusCode CurveFS::GetAllocatedSize(const std::string& fileName,
                                     const FileInfo& fileInfo,
                                     AllocatedSize* allocSize) {
    if (fileInfo.filetype() != curve::mds::FileType::INODE_DIRECTORY) {
        return GetFileAllocSize(fileName, fileInfo, allocSize);
    } else {
        // for directory, calculate the size of each file recursively and sum up
        return GetDirAllocSize(fileName, fileInfo, allocSize);
    }
}

StatusCode CurveFS::GetFileAllocSize(const std::string& fileName,
                                     const FileInfo& fileInfo,
                                     AllocatedSize* allocSize) {
    std::vector<PageFileSegment> segments;
    auto listSegmentRet = storage_->ListSegment(fileInfo.id(), &segments);

    if (listSegmentRet != StoreStatus::OK) {
        return StatusCode::kStorageError;
    }
    for (const auto& segment : segments) {
        const auto & poolId = segment.logicalpoolid();
        allocSize->allocSizeMap[poolId] += fileInfo.segmentsize();
    }
    allocSize->total = fileInfo.segmentsize() * segments.size();
    return StatusCode::kOK;
}

StatusCode CurveFS::GetDirAllocSize(const std::string& fileName,
                                    const FileInfo& fileInfo,
                                    AllocatedSize* allocSize) {
    std::vector<FileInfo> files;
    StatusCode ret = ReadDir(fileName, &files);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "ReadDir Fail, fileName: " << fileName;
        return ret;
    }
    for (const auto& file : files) {
        std::string fullPathName;
        if (fileName == "/") {
            fullPathName = fileName + file.filename();
        } else {
            fullPathName = fileName + "/" + file.filename();
        }
        AllocatedSize size;
        if (GetAllocatedSize(fullPathName, file, &size) != 0) {
            std::cout << "Get allocated size of " << fullPathName
                      << " fail!" << std::endl;
            continue;
        }
        *allocSize += size;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::GetFileSize(const std::string& fileName, uint64_t* size) {
    assert(size != nullptr);
    *size = 0;
    FileInfo fileInfo;
    auto ret = GetFileInfo(fileName, &fileInfo);
    if (ret != StatusCode::kOK) {
        return ret;
    }

    if (fileInfo.filetype() != curve::mds::FileType::INODE_DIRECTORY &&
        fileInfo.filetype() != curve::mds::FileType::INODE_PAGEFILE &&
        fileInfo.filetype() != curve::mds::FileType::INODE_CLONE_PAGEFILE) {
        LOG(ERROR) << "GetFileSize not support file type : "
                   << fileInfo.filetype() << ", fileName = " << fileName;
        return StatusCode::kNotSupported;
    }
    return GetFileSize(fileName, fileInfo, size);
}

StatusCode CurveFS::GetFileSize(const std::string& fileName,
                                const FileInfo& fileInfo,
                                uint64_t* fileSize) {
    // return file length if it is a file
    switch (fileInfo.filetype()) {
        case FileType::INODE_PAGEFILE: 
        case FileType::INODE_CLONE_PAGEFILE: {
            *fileSize = fileInfo.length();
            return StatusCode::kOK;
        }
        case FileType::INODE_SNAPSHOT_PAGEFILE: {
            // Do not count snapshot file size, set fileSize=0
            *fileSize = 0;
            return StatusCode::kOK;
        }
        case FileType::INODE_DIRECTORY: {
            break;
        }
        default: {
            LOG(ERROR) << "Get file size of type "
                       << FileType_Name(fileInfo.filetype())
                       << " not supported";
            return StatusCode::kNotSupported;
        }
    }
    // if it is a directory, list the dir and calculate file size recursively
    std::vector<FileInfo> files;
    StatusCode ret = ReadDir(fileName, &files);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "ReadDir Fail, fileName: " << fileName
                   << ", error code: " << ret;
        return ret;
    }
    for (auto& file : files) {
        std::string fullPathName;
        if (fileName == "/") {
            fullPathName = fileName + file.filename();
        } else {
            fullPathName = fileName + "/" + file.filename();
        }
        uint64_t size = 0;
        ret = GetFileSize(fullPathName, file, &size);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "Get file size of " << fullPathName
                       << " fail, error code: " << ret;
            return ret;
        }
        *fileSize += size;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::isDirectoryEmpty(const FileInfo &fileInfo, bool *result) {
    assert(fileInfo.filetype() == FileType::INODE_DIRECTORY);
    std::vector<FileInfo> fileInfoList;
    auto storeStatus = storage_->ListFile(fileInfo.id(), fileInfo.id() + 1,
                                          &fileInfoList);
    if (storeStatus == StoreStatus::KeyNotExist) {
        *result = true;
        return StatusCode::kOK;
    }

    if (storeStatus != StoreStatus::OK) {
        LOG(ERROR) << "list file fail, inodeid = " << fileInfo.id()
                   << ", dir name = " << fileInfo.filename();
        return StatusCode::kStorageError;
    }

    if (fileInfoList.size() ==  0) {
        *result = true;
        return StatusCode::kOK;
    }

    *result = false;
    return StatusCode::kOK;
}

StatusCode CurveFS::IsSnapshotAllowed(const std::string &fileName) {
    if (!IsStartEnoughTime(10)) {
        LOG(INFO) << "snapshot is not allowed now, fileName = " << fileName;
        return StatusCode::kSnapshotFrozen;
    }

    // the client version satisfies the conditions
    std::string clientVersion;
    bool exist = fileRecordManager_->GetMinimumFileClientVersion(
        fileName, &clientVersion);
    if (!exist) {
        return StatusCode::kOK;
    }

    if (clientVersion < kLeastSupportSnapshotClientVersion) {
        LOG(INFO) << "current client version is not support snapshot"
                  << ", fileName = " << fileName
                  << ", clientVersion = " << clientVersion
                  << ", leastSupportSnapshotClientVersion = "
                  << kLeastSupportSnapshotClientVersion;
        return StatusCode::kClientVersionNotMatch;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::DeleteFile(const std::string & filename, uint64_t fileId,
                            bool deleteForce) {
    std::string lastEntry;
    FileInfo parentFileInfo;
    auto ret = WalkPath(filename, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        return ret;
    }

    if (lastEntry.empty()) {
        LOG(INFO) << "can not remove rootdir";
        return StatusCode::kParaError;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "delete file lookupfile fail, fileName = " << filename;
        return ret;
    }

    if (fileId != kUnitializedFileID && fileId != fileInfo.id()) {
        LOG(WARNING) << "delete file, file id missmatch"
                   << ", fileName = " << filename
                   << ", fileInfo.id() = " << fileInfo.id()
                   << ", para fileId = " << fileId;
        return StatusCode::kFileIdNotMatch;
    }

    if (fileInfo.filetype() == FileType::INODE_DIRECTORY) {
        // if there are still files in it, the directory cannot be deleted
        bool isEmpty = false;
        auto ret1 = isDirectoryEmpty(fileInfo, &isEmpty);
        if (ret1 != StatusCode::kOK) {
            LOG(ERROR) << "check is directory empty fail, filename = "
                       << filename << ", ret = " << ret1;
            return ret1;
        }
        if (!isEmpty) {
            LOG(WARNING) << "delete file, file is directory and not empty"
                       << ", filename = " << filename;
            return StatusCode::kDirNotEmpty;
        }
        auto ret = storage_->DeleteFile(fileInfo.parentid(),
                                                fileInfo.filename());
        if (ret != StoreStatus::OK) {
            LOG(ERROR) << "delete file, file is directory and delete fail"
                       << ", filename = " << filename
                       << ", ret = " << ret;
            return StatusCode::kStorageError;
        }

        LOG(INFO) << "delete file success, file is directory"
                  << ", filename = " << filename;
        return StatusCode::kOK;
    } else if (fileInfo.filetype() == FileType::INODE_PAGEFILE) {
        StatusCode ret = CheckFileCanChange(filename, fileInfo);
        if (ret == StatusCode::kDeleteFileBeingCloned) {
            bool isHasCloneRely;
            StatusCode ret1 = CheckHasCloneRely(filename,
                                                       fileInfo.owner(),
                                                       &isHasCloneRely);
            if (ret1 != StatusCode::kOK) {
                LOG(ERROR) << "delete file, check file clone ref fail,"
                           << "filename = " << filename
                           << ", ret = " << ret1;
                return ret1;
            }

            if (isHasCloneRely) {
                LOG(WARNING) << "delete file, can not delete file, "
                            << "file has clone rely, filename = " << filename;
                return StatusCode::kDeleteFileBeingCloned;
            }
        } else if (ret != StatusCode::kOK) {
            LOG(ERROR) << "delete file, can not delete file"
                       << ", filename = " << filename
                       << ", ret = " << ret;
            return ret;
        }
        if (deleteForce == false) {
            // move the file to the recycle bin
            FileInfo recycleFileInfo;
            recycleFileInfo.CopyFrom(fileInfo);
            recycleFileInfo.set_parentid(RECYCLEBININODEID);
            recycleFileInfo.set_filename(fileInfo.filename() + "-" +
                std::to_string(recycleFileInfo.id()));
            recycleFileInfo.set_originalfullpathname(filename);

            StoreStatus ret1 =
                storage_->MoveFileToRecycle(fileInfo, recycleFileInfo);
            if (ret1 != StoreStatus::OK) {
                LOG(ERROR) << "delete file, move file to recycle fail"
                        << ", filename = " << filename
                        << ", ret = " << ret1;
                return StatusCode::kStorageError;
            }
            LOG(INFO) << "file delete to recyclebin, fileName = " << filename
                      << ", recycle filename = " << recycleFileInfo.filename();
            return StatusCode::kOK;
        } else {
            // direct removefile is not support
            if (fileInfo.parentid() != RECYCLEBININODEID) {
                LOG(WARNING)
                    << "force delete file not in recyclebin"
                    << "not support yet, filename = " << filename;
                return StatusCode::kNotSupported;
            }

            if (fileInfo.filestatus() == FileStatus::kFileDeleting) {
                LOG(INFO) << "file is underdeleting, filename = " << filename;
                return StatusCode::kFileUnderDeleting;
            }

            // check whether the task already exist
            if ( cleanManager_->GetTask(fileInfo.id()) != nullptr ) {
                LOG(WARNING) << "filename = " << filename << ", inode = "
                    << fileInfo.id() << ", deleteFile task already submited";
                return StatusCode::kOK;
            }

            fileInfo.set_filestatus(FileStatus::kFileDeleting);
            auto ret = PutFile(fileInfo);
            if (ret != StatusCode::kOK) {
                LOG(ERROR) << "delete file put deleting file fail, filename = "
                           << filename << ", retCode = " << ret;
                return StatusCode::KInternalError;
            }

            // submit a file deletion task
            if (!cleanManager_->SubmitDeleteCommonFileJob(fileInfo)) {
                LOG(ERROR) << "fileName = " << filename
                        << ", inode = " << fileInfo.id()
                        << ", submit delete file job fail.";
                return StatusCode::KInternalError;
            }

            LOG(INFO) << "delete file task submitted, file is pagefile"
                        << ", inode = " << fileInfo.id()
                        << ", filename = " << filename;
            return StatusCode::kOK;
        }
     } else {
        // Currently deletefile only supports the deletion of INODE_DIRECTORY
        // and INODE_PAGEFILE type files.
        // INODE_SNAPSHOT_PAGEFILE is not deleted by this interface to delete.
        // Other file types are temporarily not supported.
        LOG(ERROR) << "delete file fail, file type not support delete"
                   << ", filename = " << filename
                   << ", fileType = " << fileInfo.filetype();
        return kNotSupported;
    }
}

StatusCode CurveFS::CheckOrAssignPoolset(const std::string& filename,
                                         std::string* poolset) const {
    const auto names = topology_->GetPoolsetNameInCluster();
    if (names.empty()) {
        LOG(WARNING) << "Cluster doesn't have poolsets";
        return StatusCode::kPoolsetNotExist;
    }

    if (poolset->empty()) {
        *poolset = SelectPoolsetByRules(filename, poolsetRules_);
        LOG(INFO) << "Poolset is empty, set to: " << *poolset;
    }

    auto it = std::find(names.begin(), names.end(), *poolset);
    if (it == names.end()) {
        LOG(WARNING) << "Poolset `" << *poolset << "` not found";
        return StatusCode::kPoolsetNotExist;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::RecoverFile(const std::string & originFileName,
                                const std::string & recycleFileName,
                                uint64_t fileId) {
    // check the same file exists
    FileInfo parentFileInfo;
    std::string lastEntry;
    auto ret = WalkPath(originFileName, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        LOG(WARNING) << "WalkPath failed, the middle path of "
                     << originFileName << " is not exist";
        return ret;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret == StatusCode::kOK) {
        return StatusCode::kFileExists;
    }

    FileInfo  recycleFileInfo;
    ret = GetFileInfo(recycleFileName, &recycleFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get recycle file error, recycleFilename = "
                  << recycleFileName << ", errCode = " << ret;
        return ret;
    }

    if (fileId != kUnitializedFileID && fileId != recycleFileInfo.id()) {
        LOG(WARNING) << "recover fail, recycleFileId missmatch"
                     << ", recycleFileName = " << recycleFileName
                     << ", originFileName = " << originFileName
                     << ", recycleFileInfo.id = " << recycleFileInfo.id()
                     << ", fileId = " << fileId;
        return StatusCode::kFileIdNotMatch;
    }

    // determine whether recycleFileName can be recovered
    switch (recycleFileInfo.filestatus()) {
        case FileStatus::kFileDeleting:
            LOG(ERROR) << "recover fail, can not recover file under deleting"
                    << ", recycleFileName = " << recycleFileName;
            return StatusCode::kFileUnderDeleting;
        case FileStatus::kFileCloneMetaInstalled:
            LOG(ERROR) << "recover fail, can not recover file "
                    << "cloneMetaInstalled, recycleFileName = "
                    << recycleFileName;
            return StatusCode::kRecoverFileCloneMetaInstalled;
        case FileStatus::kFileCloning:
            LOG(ERROR) << "recover fail, can not recover file cloning"
                    << ", recycleFileName = " << recycleFileName;
            return StatusCode::kRecoverFileError;
        default:
            break;
    }

    FileInfo recoverFileInfo;
    recoverFileInfo.CopyFrom(recycleFileInfo);
    recoverFileInfo.set_parentid(parentFileInfo.id());
    recoverFileInfo.set_filename(lastEntry);
    recoverFileInfo.clear_originalfullpathname();
    if (recycleFileInfo.filestatus() == FileStatus::kFileBeingCloned) {
        recycleFileInfo.set_filestatus(FileStatus::kFileCreated);
    }

    auto ret1 = storage_->RenameFile(recycleFileInfo, recoverFileInfo);
    if ( ret1 != StoreStatus::OK ) {
        LOG(ERROR) << "storage_ recoverfile error, error = " << ret1;
        return StatusCode::kStorageError;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::IncreaseFileEpoch(const std::string &filename,
    FileInfo *fileInfo,
    ::google::protobuf::RepeatedPtrField<ChunkServerLocation> *cslocs) {
    assert(fileInfo != nullptr);
    assert(cslocs != nullptr);
    std::string lastEntry;
    FileInfo parentFileInfo;
    auto ret = WalkPath(filename, &parentFileInfo, &lastEntry);
    if (ret != StatusCode::kOK) {
        if (ret == StatusCode::kNotDirectory) {
            return StatusCode::kFileNotExists;
        }
        return ret;
    } else {
        if (lastEntry.empty()) {
            LOG(ERROR) << "IncreaseFileEpoch found a directory"
                       << ", filename: " << filename;
            return StatusCode::kParaError;
        }
        ret = LookUpFile(parentFileInfo, lastEntry, fileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "LookUpFile failed, ret: " << ret
                       << ", filename: " << filename;
            return ret;
        }
        if (fileInfo->has_epoch()) {
            uint64_t old = fileInfo->epoch();
            fileInfo->set_epoch(++old);
        } else {
            fileInfo->set_epoch(1);
        }
        ret = PutFile(*fileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "Put File faied, ret: " << ret
                       << ", filename: " << filename;
            return ret;
        }

        // add chunkserver locations
        std::vector<ChunkServerIdType> chunkserverlist =
            topology_->GetChunkServerInCluster(
                [] (const ChunkServer &cs) {
                    return cs.GetStatus() != ChunkServerStatus::RETIRED;
                });
        for (auto &id : chunkserverlist) {
            ChunkServer cs;
            if (topology_->GetChunkServer(id, &cs)) {
                ChunkServerLocation *lc = cslocs->Add();
                lc->set_chunkserverid(id);
                lc->set_hostip(cs.GetHostIp());
                lc->set_port(cs.GetPort());
                lc->set_externalip(cs.GetExternalHostIp());
            }
        }
        return StatusCode::kOK;
    }
}

// TODO(hzsunjianliang): CheckNormalFileDeleteStatus?

StatusCode CurveFS::ReadDir(const std::string & dirname,
                            std::vector<FileInfo> * files) const {
    assert(files != nullptr);

    FileInfo fileInfo;
    auto ret = GetFileInfo(dirname, &fileInfo);
    if (ret != StatusCode::kOK) {
        if ( ret == StatusCode::kFileNotExists ) {
            return StatusCode::kDirNotExist;
        }
        return ret;
    }

    if (fileInfo.filetype() != FileType::INODE_DIRECTORY) {
        return StatusCode::kNotDirectory;
    }

    if (storage_->ListFile(fileInfo.id(),
                            fileInfo.id() + 1,
                            files) != StoreStatus::OK ) {
        return StatusCode::kStorageError;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::CheckFileCanChange(const std::string &fileName,
    const FileInfo &fileInfo) {
    // Check if the file has a snapshot
    std::vector<FileInfo> snapshotFileInfos;
    auto ret = ListSnapShotFile(fileName, &snapshotFileInfos);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "CheckFileCanChange, list snapshot file fail"
                    << ", fileName = " << fileName;
        return ret;
    }

    if (snapshotFileInfos.size() != 0) {
        LOG(WARNING) << "CheckFileCanChange, file is under snapshot, "
                     << "cannot delete or rename, fileName = " << fileName;
        return StatusCode::kFileUnderSnapShot;
    }

    if (fileInfo.filestatus() == FileStatus::kFileBeingCloned) {
        LOG(WARNING) << "CheckFileCanChange, file is being Cloned, "
                   << "need check first, fileName = " << fileName;
        return StatusCode::kDeleteFileBeingCloned;
    }

    // since the file record is not persistent, after mds switching the leader,
    // file record manager is empty
    // after file is opened, there will be refresh session requests in each
    // file record expiration time
    // so wait for a file record expiration time to make sure that
    // the file record is updated
    if (!IsStartEnoughTime(1)) {
        LOG(WARNING) << "MDS doesn't start enough time";
        return StatusCode::kNotSupported;
    }

    std::vector<butil::EndPoint> endPoints;
    if (fileRecordManager_->FindFileMountPoint(fileName, &endPoints) &&
        !endPoints.empty()) {
        LOG(WARNING) << fileName << " has " << endPoints.size()
                     << " mount points";
        return StatusCode::kFileOccupied;
    }

    return StatusCode::kOK;
}

// TODO(hzchenwei3): change oldFileName to sourceFileName
//                   and newFileName to destFileName)
StatusCode CurveFS::RenameFile(const std::string & oldFileName,
                               const std::string & newFileName,
                               uint64_t oldFileId, uint64_t newFileId) {
    if (oldFileName == "/" || newFileName  == "/") {
        return StatusCode::kParaError;
    }

    if (!oldFileName.compare(newFileName)) {
        LOG(INFO) << "rename same name, oldFileName = " << oldFileName
                  << ", newFileName = " << newFileName;
        return StatusCode::kFileExists;
    }

    FileInfo  oldFileInfo;
    StatusCode ret = GetFileInfo(oldFileName, &oldFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return ret;
    }

    if (oldFileId != kUnitializedFileID && oldFileId != oldFileInfo.id()) {
        LOG(WARNING) << "rename file, oldFileId missmatch"
                   << ", oldFileName = " << oldFileName
                   << ", newFileName = " << newFileName
                   << ", oldFileInfo.id() = " << oldFileInfo.id()
                   << ", oldFileId = " << oldFileId;
        return StatusCode::kFileIdNotMatch;
    }

    // only the rename of INODE_PAGEFILE is supported
    if (oldFileInfo.filetype() != FileType::INODE_PAGEFILE) {
        LOG(ERROR) << "rename oldFileName = " << oldFileName
                   << ", fileType not support, fileType = "
                   << oldFileInfo.filetype();
        return StatusCode::kNotSupported;
    }

    // determine whether oldFileName can be renamed (whether being used,
    // during snapshot or being cloned)
    ret = CheckFileCanChange(oldFileName, oldFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "rename fail, can not rename file"
                << ", oldFileName = " << oldFileName
                << ", ret = " << ret;
        return ret;
    }

    FileInfo parentFileInfo;
    std::string  lastEntry;
    auto ret2 = WalkPath(newFileName, &parentFileInfo, &lastEntry);
    if (ret2 != StatusCode::kOK) {
        LOG(WARNING) << "dest middle dir not exist";
        return StatusCode::kFileNotExists;
    }

    FileInfo existNewFileInfo;
    auto ret3 = LookUpFile(parentFileInfo, lastEntry, &existNewFileInfo);
    if (ret3 == StatusCode::kOK) {
        if (newFileId != kUnitializedFileID
            && newFileId != existNewFileInfo.id()) {
            LOG(WARNING) << "rename file, newFileId missmatch"
                        << ", oldFileName = " << oldFileName
                        << ", newFileName = " << newFileName
                        << ", newFileInfo.id() = " << existNewFileInfo.id()
                        << ", newFileId = " << newFileId;
            return StatusCode::kFileIdNotMatch;
        }

        // determine whether it can be covered. Judge the file type first
         if (existNewFileInfo.filetype() != FileType::INODE_PAGEFILE) {
            LOG(ERROR) << "rename oldFileName = " << oldFileName
                       << " to newFileName = " << newFileName
                       << "file type mismatch. old fileType = "
                       << oldFileInfo.filetype() << ", new fileType = "
                       << existNewFileInfo.filetype();
            return StatusCode::kFileExists;
        }

        // determine whether newFileName can be renamed (whether being used,
        // during snapshot or being cloned)
        StatusCode ret = CheckFileCanChange(newFileName, existNewFileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "cannot rename file"
                        << ", newFileName = " << newFileName
                        << ", ret = " << ret;
            return ret;
        }

        // move existNewFileInfo to the recycle bin
        FileInfo recycleFileInfo;
        recycleFileInfo.CopyFrom(existNewFileInfo);
        recycleFileInfo.set_parentid(RECYCLEBININODEID);
        recycleFileInfo.set_filename(recycleFileInfo.filename() + "-" +
                std::to_string(recycleFileInfo.id()));
        recycleFileInfo.set_originalfullpathname(newFileName);

        // rename!
        FileInfo newFileInfo;
        newFileInfo.CopyFrom(oldFileInfo);
        newFileInfo.set_parentid(parentFileInfo.id());
        newFileInfo.set_filename(lastEntry);

        auto ret1 = storage_->ReplaceFileAndRecycleOldFile(oldFileInfo,
                                                        newFileInfo,
                                                        existNewFileInfo,
                                                        recycleFileInfo);
        if (ret1 != StoreStatus::OK) {
            LOG(ERROR) << "storage_ ReplaceFileAndRecycleOldFile error"
                        << ", oldFileName = " << oldFileName
                        << ", newFileName = " << newFileName
                        << ", ret = " << ret1;

            return StatusCode::kStorageError;
        }
        return StatusCode::kOK;
    } else if (ret3 == StatusCode::kFileNotExists) {
        // newFileName does not exist, rename directly
        FileInfo newFileInfo;
        newFileInfo.CopyFrom(oldFileInfo);
        newFileInfo.set_parentid(parentFileInfo.id());
        newFileInfo.set_filename(lastEntry);

        auto ret = storage_->RenameFile(oldFileInfo, newFileInfo);
        if ( ret != StoreStatus::OK ) {
            LOG(ERROR) << "storage_ renamefile error, error = " << ret;
            return StatusCode::kStorageError;
        }
        return StatusCode::kOK;
    } else {
        LOG(INFO) << "dest file LookUpFile return: " << ret3;
        return ret3;
    }
}

StatusCode CurveFS::ExtendFile(const std::string &filename,
                               uint64_t newLength) {
    FileInfo  fileInfo;
    auto ret = GetFileInfo(filename, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return  ret;
    }

    if (fileInfo.filetype() != FileType::INODE_PAGEFILE) {
        LOG(INFO) << "Only INODE_PAGEFILE support extent";
        return StatusCode::kNotSupported;
    }

    if (newLength > maxFileLength_) {
        LOG(ERROR) << "ExtendFile newLength > maxFileLength, fileName = "
                       << filename << ", newLength = " << newLength
                       << ", maxFileLength = " << maxFileLength_;
            return StatusCode::kFileLengthNotSupported;
    }

    if (newLength < fileInfo.length()) {
        LOG(INFO) << "newLength = " << newLength
                  << ", smaller than file length " << fileInfo.length();
        return StatusCode::kShrinkBiggerFile;
    } else if (newLength == fileInfo.length()) {
        LOG(INFO) << "newLength equals file length" << newLength;
        return StatusCode::kOK;
    } else {
        uint64_t deltaLength = newLength - fileInfo.length();
        if (fileInfo.segmentsize() == 0) {
            LOG(ERROR) << "segmentsize = 0 , filename = " << filename;
            return StatusCode::KInternalError;
        }
        if (deltaLength % fileInfo.segmentsize()  != 0) {
            LOG(INFO) << "extent unit error, mini extentsize = "
                      << fileInfo.segmentsize();
            return   StatusCode::kExtentUnitError;
        }
        fileInfo.set_length(newLength);
        return PutFile(fileInfo);
    }
}

StatusCode CurveFS::ChangeOwner(const std::string &filename,
                                const std::string &newOwner) {
    FileInfo  fileInfo;
    StatusCode ret = GetFileInfo(filename, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return  ret;
    }

    if (newOwner == fileInfo.owner()) {
        LOG(INFO) << "change owner sucess, file owner is same with newOwner, "
                  << "filename = " << filename
                  << ", file.owner() = " << fileInfo.owner()
                  << ", newOwner = " << newOwner;
        return StatusCode::kOK;
    }

    // check whether change owner is supported. Only INODE_PAGEFILE
    // and INODE_DIRECTORY are supported
    if (fileInfo.filetype() == FileType::INODE_PAGEFILE) {
        // determine whether the owner of the file can be changed (whether
        // the file is being used, during snapshot or being cloned)
        ret = CheckFileCanChange(filename, fileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "cannot changeOwner file"
                        << ", filename = " << filename
                        << ", ret = " << ret;
            return ret;
        }
    } else if (fileInfo.filetype() == FileType::INODE_DIRECTORY) {
        // if there are files in the directory, can not change owner
        bool isEmpty = false;
        ret = isDirectoryEmpty(fileInfo, &isEmpty);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "check is directory empty fail, filename = "
                       << filename << ", ret = " << ret;
            return ret;
        }
        if (!isEmpty) {
            LOG(WARNING) << "ChangeOwner fail, file is directory and not empty"
                       << ", filename = " << filename;
            return StatusCode::kDirNotEmpty;
        }
    } else {
        LOG(ERROR) << "file type not support change owner"
                  << ", filename = " << filename;
        return StatusCode::kNotSupported;
    }

    // change owner!
    fileInfo.set_owner(newOwner);
    return PutFile(fileInfo);
}

StatusCode CurveFS::GetOrAllocateSegment(const std::string & filename,
        offset_t offset, bool allocateIfNoExist,
        PageFileSegment *segment) {
    assert(segment != nullptr);
    FileInfo  fileInfo;
    StatusCode ret = GetFileInfo(filename, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return  ret;
    }

    if (fileInfo.filetype() != FileType::INODE_PAGEFILE &&
        fileInfo.filetype() != FileType::INODE_CLONE_PAGEFILE) {
        LOG(INFO) << "not pageFile, can't do this";
        return StatusCode::kParaError;
    }

    if (offset % fileInfo.segmentsize() != 0) {
        LOG(INFO) << "offset not align with segment";
        return StatusCode::kParaError;
    }

    if (offset + fileInfo.segmentsize() > fileInfo.length()) {
        LOG(INFO) << "bigger than file length, first extentFile";
        return StatusCode::kParaError;
    }

    auto storeRet = storage_->GetSegment(fileInfo.id(), offset, segment);
    if (storeRet == StoreStatus::OK) {
        return StatusCode::kOK;
    } else if (storeRet == StoreStatus::KeyNotExist) {
        if (allocateIfNoExist == false) {
            LOG(INFO) << "file = " << filename <<", segment offset = " << offset
                      << ", not allocated";
            return  StatusCode::kSegmentNotAllocated;
        } else {
            // TODO(hzsunjianliang): check the user and define the logical pool
            auto ifok = chunkSegAllocator_->AllocateChunkSegment(
                    fileInfo.filetype(), fileInfo.segmentsize(),
                    fileInfo.chunksize(),
                    fileInfo.has_poolset() ? fileInfo.poolset()
                                           : kDefaultPoolsetName,
                    offset, segment);
            if (ifok == false) {
                LOG(ERROR) << "AllocateChunkSegment error";
                return StatusCode::kSegmentAllocateError;
            }
            int64_t revision;
            if (storage_->PutSegment(fileInfo.id(), offset, segment, &revision)
                != StoreStatus::OK) {
                LOG(ERROR) << "PutSegment fail, fileInfo.id() = "
                           << fileInfo.id()
                           << ", offset = "
                           << offset;
                return StatusCode::kStorageError;
            }
            allocStatistic_->AllocSpace(segment->logicalpoolid(),
                    segment->segmentsize(),
                    revision);
            LOG(INFO) << "alloc segment success, fileInfo.id() = "
                      << fileInfo.id()
                      << ", offset = " << offset;
            return StatusCode::kOK;
        }
    }  else {
        return StatusCode::KInternalError;
    }
}

StatusCode CurveFS::CreateSnapShotFile(const std::string &fileName,
                                    FileInfo *snapshotFileInfo) {
    FileInfo  parentFileInfo;
    std::string lastEntry;

    auto ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", Walk Path error";
        return ret;
    }
    if (lastEntry.empty()) {
        return StatusCode::kFileNotExists;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", LookUpFile error";
        return ret;
    }

    // check file type
    if ((fileInfo.filetype() != FileType::INODE_PAGEFILE) &&
        (fileInfo.filetype() != FileType::INODE_CLONE_PAGEFILE)) {
        return StatusCode::kNotSupported;
    }

    // compatibility check, when the client version does not exist or is lower
    // than 0.0.6, snapshots are not allowed
    ret = IsSnapshotAllowed(fileName);
    if (ret != kOK) {
        return ret;
    }

    // check whether snapshot exist
    std::vector<FileInfo> snapShotFiles;
    if (storage_->ListSnapshotFile(fileInfo.id(),
                  fileInfo.id() + 1, &snapShotFiles) != StoreStatus::OK ) {
        LOG(ERROR) << fileName  << "listFile fail";
        return StatusCode::kStorageError;
    }
    if (snapShotFiles.size() != 0) {
        LOG(INFO) << fileName << " exist snapshotfile, num = "
            << snapShotFiles.size()
            << ", snapShotFiles[0].seqNum = " << snapShotFiles[0].seqnum();
        *snapshotFileInfo = snapShotFiles[0];
        return StatusCode::kFileUnderSnapShot;
    }

    // TTODO(hzsunjianliang): check if fileis open and session not expire
    // then invalide client

    // do snapshot
    // first new snapshot fileinfo, based on the original fileInfo
    InodeID inodeID;
    if (InodeIDGenerator_->GenInodeID(&inodeID) != true) {
        LOG(ERROR) << fileName << ", createSnapShotFile GenInodeID error";
        return StatusCode::kStorageError;
    }
    *snapshotFileInfo = fileInfo;
    snapshotFileInfo->set_filetype(FileType::INODE_SNAPSHOT_PAGEFILE);
    snapshotFileInfo->set_id(inodeID);
    snapshotFileInfo->set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
    snapshotFileInfo->set_parentid(fileInfo.id());
    snapshotFileInfo->set_filename(MakeSnapshotName(fileInfo.filename(),
        fileInfo.seqnum()));
    snapshotFileInfo->set_filestatus(FileStatus::kFileCreated);

    // add original file snapshot seq number
    fileInfo.set_seqnum(fileInfo.seqnum() + 1);

    // do storage
    ret = SnapShotFile(&fileInfo, snapshotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", SnapShotFile error";
        return StatusCode::kStorageError;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::CreateSnapShotFile2(const std::string &fileName,
                                    FileInfo *snapshotFileInfo) {
    FileInfo  parentFileInfo;
    std::string lastEntry;

    auto ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", Walk Path error";
        return ret;
    }
    if (lastEntry.empty()) {
        return StatusCode::kFileNotExists;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", LookUpFile error";
        return ret;
    }

    // check file type
    if ((fileInfo.filetype() != FileType::INODE_PAGEFILE) &&
        (fileInfo.filetype() != FileType::INODE_CLONE_PAGEFILE)) {
        return StatusCode::kNotSupported;
    }

    // compatibility check, when the client version does not exist or is lower
    // than 0.0.6, snapshots are not allowed
    ret = IsSnapshotAllowed(fileName);
    if (ret != kOK) {
        return ret;
    }

    // TTODO(hzsunjianliang): check if fileis open and session not expire
    // then invalide client

    // do snapshot
    // first new snapshot fileinfo, based on the original fileInfo
    InodeID inodeID;
    if (InodeIDGenerator_->GenInodeID(&inodeID) != true) {
        LOG(ERROR) << fileName << ", createSnapShotFile GenInodeID error";
        return StatusCode::kStorageError;
    }
    *snapshotFileInfo = fileInfo;
    snapshotFileInfo->set_filetype(FileType::INODE_SNAPSHOT_PAGEFILE);
    snapshotFileInfo->set_id(inodeID);
    snapshotFileInfo->set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
    snapshotFileInfo->set_parentid(fileInfo.id());
    snapshotFileInfo->set_filename(fileInfo.filename() + "-" +
            std::to_string(fileInfo.seqnum()));
    snapshotFileInfo->set_filestatus(FileStatus::kFileCreated);

    // save snapshot seq number to fileInfo,
    // in order to let client know the latest snaps through RefreshSession rpc
    fileInfo.add_snaps(fileInfo.seqnum());

    // add original file snapshot seq number
    fileInfo.set_seqnum(fileInfo.seqnum() + 1);

    // do storage
    ret = SnapShotFile(&fileInfo, snapshotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", SnapShotFile error";
        return StatusCode::kStorageError;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::ListSnapShotFile(const std::string & fileName,
                            std::vector<FileInfo> *snapshotFileInfos, FileInfo *srcfileInfo) const {
    FileInfo parentFileInfo;
    std::string lastEntry;

    auto ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", Walk Path error";
        return ret;
    }
    if (lastEntry.empty()) {
        LOG(INFO) << fileName << ", dir not suppport SnapShot";
        return StatusCode::kNotSupported;
    }

    FileInfo tmpfileInfo;
    FileInfo* fileInfo = srcfileInfo != nullptr ? srcfileInfo : &tmpfileInfo;
    
    ret = LookUpFile(parentFileInfo, lastEntry, fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", LookUpFile error";
        return ret;
    }

    // check file type
    if ((fileInfo->filetype() != FileType::INODE_PAGEFILE) &&
        (fileInfo->filetype() != FileType::INODE_CLONE_PAGEFILE)) {
        LOG(INFO) << fileName << ", filetype not support SnapShot";
        return StatusCode::kNotSupported;
    }

    // list snapshot files
    auto storeStatus =  storage_->ListSnapshotFile(fileInfo->id(),
                                           fileInfo->id() + 1,
                                           snapshotFileInfos);
    if (storeStatus == StoreStatus::KeyNotExist ||
        storeStatus == StoreStatus::OK) {
        return StatusCode::kOK;
    } else {
        LOG(ERROR) << fileName << ", storage ListFile return = " << storeStatus;
        return StatusCode::kStorageError;
    }
}

StatusCode CurveFS::GetSnapShotFileInfo(const std::string &fileName,
                        FileSeqType seq, FileInfo *snapshotFileInfo, FileInfo *fileInfo) const {
    std::vector<FileInfo> snapShotFileInfos;
    StatusCode ret =  ListSnapShotFile(fileName, &snapShotFileInfos, fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "ListSnapShotFile error";
        return ret;
    }

    if (snapShotFileInfos.size() == 0) {
        LOG(INFO) << "file not under snapshot";
        return StatusCode::kSnapshotFileNotExists;
    }

    unsigned int index;
    for ( index = 0; index != snapShotFileInfos.size(); index++ ) {
        if (snapShotFileInfos[index].seqnum() == static_cast<uint64_t>(seq)) {
          break;
        }
    }
    if (index == snapShotFileInfos.size()) {
        LOG(INFO) << fileName << " snapshotFile seq = " << seq << " not find";
        return StatusCode::kSnapshotFileNotExists;
    }

    *snapshotFileInfo = snapShotFileInfos[index];
    return StatusCode::kOK;
}

StatusCode CurveFS::DeleteFileSnapShotFile(const std::string &fileName,
                        FileSeqType seq,
                        std::shared_ptr<AsyncDeleteSnapShotEntity> entity) {
    FileInfo snapShotFileInfo;
    StatusCode ret =  GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "fileName = " << fileName
            << ", seq = "<< seq
            << ", GetSnapShotFileInfo file ,ret = " << ret;
        return ret;
    } else {
        LOG(INFO) << "snapshotfile info = " << snapShotFileInfo.filename();
    }

    if (snapShotFileInfo.filestatus() == FileStatus::kFileDeleting) {
        LOG(INFO) << "fileName = " << fileName
        << ", seq = " << seq
        << ", snapshot is under deleting";
        return StatusCode::kSnapshotDeleting;
    }

    if (snapShotFileInfo.filestatus() != FileStatus::kFileCreated) {
        LOG(ERROR) << "fileName = " << fileName
        << ", seq = " << seq
        << ", status error, status = " << snapShotFileInfo.filestatus();
        return StatusCode::KInternalError;
    }

    snapShotFileInfo.set_filestatus(FileStatus::kFileDeleting);
    ret = PutFile(snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "fileName = " << fileName
            << ", seq = " << seq
            << ", PutFile error = " << ret;
        return StatusCode::KInternalError;
    }

    //  message the snapshot delete manager
    if (!cleanManager_->SubmitDeleteSnapShotFileJob(
                        snapShotFileInfo, entity)) {
        LOG(ERROR) << "fileName = " << fileName
                << ", seq = " << seq
                << ", Delete Task Deduplicated";
        return StatusCode::KInternalError;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::DeleteFileSnapShotFile2(const std::string &fileName,
                        FileSeqType seq,
                        std::shared_ptr<AsyncDeleteSnapShotEntity> entity) {
    FileInfo snapShotFileInfo, fileInfo;
    StatusCode ret =  GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "fileName = " << fileName
            << ", seq = "<< seq
            << ", GetSnapShotFileInfo file ,ret = " << ret;
        return ret;
    }

    if (snapShotFileInfo.filestatus() == FileStatus::kFileDeleting) {
        LOG(INFO) << "fileName = " << fileName
        << ", seq = " << seq
        << ", snapshot is under deleting";
        return StatusCode::kSnapshotDeleting;
    }

    if (snapShotFileInfo.filestatus() != FileStatus::kFileCreated) {
        LOG(ERROR) << "fileName = " << fileName
        << ", seq = " << seq
        << ", status error, status = " << snapShotFileInfo.filestatus();
        return StatusCode::KInternalError;
    }

    snapShotFileInfo.set_filestatus(FileStatus::kFileDeleting);
    // remove snapshot seq number from fileInfo,
    // in order to let client know the latest snaps through RefreshSession rpc
    auto snaps = fileInfo.mutable_snaps();
    auto findSnap = std::find(snaps->begin(), snaps->end(), seq);
    if (findSnap == snaps->end()) {
        LOG(WARNING) << "fileName = " << fileName
                     << ", seq = " << seq 
                     << ", not found in fileInfo!";
    } else {
        snaps->erase(findSnap);
    }
    // // set current existed snapshot seqs to the snapshot fileinfo, 
    // // thus no need to get these snaps info from original fileinfo
    // for (auto i = 0; i < fileInfo.snaps_size(); i++) {
    //     snapShotFileInfo.add_snaps(fileInfo.snaps(i));
    // }

    // do storage
    ret = SnapShotFile(&fileInfo, &snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "fileName = " << fileName
            << ", seq = " << seq
            << ", SnapShotFile error = " << ret;
        LOG(ERROR) << fileName << ", SnapShotFile error";
        return StatusCode::kStorageError;
    }
    LOG(INFO) << "DeleteFileSnapShotFile2 update snap and file to storage success"
              << ", snapInfo: \n" << snapShotFileInfo.DebugString();

    //  message the snapshot delete manager
    if (!cleanManager_->SubmitDeleteBatchSnapShotFileJob(
                        snapShotFileInfo, entity)) {
        LOG(ERROR) << "fileName = " << fileName
                << ", seq = " << seq
                << ", Delete Task Deduplicated";
        return StatusCode::KInternalError;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::CheckSnapShotFileStatus(const std::string &fileName,
                                    FileSeqType seq, FileStatus * status,
                                    uint32_t * progress) const {
    FileInfo snapShotFileInfo;
    StatusCode ret =  GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(WARNING) << "GetSnapShotFileInfo file fail, fileName = "
                   << fileName << ", seq = " << seq << ", ret = " << ret;
        return ret;
    }

    *status = snapShotFileInfo.filestatus();
    if (snapShotFileInfo.filestatus() == FileStatus::kFileDeleting) {
        TaskIDType taskID = static_cast<TaskIDType>(snapShotFileInfo.id());
        auto task = cleanManager_->GetTask(taskID);
        if (task == nullptr) {
            // GetSnapShotFileInfo again
            StatusCode ret2 =
                GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
            // if not exist, means delete succeed.
            if (StatusCode::kSnapshotFileNotExists == ret2) {
                *progress = 100;
                return StatusCode::kSnapshotFileNotExists;
            // else the snapshotFile still exist,
            // means delete failed and retry times exceed.
            } else {
                *progress = 0;
                LOG(ERROR) << "snapshot file delete fail, fileName = "
                           << fileName << ", seq = " << seq;
                return StatusCode::kSnapshotFileDeleteError;
            }
        }

        TaskStatus taskStatus = task->GetTaskProgress().GetStatus();
        switch (taskStatus) {
            case TaskStatus::PROGRESSING:
            case TaskStatus::FAILED:  // FAILED task will retry
                *progress = task->GetTaskProgress().GetProgress();
                break;
            case TaskStatus::SUCCESS:
                *progress = 100;
                break;
        }
    } else {
        // means delete haven't begin.
        *progress = 0;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::CheckSnapShotFileStatus2(const std::string &fileName,
                                    FileSeqType seq, FileStatus * status,
                                    uint32_t * progress) const {
    FileInfo snapShotFileInfo;
    StatusCode ret =  GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(WARNING) << "GetSnapShotFileInfo file fail, fileName = "
                   << fileName << ", seq = " << seq << ", ret = " << ret;
        return ret;
    }

    *status = snapShotFileInfo.filestatus();
    if (snapShotFileInfo.filestatus() == FileStatus::kFileDeleting) {
        TaskIDType fileID = static_cast<TaskIDType>(snapShotFileInfo.parentid());
        TaskIDType snapSn = static_cast<TaskIDType>(snapShotFileInfo.seqnum());
        auto task = cleanManager_->GetTask(fileID, snapSn);
        if (task == nullptr) {
            // GetSnapShotFileInfo again
            StatusCode ret2 =
                GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
            // if not exist, means delete succeed.
            if (StatusCode::kSnapshotFileNotExists == ret2) {
                *progress = 100;
                return StatusCode::kSnapshotFileNotExists;
            // else the snapshotFile still exist,
            // means delete failed and retry times exceed.
            } else {
                *progress = 0;
                LOG(ERROR) << "snapshot file delete fail, fileName = "
                           << fileName << ", seq = " << seq;
                return StatusCode::kSnapshotFileDeleteError;
            }
        }

        TaskStatus taskStatus = task->GetTaskProgress().GetStatus();
        switch (taskStatus) {
            case TaskStatus::PROGRESSING:
            case TaskStatus::FAILED:  // FAILED task will retry
                *progress = task->GetTaskProgress().GetProgress();
                break;
            case TaskStatus::SUCCESS:
                *progress = 100;
                break;
        }
    } else {
        // means delete haven't begin.
        *progress = 0;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::GetSnapShotFileSegment(
        const std::string & fileName,
        FileSeqType seq,
        offset_t offset,
        PageFileSegment *segment) {
    assert(segment != nullptr);

    FileInfo snapShotFileInfo;
    StatusCode ret = GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(WARNING) << "GetSnapShotFileInfo file fail, fileName = "
                   << fileName << ", seq = " << seq << ", ret = " << ret;
        return ret;
    }

    if (offset % snapShotFileInfo.segmentsize() != 0) {
        LOG(WARNING) << "offset not align with segment, fileName = "
                   << fileName << ", seq = " << seq;
        return StatusCode::kParaError;
    }

    if (offset + snapShotFileInfo.segmentsize() > snapShotFileInfo.length()) {
        LOG(WARNING) << "bigger than file length, fileName = "
                   << fileName << ", seq = " << seq;
        return StatusCode::kParaError;
    }

    FileInfo fileInfo;
    auto ret1 = GetFileInfo(fileName, &fileInfo);
    if (ret1 != StatusCode::kOK) {
        LOG(ERROR) << "get origin file error, fileName = "
                   << fileName << ", errCode = " << ret1;
        return  ret1;
    }

    if (offset % fileInfo.segmentsize() != 0) {
        LOG(WARNING) << "origin file offset not align with segment, fileName = "
                   << fileName << ", offset = " << offset
                   << ", file segmentsize = " << fileInfo.segmentsize();
        return StatusCode::kParaError;
    }

    if (offset + fileInfo.segmentsize() > fileInfo.length()) {
        LOG(WARNING) << "bigger than origin file length, fileName = "
                   << fileName << ", offset = " << offset
                   << ", file segmentsize = " << fileInfo.segmentsize()
                   << ", file length = " << fileInfo.length();
        return StatusCode::kParaError;
    }

    StoreStatus storeRet = storage_->GetSegment(fileInfo.id(), offset, segment);
    if (storeRet == StoreStatus::OK) {
        return StatusCode::kOK;
    } else if (storeRet == StoreStatus::KeyNotExist) {
        LOG(INFO) << "get segment fail, kSegmentNotAllocated, fileName = "
                  << fileName
                  << ", fileInfo.id() = "
                  << fileInfo.id()
                  << ", offset = " << offset;
        return StatusCode::kSegmentNotAllocated;
    } else {
        LOG(ERROR) << "get segment fail, KInternalError, ret = " << storeRet
                  << ", fileInfo.id() = "
                  << fileInfo.id()
                  << ", offset = " << offset;
        return StatusCode::KInternalError;
    }
}

StatusCode CurveFS::Clone(const std::string &fileName,
        const std::string& owner,
        const std::string &srcFileName,
        FileSeqType seq,
        FileInfo *fileInfo) {
    // check the existence of the file
    FileInfo parentFileInfo;
    std::string lastEntry;
    auto ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        LOG(ERROR) << "WalkPath failed, ret: " << ret
                   << ", path: " << fileName;
        return ret;
    }
    if (lastEntry.empty()) {
        return StatusCode::kCloneFileNameIllegal;
    }

    ret = LookUpFile(parentFileInfo, lastEntry, fileInfo);
    if (ret == StatusCode::kOK) {
        LOG(ERROR) << "Clone failed, dstfile exist.";
        return StatusCode::kFileExists;
    }
    if (ret != StatusCode::kFileNotExists) {
        LOG(ERROR) << "LookUpFile failed, ret: " << ret;
        return ret;
    } 

    // check the existence of the snap file
    FileInfo srcParentFileInfo;
    std::string srcLastEntry;
    ret = WalkPath(srcFileName, &srcParentFileInfo, &srcLastEntry);
    if (ret != StatusCode::kOK ) {
        LOG(ERROR) << "WalkPath failed, ret: " << ret
                   << ", path: " << srcFileName;
        return ret;
    }
    if (srcLastEntry.empty()) {
        LOG(ERROR) << "Clone failed, srcfile not exist.";
        return StatusCode::kSnapshotFileNotExists;
    }
    FileInfo srcFileInfo;
    ret = LookUpFile(srcParentFileInfo, srcLastEntry, &srcFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "LookUpFile srcfile: " << srcLastEntry
                   << ", failed, ret: " << ret;
        return ret;
    }
    FileInfo snapFileInfo;
    ret = LookUpSnapFile(srcFileInfo, 
        MakeSnapshotName(srcLastEntry, seq), &snapFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "LookUpSnapFile srcfile: " 
                   << MakeSnapshotName(srcLastEntry, seq)
                   << ", failed, ret: " << ret;
        return ret;
    }

    // start clone
    InodeID inodeID;
    if (InodeIDGenerator_->GenInodeID(&inodeID) != true) {
        LOG(ERROR) << "Clone fileName = " << fileName
            << ", GenInodeID error";
        return StatusCode::kStorageError;
    }

    fileInfo->set_id(inodeID);
    fileInfo->set_filename(lastEntry);
    fileInfo->set_parentid(parentFileInfo.id());
    fileInfo->set_filetype(FileType::INODE_CLONE_PAGEFILE);
    fileInfo->set_owner(owner);
    fileInfo->set_chunksize(srcFileInfo.chunksize());
    uint64_t segmentSize = srcFileInfo.segmentsize();
    fileInfo->set_segmentsize(srcFileInfo.segmentsize());
    fileInfo->set_length(srcFileInfo.length());
    fileInfo->set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
    fileInfo->set_seqnum(kStartSeqNum);
    fileInfo->set_filestatus(FileStatus::kFileCreated);
    fileInfo->set_stripeunit(srcFileInfo.stripeunit());
    fileInfo->set_stripecount(srcFileInfo.stripecount());

    fileInfo->set_clonesource(srcFileName);
    uint64_t cloneLength = snapFileInfo.length();
    // use length when doing snapshot
    fileInfo->set_clonelength(cloneLength);

    fileInfo->set_initclonesegment(false);

    // 先持久化一次，万一clone中断，可依据此Fileinfo做清理
    // TODO(xuchaojie): 克隆失败时需清理segment
    ret = PutFile(*fileInfo);
    if (ret != StatusCode::kOK) {
        return ret;
    }

    // clone segment
    LOG(INFO) << "Clone Segment length: " << cloneLength;
    for (uint64_t offset = 0; offset < cloneLength; offset += segmentSize) {
        PageFileSegment segment;
        PageFileSegment srcSegment;
        auto storeRet = storage_->GetSegment(
            srcFileInfo.id(), offset, &srcSegment);
        if (storeRet == StoreStatus::OK) {
            auto ifok = chunkSegAllocator_->CloneChunkSegment(
                    srcFileName, srcFileInfo.id(), srcSegment, &segment);
            if (ifok == false) {
                LOG(ERROR) << "CloneChunkSegment error";
                return StatusCode::kSegmentAllocateError;
            }
            int64_t revision;
            if (storage_->PutSegment(fileInfo->id(), offset, &segment, &revision)
                != StoreStatus::OK) {
                LOG(ERROR) << "PutSegment fail, fileInfo.id() = "
                           << fileInfo->id()
                           << ", offset = "
                           << offset;
                return StatusCode::kStorageError;
            }
            allocStatistic_->AllocSpace(segment.logicalpoolid(),
                    segment.segmentsize(),
                    revision);
            LOG(INFO) << "clone segment success, fileInfo.id() = "
                      << fileInfo->id()
                      << ", offset = " << offset;
        } else if (storeRet == StoreStatus::KeyNotExist) {
            LOG(INFO) << "clone segment source not exist"
                      << ", use normal allocate logical.";
            // not exist, use normal allocate logical
        } else {
            LOG(ERROR) << "Clone Get srcSegment error";
            return StatusCode::KInternalError;
        }
    }

    // allocate cloneNo
    uint64_t cloneNo;
    if (cloneIdGenerator_->GenCloneID(&cloneNo) != true) {
        LOG(ERROR) << "Clone fileName = " << fileName
            << ", GenCloneNo error";
        return StatusCode::kStorageError;
    }
    fileInfo->set_cloneno(cloneNo);
    fileInfo->set_clonesn(seq);
    fileInfo->set_initclonesegment(true);

    ret = PutFile(*fileInfo);
    if (ret != StatusCode::kOK) {
        return ret;
    }

    // lookup clone chain
    FileInfo_CloneInfos *cinfo = fileInfo->add_clones();
    // is clone file & not flattend
    if ((FileType::INODE_CLONE_PAGEFILE == srcFileInfo.filetype()) && 
           (srcFileInfo.has_clonesource())) {
        cinfo->set_cloneno(srcFileInfo.cloneno());
    } else {
        cinfo->set_cloneno(0);
    }
    cinfo->set_clonesn(seq);
    ret = LookUpCloneChain(srcFileInfo, fileInfo);
    return ret;
}

StatusCode CurveFS::LookUpCloneChain(
    const FileInfo &srcFileInfo, FileInfo *fileInfo) const {
    // is clone file & not flattend
    FileInfo tmpInfo = srcFileInfo;
    while ((FileType::INODE_CLONE_PAGEFILE == tmpInfo.filetype()) && 
           (tmpInfo.has_clonesource())) {
        // lookup parent
        FileInfo parentFileInfo;
        std::string lastEntry;
        auto ret = WalkPath(
            tmpInfo.clonesource(), &parentFileInfo, &lastEntry);
        if ( ret != StatusCode::kOK ) {
            LOG(ERROR) << "WalkPath failed, ret: " << ret
                       << ", path: " << tmpInfo.clonesource();
            return ret;
        }
        if (lastEntry.empty()) {
            LOG(ERROR) << "LookUpCloneChain found CloneFileNameIllegal"
                       << ", clonesource: " << tmpInfo.clonesource();
            return StatusCode::kCloneFileNameIllegal;
        }
        FileInfo cloneSourceFileInfo;
        ret = LookUpFile(parentFileInfo, lastEntry, &cloneSourceFileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "LookUpCloneChain lookup clonesource error: " << ret;
            return ret;
        }
        
        FileInfo_CloneInfos *cinfo = fileInfo->add_clones();
        if ((FileType::INODE_CLONE_PAGEFILE == cloneSourceFileInfo.filetype()) && 
               (cloneSourceFileInfo.has_clonesource())) {
            cinfo->set_cloneno(cloneSourceFileInfo.cloneno());
        } else {
            cinfo->set_cloneno(0);
        }
        if (!tmpInfo.has_clonesn()) {
            LOG(ERROR) << "LookUpCloneChain found tmpInfo is a clone file "
                       << "but do not have clonesn.";
            return StatusCode::KInternalError;
        }
        cinfo->set_clonesn(tmpInfo.clonesn());

        LOG(INFO) << "LookUpCloneChain one step success"
                  << ", clonesource: " << tmpInfo.clonesource()
                  << ", cloneno: " << cinfo->cloneno()
                  << ", clonesn: " << cinfo->clonesn();
        tmpInfo = cloneSourceFileInfo;
    };
    return StatusCode::kOK;
}

// flatten
StatusCode CurveFS::Flatten(const std::string &fileName,
        const std::string &owner) {
    // check the existence of the file
    FileInfo fileInfo;
    StatusCode ret = GetFileInfoWithCloneChain(fileName, &fileInfo);
    if (ret == StatusCode::kFileNotExists) {
        LOG(WARNING) << "Flatten file not exist, fileName = " << fileName
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    } else if (ret != StatusCode::kOK) {
        LOG(ERROR) << "Flatten get file info error, fileName = " << fileName
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    }
    
    // check the owner of the file
    if (fileInfo.owner() != owner) {
        LOG(ERROR) << "Flatten owner not match, fileName = " << fileName
                   << ", owner = " << owner
                   << ", fileInfo.owner() = " << fileInfo.owner();
        return StatusCode::kOwnerAuthFail;
    }

    // check file type
    if (fileInfo.filetype() != FileType::INODE_CLONE_PAGEFILE) {
        if (fileInfo.filetype() == FileType::INODE_PAGEFILE) {
            LOG(WARNING) << "Flatten file already flattened, "
                << " fileName = " << fileName
                << ", fileType = " << fileInfo.filetype();
            return StatusCode::kOK;
        }
        LOG(ERROR) << "Flatten file type not match, "
            << " fileName = " << fileName
            << ", fileType = " << fileInfo.filetype();
        return StatusCode::kFileTypeInvalid;
    }
    // check file status
    if (fileInfo.filestatus() != FileStatus::kFileCreated) {
        if (fileInfo.filestatus() == FileStatus::kFileFlattening) {
            auto task = flattenManager_->GetFlattenTask(fileInfo.id()); 
            // not exist, means the task has been finished or resume failed
            if (task == nullptr) {
                // get file info again
                ret = GetFileInfo(fileName, &fileInfo);
                if (ret != StatusCode::kOK) {
                    LOG(ERROR) << "Flatten get file info error"
                               << ", fileName = " << fileName
                               << ", errCode = " << ret
                               << ", errName = " << StatusCode_Name(ret);
                    return ret;
                }
                // check file type
                if (fileInfo.filetype() == FileType::INODE_CLONE_PAGEFILE) {
                    // contnue to resume flatten job
                } else {
                    if (fileInfo.filetype() == FileType::INODE_PAGEFILE) {
                        LOG(WARNING) << "Flatten file already flattened, "
                            << " fileName = " << fileName
                            << ", fileType = " << fileInfo.filetype();
                        return StatusCode::kOK;
                    }
                    LOG(ERROR) << "Flatten file type not match, "
                        << " fileName = " << fileName
                        << ", fileType = " << fileInfo.filetype();
                    return StatusCode::kFileTypeInvalid;
                }
            } else {
                LOG(INFO) << "Flatten file already flattening, "
                    << " fileName = " << fileName
                    << ", fileType = " << fileInfo.filetype();
                // task exist, means the task is running
                return StatusCode::kOK;
            }
        } else {
            LOG(ERROR) << "Flatten file status invalid, fileName = " << fileName
                       << ", fileStatus = " << fileInfo.filestatus();
            return StatusCode::kFileStatusInvalid;
        }
    }

    // set file status to flattening
    fileInfo.set_filestatus(FileStatus::kFileFlattening);
    // set filename for resume flatten job
    fileInfo.set_originalfullpathname(fileName);
    ret = PutFile(fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "Flatten put file error, fileName = " << fileName
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    }

    // submit to FlattenManager
    bool success =  flattenManager_->SubmitFlattenJob(
        fileInfo.id(), fileName, fileInfo);
    if (!success) {
        LOG(ERROR) << "Flatten submit flatten job failed, fileName = "
                   << fileName
                   << ", fileId = " << fileInfo.id();
        return StatusCode::KInternalError;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::QueryFlattenStatus(const std::string &fileName,
        const std::string& owner,
        FileStatus* status,
        uint32_t* progress) {
    // check the existence of the file
    FileInfo fileInfo;
    StatusCode ret;
    ret = GetFileInfo(fileName, &fileInfo);
    if (ret == StatusCode::kFileNotExists) {
        LOG(WARNING) << "QueryFlattenStatus file not exist, fileName = "
                   << fileName
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    } else if (ret != StatusCode::kOK) {
        LOG(ERROR) << "QueryFlattenStatus get file info error, fileName = "
                   << fileName
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    }

    // check the owner of the file
    if (fileInfo.owner() != owner) {
        LOG(ERROR) << "QueryFlattenStatus owner not match, fileName = "
                   << fileName
                   << ", owner = " << owner
                   << ", fileInfo.owner() = " << fileInfo.owner();
        return StatusCode::kOwnerAuthFail;
    }

    *status = fileInfo.filestatus();
    *progress = 0;

    // check file type
    if (fileInfo.filetype() != FileType::INODE_CLONE_PAGEFILE) {
        if (fileInfo.filetype() == FileType::INODE_PAGEFILE) {
            *progress = 100;
            return StatusCode::kOK;
        }  else {
            LOG(ERROR) << "QueryFlattenStatus file type not match"
                << ", fileName = " << fileName
                << ", fileType = " << fileInfo.filetype();
            return StatusCode::kFileTypeInvalid;
        }
    }

    // check file status
    if (fileInfo.filestatus() != FileStatus::kFileFlattening) {
        if (fileInfo.filestatus() == FileStatus::kFileCreated) {
            LOG(INFO) << "QueryFlattenStatus file status have not begin "
                      << "flatten, fileName = "
                      << fileName
                      << ", fileStatus = " << fileInfo.filestatus();
            return StatusCode::kOK;
        } else if (fileInfo.filestatus() == FileStatus::kFileDeleting) {
            LOG(INFO) << "QueryFlattenStatus file status is deleting"
                      << ", fileName = " << fileName
                      << ", fileStatus = " << fileInfo.filestatus();
            return StatusCode::kFileUnderDeleting;
        } else {
            LOG(ERROR) << "QueryFlattenStatus file status invalid, fileName = "
                       << fileName
                       << ", fileStatus = " << fileInfo.filestatus();
            return StatusCode::kFileStatusInvalid;
        }
    }

    // get flatten progress
    auto task = flattenManager_->GetFlattenTask(fileInfo.id()); 
    // not exist, means the task has been finished
    if (task == nullptr) {
        // get file info again
        ret = GetFileInfo(fileName, &fileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "QueryFlattenStatus get file info error, fileName = "
                       << fileName
                       << ", errCode = " << ret
                       << ", errName = " << StatusCode_Name(ret);
            return ret;
        }
        // check file type
        if (fileInfo.filetype() == FileType::INODE_PAGEFILE) {
            *status = fileInfo.filestatus();
            *progress = 100;
            return StatusCode::kOK;
        }  else {
            LOG(ERROR) << "QueryFlattenStatus file type not match"
                << ", fileName = " << fileName
                << ", fileType = " << fileInfo.filetype();
            return StatusCode::kFileTypeInvalid;
        }
    }

    *progress = task->GetProgress();
    return StatusCode::kOK;
}

StatusCode CurveFS::OpenFile(const std::string &fileName,
                             const std::string &clientIP,
                             ProtoSession *protoSession,
                             FileInfo  *fileInfo,
                             CloneSourceSegment* cloneSourceSegment) {
    // check the existence of the file
    StatusCode ret;
    ret = GetFileInfo(fileName, fileInfo);
    if (ret == StatusCode::kFileNotExists) {
        LOG(WARNING) << "OpenFile file not exist, fileName = " << fileName
                   << ", clientIP = " << clientIP
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    } else if (ret != StatusCode::kOK) {
        LOG(ERROR) << "OpenFile get file info error, fileName = " << fileName
                   << ", clientIP = " << clientIP
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    }

    if (fileInfo->filetype() != FileType::INODE_PAGEFILE &&
        fileInfo->filetype() != FileType::INODE_CLONE_PAGEFILE) {
        LOG(ERROR) << "OpenFile file type not support, fileName = " << fileName
                   << ", clientIP = " << clientIP
                   << ", filetype = " << fileInfo->filetype();
        return StatusCode::kNotSupported;
    }

    fileRecordManager_->GetRecordParam(protoSession);

    // clone file
    if (fileInfo->filetype() == FileType::INODE_CLONE_PAGEFILE) {
        ret = LookUpCloneChain(*fileInfo, fileInfo);
    } else {
        if (fileInfo->has_clonesource() && isPathValid(fileInfo->clonesource())) {
            ret = ListCloneSourceFileSegments(fileInfo, cloneSourceSegment);
        }
    }

    if (!fileInfo->blocksize()) {
        fileInfo->set_blocksize(g_block_size);
    }

    LOG(INFO) << "Open FileInfo, " << fileInfo->ShortDebugString();
    return ret;
}

StatusCode CurveFS::CloseFile(const std::string &fileName,
                              const std::string &sessionID,
                              const std::string &clientIP,
                              uint32_t clientPort) {
    // check the existence of the file
    FileInfo  fileInfo;
    StatusCode ret;
    ret = GetFileInfo(fileName, &fileInfo);
    if (ret == StatusCode::kFileNotExists) {
        LOG(WARNING) << "CloseFile file not exist, fileName = " << fileName
                   << ", sessionID = " << sessionID
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    } else if (ret != StatusCode::kOK) {
        LOG(ERROR) << "CloseFile get file info error, fileName = " << fileName
                   << ", sessionID = " << sessionID
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return  ret;
    }

    // remove file record
    fileRecordManager_->RemoveFileRecord(fileName, clientIP, clientPort);

    return StatusCode::kOK;
}

StatusCode CurveFS::RefreshSession(const std::string &fileName,
                            const std::string &sessionid,
                            const uint64_t date,
                            const std::string &signature,
                            const std::string &clientIP,
                            uint32_t clientPort,
                            const std::string &clientVersion,
                            FileInfo  *fileInfo) {
    // check the existence of the file
    StatusCode ret;
    ret = GetFileInfo(fileName, fileInfo);
    if (ret == StatusCode::kFileNotExists) {
        LOG(WARNING) << "RefreshSession file not exist, fileName = " << fileName
                   << fileName
                   << ", sessionid = " << sessionid
                   << ", date = " << date
                   << ", signature = " << signature
                   << ", clientIP = " << clientIP
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return  ret;
    } else if (ret != StatusCode::kOK) {
        LOG(ERROR) << "RefreshSession get file info error, fileName = "
                   << fileName
                   << ", sessionid = " << sessionid
                   << ", date = " << date
                   << ", signature = " << signature
                   << ", clientIP = " << clientIP
                   << ", clientPort = " << clientPort
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return  ret;
    }

    // update file records
    fileRecordManager_->UpdateFileRecord(fileName, clientVersion, clientIP,
                                         clientPort);

    return StatusCode::kOK;
}

StatusCode CurveFS::CreateCloneFile(const std::string &fileName,
                            const std::string& owner,
                            FileType filetype,
                            uint64_t length,
                            FileSeqType seq,
                            ChunkSizeType chunksize,
                            uint64_t stripeUnit,
                            uint64_t stripeCount,
                            std::string poolset,
                            FileInfo *retFileInfo,
                            const std::string & cloneSource,
                            uint64_t cloneLength) {
    // check basic params
    if (filetype != FileType::INODE_PAGEFILE) {
        LOG(WARNING) << "CreateCloneFile err, filename = " << fileName
                << ", filetype not support";
        return StatusCode::kParaError;
    }

    if  (length < minFileLength_ || seq < kStartSeqNum) {
        LOG(WARNING) << "CreateCloneFile err, filename = " << fileName
                    << "file Length < MinFileLength " << minFileLength_
                    << ", length = " << length;
        return StatusCode::kParaError;
    }

    auto ret = CheckStripeParam(defaultSegmentSize_, defaultChunkSize_,
                                stripeUnit, stripeCount);
    if (ret != StatusCode::kOK) {
        return ret;
    }

    ret = CheckOrAssignPoolset(fileName, &poolset);
    if (ret != StatusCode::kOK) {
        return ret;
    }

    // check the existence of the file
    FileInfo parentFileInfo;
    std::string lastEntry;
    ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        return ret;
    }
    if (lastEntry.empty()) {
        return StatusCode::kCloneFileNameIllegal;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret == StatusCode::kOK) {
        return StatusCode::kFileExists;
    }

    if (ret != StatusCode::kFileNotExists) {
         return ret;
    } else {
        InodeID inodeID;
        if (InodeIDGenerator_->GenInodeID(&inodeID) != true) {
            LOG(ERROR) << "CreateCloneFile filename = " << fileName
                << ", GenInodeID error";
            return StatusCode::kStorageError;
        }

        fileInfo.set_id(inodeID);
        fileInfo.set_parentid(parentFileInfo.id());

        fileInfo.set_filename(lastEntry);

        fileInfo.set_filetype(filetype);
        fileInfo.set_owner(owner);

        fileInfo.set_chunksize(chunksize);
        fileInfo.set_segmentsize(defaultSegmentSize_);
        fileInfo.set_length(length);
        fileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());

        fileInfo.set_seqnum(seq);
        fileInfo.set_clonesource(cloneSource);
        fileInfo.set_clonelength(cloneLength);

        fileInfo.set_filestatus(FileStatus::kFileCloning);
        fileInfo.set_stripeunit(stripeUnit);
        fileInfo.set_stripecount(stripeCount);
        fileInfo.set_poolset(poolset);

        ret = PutFile(fileInfo);
        if (ret == StatusCode::kOK && retFileInfo != nullptr) {
            *retFileInfo = fileInfo;
        }
        return ret;
    }
}


StatusCode CurveFS::SetCloneFileStatus(const std::string &filename,
                            uint64_t fileID,
                            FileStatus fileStatus) {
    std::string lastEntry;
    FileInfo parentFileInfo;
    auto ret = WalkPath(filename, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        if (ret == StatusCode::kNotDirectory) {
            return StatusCode::kFileNotExists;
        }
        return ret;
    } else {
        if (lastEntry.empty()) {
            return StatusCode::kCloneFileNameIllegal;
        }

        FileInfo fileInfo;
        StatusCode ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);

        if (ret != StatusCode::kOK) {
            return ret;
        } else {
            if (fileInfo.filetype() != FileType::INODE_PAGEFILE) {
                return StatusCode::kNotSupported;
            }
        }

        if (fileID !=  kUnitializedFileID && fileID != fileInfo.id()) {
            LOG(WARNING) << "SetCloneFileStatus, filename = " << filename
                << "fileID not Matched, src fileID = " << fileID
                << ", stored fileID = " << fileInfo.id();
            return StatusCode::kFileIdNotMatch;
        }

        switch (fileInfo.filestatus()) {
            case kFileCloning:
                if (fileStatus == kFileCloneMetaInstalled ||
                    fileStatus == kFileCloning) {
                    // noop
                } else {
                    return kCloneStatusNotMatch;
                }
                break;
            case kFileCloneMetaInstalled:
                if (fileStatus == kFileCloned ||
                    fileStatus == kFileCloneMetaInstalled ) {
                    // noop
                } else {
                    return kCloneStatusNotMatch;
                }
                break;
            case kFileCloned:
                if (fileStatus == kFileCloned ||
                    fileStatus == kFileBeingCloned) {
                    // noop
                } else {
                    return kCloneStatusNotMatch;
                }
                break;
            case kFileCreated:
                if (fileStatus != kFileCreated &&
                    fileStatus != kFileBeingCloned) {
                    return kCloneStatusNotMatch;
                }
                break;
            case kFileBeingCloned:
                if (fileStatus != kFileBeingCloned &&
                    fileStatus != kFileCreated &&
                    fileStatus != kFileCloned) {
                    return kCloneStatusNotMatch;
                }
                break;
            default:
                return kCloneStatusNotMatch;
        }

        fileInfo.set_filestatus(fileStatus);

        return PutFile(fileInfo);
    }
}

StatusCode CurveFS::CheckPathOwnerInternal(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              std::string *lastEntry,
                              uint64_t *parentID) {
    std::vector<std::string> paths;
    ::curve::common::SplitString(filename, "/", &paths);

    // owner verification not allowed for the root directory
    if ( paths.size() == 0 ) {
        return StatusCode::kOwnerAuthFail;
    }

    *lastEntry = paths.back();
    uint64_t tempParentID = rootFileInfo_.id();

    for (uint32_t i = 0; i < paths.size() - 1; i++) {
        FileInfo  fileInfo;
        auto ret = storage_->GetFile(tempParentID, paths[i], &fileInfo);

        if (ret ==  StoreStatus::OK) {
            if (fileInfo.filetype() !=  FileType::INODE_DIRECTORY) {
                LOG(INFO) << fileInfo.filename() << " is not an directory";
                return StatusCode::kNotDirectory;
            }

            if (fileInfo.owner() != owner) {
                LOG(ERROR) << fileInfo.filename() << " auth fail, owner = "
                           << owner;
                return StatusCode::kOwnerAuthFail;
            }
        } else if (ret == StoreStatus::KeyNotExist) {
            LOG(WARNING) << paths[i] << " not exist";
            return StatusCode::kFileNotExists;
        } else {
            LOG(ERROR) << "GetFile " << paths[i] << " error, errcode = " << ret;
            return StatusCode::kStorageError;
        }
        tempParentID =  fileInfo.id();
    }

    *parentID = tempParentID;
    return StatusCode::kOK;
}

StatusCode CurveFS::CheckDestinationOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date) {
    if (owner.empty()) {
        LOG(ERROR) << "file owner is empty, filename = " << filename
                   << ", owner = " << owner;
        return StatusCode::kOwnerAuthFail;
    }

    StatusCode ret = StatusCode::kOwnerAuthFail;

    if (!CheckDate(date)) {
        LOG(ERROR) << "check date fail, request is staled.";
        return ret;
    }

    // for root user, identity verification with signature is required
    // no more verification is required for root user
    if (owner == GetRootOwner()) {
        ret = CheckSignature(owner, signature, date)
              ? StatusCode::kOK : StatusCode::kOwnerAuthFail;
        LOG_IF(ERROR, ret == StatusCode::kOwnerAuthFail)
              << "check root owner fail, signature auth fail.";
        return ret;
    }

    std::string lastEntry;
    uint64_t parentID;
    // verify the owner of all levels of directories
    ret = CheckPathOwnerInternal(filename, owner, signature,
                                 &lastEntry, &parentID);

    if (ret != StatusCode::kOK) {
        return ret;
    }

    // if the file exists, verify the owner, if not, return kOK
    FileInfo  fileInfo;
    auto ret1 = storage_->GetFile(parentID, lastEntry, &fileInfo);

    if (ret1 == StoreStatus::OK) {
        if (fileInfo.owner() != owner) {
            return StatusCode::kOwnerAuthFail;
        }
        return StatusCode::kOK;
    } else if  (ret1 == StoreStatus::KeyNotExist) {
        return StatusCode::kOK;
    } else {
        return StatusCode::kStorageError;
    }
}

StatusCode CurveFS::CheckPathOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date) {
    if (owner.empty()) {
        LOG(ERROR) << "file owner is empty, filename = " << filename
                   << ", owner = " << owner;
        return StatusCode::kOwnerAuthFail;
    }

    StatusCode ret = StatusCode::kOwnerAuthFail;

    if (!CheckDate(date)) {
        LOG(ERROR) << "check date fail, request is staled.";
        return ret;
    }

    // for root user, identity verification with signature is required
    // no more verification is required for root user
    if (owner == GetRootOwner()) {
        ret = CheckSignature(owner, signature, date)
              ? StatusCode::kOK : StatusCode::kOwnerAuthFail;
        LOG_IF(ERROR, ret == StatusCode::kOwnerAuthFail)
              << "check root owner fail, signature auth fail.";
        return ret;
    }

    std::string lastEntry;
    uint64_t parentID;
    return CheckPathOwnerInternal(filename, owner, signature,
                                    &lastEntry, &parentID);
}

StatusCode CurveFS::CheckRootOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date) {
    if (owner.empty()) {
        LOG(ERROR) << "file owner is empty, filename = " << filename
                   << ", owner = " << owner;
        return StatusCode::kOwnerAuthFail;
    }

    StatusCode ret = StatusCode::kOwnerAuthFail;

    if (!CheckDate(date)) {
        LOG(ERROR) << "check date fail, request is staled.";
        return ret;
    }

    if (owner != GetRootOwner()) {
        LOG(ERROR) << "check root owner fail, owner is :" << owner;
        return ret;
    }

    // use signature to check root user identity
    ret = CheckSignature(owner, signature, date)
            ? StatusCode::kOK : StatusCode::kOwnerAuthFail;
    LOG_IF(ERROR, ret == StatusCode::kOwnerAuthFail)
            << "check root owner fail, signature auth fail.";
    return ret;
}

StatusCode CurveFS::CheckFileOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date) {
    if (owner.empty()) {
        LOG(ERROR) << "file owner is empty, filename = " << filename
                   << ", owner = " << owner;
        return StatusCode::kOwnerAuthFail;
    }

    StatusCode ret = StatusCode::kOwnerAuthFail;

    if (!CheckDate(date)) {
        LOG(ERROR) << "check date fail, request is staled.";
        return ret;
    }

    // for root user, identity verification with signature is required
    // no more verification is required for root user
    if (owner == GetRootOwner()) {
        ret = CheckSignature(owner, signature, date)
              ? StatusCode::kOK : StatusCode::kOwnerAuthFail;
        LOG_IF(ERROR, ret == StatusCode::kOwnerAuthFail)
              << "check root owner fail, signature auth fail.";
        return ret;
    }

    std::string lastEntry;
    uint64_t parentID;
    ret = CheckPathOwnerInternal(filename, owner, signature,
                                 &lastEntry, &parentID);

    if (ret != StatusCode::kOK) {
        return ret;
    }

    FileInfo  fileInfo;
    auto ret1 = storage_->GetFile(parentID, lastEntry, &fileInfo);

    if (ret1 == StoreStatus::OK) {
        if (fileInfo.owner() != owner) {
            return StatusCode::kOwnerAuthFail;
        }
        return StatusCode::kOK;
    } else if  (ret1 == StoreStatus::KeyNotExist) {
        return StatusCode::kFileNotExists;
    } else {
        return StatusCode::kStorageError;
    }
}

StatusCode CurveFS::CheckEpoch(const std::string &filename,
                               uint64_t epoch) {
    FileInfo fileInfo;
    auto ret = GetFileInfo(filename, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return  ret;
    }
    if (fileInfo.has_epoch() && fileInfo.epoch() > epoch) {
        LOG(ERROR) << "Check Epoch Failed, fileInfo.epoch: "
                   << fileInfo.epoch()
                   << ", epoch: " << epoch;
        return StatusCode::kEpochTooOld;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::CheckRecycleFileOwner(const std::string &filename,
                                          const std::string &owner,
                                          const std::string &signature,
                                          uint64_t date) {
    if (owner.empty()) {
        LOG(ERROR) << "file owner is empty, filename = " << filename
                   << ", owner = " << owner;
        return StatusCode::kOwnerAuthFail;
    }

    StatusCode ret = StatusCode::kOwnerAuthFail;

    if (!CheckDate(date)) {
        LOG(ERROR) << "check date fail, request is staled.";
        return ret;
    }

    if (owner == GetRootOwner()) {
        ret = CheckSignature(owner, signature, date)
              ? StatusCode::kOK : StatusCode::kOwnerAuthFail;
        LOG_IF(ERROR, ret == StatusCode::kOwnerAuthFail)
              << "check root owner fail, signature auth fail.";
        return ret;
    }

    std::vector<std::string> paths;
    ::curve::common::SplitString(filename, "/", &paths);
    if (paths.size() != 2 || paths[0] != RECYCLEBINDIRNAME) {
        return StatusCode::kOwnerAuthFail;
    }

    FileInfo  fileInfo;
    auto ret1 = storage_->GetFile(RECYCLEBININODEID, paths[1], &fileInfo);

    if (ret1 == StoreStatus::OK) {
        if (fileInfo.owner() != owner) {
            return StatusCode::kOwnerAuthFail;
        }
        return StatusCode::kOK;
    } else if  (ret1 == StoreStatus::KeyNotExist) {
        return StatusCode::kFileNotExists;
    } else {
        return StatusCode::kStorageError;
    }
}

// kStaledRequestTimeIntervalUs represents the expiration time of the request
// to prevent the request from being intercepted and played back
bool CurveFS::CheckDate(uint64_t date) {
    uint64_t current = TimeUtility::GetTimeofDayUs();

    // prevent time shift between machines
    uint64_t interval = (date > current) ? date - current : current - date;

    return interval < kStaledRequestTimeIntervalUs;
}

bool CurveFS::CheckSignature(const std::string& owner,
                             const std::string& signature,
                             uint64_t date) {
    std::string str2sig = Authenticator::GetString2Signature(date, owner);
    std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                rootAuthOptions_.rootPassword);
    return signature == sig;
}

StatusCode CurveFS::ListClient(bool listAllClient,
                               std::vector<ClientInfo>* clientInfos) {
    std::set<butil::EndPoint> allClients = fileRecordManager_->ListAllClient();

    for (const auto &c : allClients) {
        clientInfos->emplace_back(EndPointToClientInfo(c));
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::CheckHasCloneRely(const std::string & filename,
                                             const std::string &owner,
                                             bool *isHasCloneRely) {
    CloneRefStatus refStatus;
    std::vector<snapshotcloneclient::DestFileInfo> fileCheckList;
    StatusCode ret = snapshotCloneClient_->GetCloneRefStatus(filename,
                                            owner, &refStatus, &fileCheckList);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "delete file, check file clone ref fail,"
                    << "filename = " << filename
                    << ", ret = " << ret;
        return ret;
    }
    bool hasCloneRef = false;
    if (refStatus == CloneRefStatus::kHasRef) {
        hasCloneRef = true;
    } else if (refStatus == CloneRefStatus::kNoRef) {
        hasCloneRef = false;
    } else {
        int recordNum = fileCheckList.size();
        for (int i = 0; i < recordNum; i++) {
            FileInfo  destFileInfo;
            StatusCode ret2 = GetFileInfo(fileCheckList[i].filename,
                                            &destFileInfo);
            if (ret2 == StatusCode::kFileNotExists) {
                continue;
            } else if (ret2 == StatusCode::kOK) {
                if (destFileInfo.id() == fileCheckList[i].inodeid) {
                    hasCloneRef = true;
                    break;
                }
            } else {
                LOG(ERROR) << "CheckHasCloneRely, check clonefile exist fail"
                            << ", filename = " << filename
                            << ", clonefile = " << fileCheckList[i].filename
                            << ", ret = " << ret2;
                return ret2;
            }
        }
    }

    *isHasCloneRely = hasCloneRef;
    return StatusCode::kOK;
}

StatusCode CurveFS::ListCloneSourceFileSegments(
    const FileInfo* fileInfo, CloneSourceSegment* cloneSourceSegment) const {
    if (fileInfo->filestatus() != FileStatus::kFileCloneMetaInstalled) {
        LOG(INFO) << fileInfo->filename()
                  << " hash clone source, but file status is "
                  << FileStatus_Name(fileInfo->filestatus())
                  << ", return empty CloneSourceSegment";
        return StatusCode::kOK;
    }

    if (!cloneSourceSegment) {
        LOG(ERROR) << "OpenFile failed, file has clone source, but "
                      "cloneSourceSegments is nullptr, filename = "
                   << fileInfo->filename();
        return StatusCode::kParaError;
    }

    FileInfo cloneSourceFileInfo;
    StatusCode ret = GetFileInfo(fileInfo->clonesource(), &cloneSourceFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR)
            << "OpenFile failed, Get clone source file info failed, ret = "
            << StatusCode_Name(ret) << ", filename = " << fileInfo->filename()
            << ", clone source = " << fileInfo->clonesource()
            << ", file status = " << FileStatus_Name(fileInfo->filestatus());
        return ret;
    }

    std::vector<PageFileSegment> segments;
    StoreStatus status =
        storage_->ListSegment(cloneSourceFileInfo.id(), &segments);
    if (status != StoreStatus::OK) {
        LOG(ERROR) << "OpenFile failed, list clone source segment failed, "
                      "filename = "
                   << fileInfo->filename()
                   << ", source file name = " << fileInfo->clonesource()
                   << ", ret = " << status;
        return StatusCode::kStorageError;
    }

    cloneSourceSegment->set_segmentsize(fileInfo->segmentsize());

    if (segments.empty()) {
        LOG(WARNING) << "Clone source file has no segments, filename = "
                     << fileInfo->clonesource();
    } else {
        for (const auto& segment : segments) {
            cloneSourceSegment->add_allocatedsegmentoffset(
                segment.startoffset());
        }
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::FindFileMountPoint(const std::string& fileName,
                                       std::vector<ClientInfo>* clientInfos) {
    std::vector<butil::EndPoint> mps;
    auto res = fileRecordManager_->FindFileMountPoint(fileName, &mps);

    if (res) {
        clientInfos->reserve(mps.size());
        for (auto& mp : mps) {
            clientInfos->emplace_back(EndPointToClientInfo(mp));
        }
        return StatusCode::kOK;
    }

    return StatusCode::kFileNotExists;
}

StatusCode CurveFS::ListVolumesOnCopyset(
                        const std::vector<common::CopysetInfo>& copysets,
                        std::vector<std::string>* fileNames) {
    std::vector<FileInfo> files;
    StatusCode ret = ListAllFiles(ROOTINODEID, &files);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "List all files in root directory fail";
        return ret;
    }
    std::map<LogicalPoolIdType, std::set<CopySetIdType>> copysetMap;
    for (const auto& copyset : copysets) {
        copysetMap[copyset.logicalpoolid()].insert(copyset.copysetid());
    }
    for (const auto& file : files) {
        std::vector<PageFileSegment> segments;
        StoreStatus ret = storage_->ListSegment(file.id(), &segments);
        if (ret != StoreStatus::OK) {
            LOG(ERROR) << "List segments of " << file.filename() << " fail";
            return StatusCode::kStorageError;
        }
        bool found = false;
        for (const auto& segment : segments) {
            if (copysetMap.find(segment.logicalpoolid()) == copysetMap.end()) {
                continue;
            }
            for (int i = 0; i < segment.chunks_size(); i++) {
                auto copysetId = segment.chunks(i).copysetid();
                if (copysetMap[segment.logicalpoolid()].count(copysetId) != 0) {
                    fileNames->emplace_back(file.filename());
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }
        }
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::ListAllFiles(uint64_t inodeId,
                                 std::vector<FileInfo>* files) {
    std::vector<FileInfo> tempFiles;
    StoreStatus ret = storage_->ListFile(inodeId, inodeId + 1, &tempFiles);
    if (ret != StoreStatus::OK) {
        return StatusCode::kStorageError;
    }
    for (const auto& file : tempFiles) {
        if ((file.filetype() == FileType::INODE_PAGEFILE) ||
            (file.filetype() == FileType::INODE_CLONE_PAGEFILE)) {
            files->emplace_back(file);
        } else if (file.filetype() == FileType::INODE_DIRECTORY) {
            std::vector<FileInfo> tempFiles2;
            StatusCode ret = ListAllFiles(file.id(), &tempFiles2);
            if (ret == StatusCode::kOK) {
                files->insert(files->end(), tempFiles2.begin(),
                              tempFiles2.end());
            } else {
                LOG(ERROR) << "ListAllFiles in file " << inodeId << " fail";
                return ret;
            }
        }
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::BuildEpochMap(::google::protobuf::Map<
    ::google::protobuf::uint64, ::google::protobuf::uint64>  *epochMap) {
    epochMap->clear();
    std::vector<FileInfo> files;
    StatusCode ret = ListAllFiles(ROOTINODEID, &files);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "List all files in root directory fail";
        return ret;
    }
    for (const auto& file : files) {
        if (file.has_epoch()) {
            epochMap->insert({file.id(), file.epoch()});
        }
    }
    return ret;
}

uint64_t CurveFS::GetOpenFileNum() {
    if (fileRecordManager_ == nullptr) {
        return 0;
    }

    return fileRecordManager_->GetOpenFileNum();
}

uint64_t CurveFS::GetDefaultChunkSize() {
    return defaultChunkSize_;
}

uint64_t CurveFS::GetDefaultSegmentSize() {
    return defaultSegmentSize_;
}

uint64_t CurveFS::GetMinFileLength() {
    return minFileLength_;
}

uint64_t CurveFS::GetMaxFileLength() {
    return maxFileLength_;
}

uint32_t CurveFS::GetBlockSize() const {
    return g_block_size;
}

StatusCode CheckStripeParam(uint64_t segmentSize,
                            uint64_t chunkSize,
                            uint64_t stripeUnit,
                            uint64_t stripeCount) {
    if ((stripeUnit == 0) && (stripeCount == 0)) {
        return StatusCode::kOK;
    }

    if ((stripeUnit && !stripeCount) || (!stripeUnit && stripeCount)) {
        LOG(WARNING) << "stripe unit/count should specify both or neither, "
                        "stripe unit: "
                     << stripeUnit << ", stripe count: " << stripeCount;
        return StatusCode::kParaError;
    }

    if (stripeUnit > chunkSize) {
        LOG(WARNING) << "stripe unit cannot exceed chunk size, stripe unit: "
                     << stripeUnit << ", chunk size: " << chunkSize;
        return StatusCode::kParaError;
    }

    if ((chunkSize % stripeUnit != 0) || (chunkSize % stripeCount != 0)) {
        LOG(WARNING) << "stripe unit/count is not divisible by chunksize. "
                        "stripe unit: "
                     << stripeUnit << ", stripe count: " << stripeCount
                     << ", chunk size: " << chunkSize;
        return StatusCode::kParaError;
    }

    // chunkserver check req offset and len align as 4k,
    // such as ChunkServiceImpl::CheckRequestOffsetAndLength
    if (stripeUnit % 4096 != 0) {
        LOG(WARNING) << "stripe unit is not aligned as 4k, stripe unit:"
                     << stripeUnit;
        return StatusCode::kParaError;
    }

    // constrains stripe in one segment
    if (stripeCount > (segmentSize / chunkSize)) {
        LOG(WARNING) << "stripe count is too large, maximum is "
                     << (segmentSize / chunkSize) << ", current stripe count: "
                     << stripeCount;
        return StatusCode::kParaError;
    }

    if (stripeCount == 1 && stripeUnit != chunkSize) {
        LOG(WARNING) << "when stripe count is 1, stripe unit must equals to "
                        "chunk size, current stripe unit: "
                     << stripeUnit << ", chunk size: " << chunkSize;
        return StatusCode::kParaError;
    }

    return StatusCode::kOK;
}

bool CurveFS::ResumeFlattenJob() {
    // load task from storage
    std::vector<FileInfo> fileInfos;
    StoreStatus storageRet = storage_->LoadFileInfos(&fileInfos);
    if (storageRet != StoreStatus::OK) {
        LOG(ERROR) << "load fileInfos from storage failed, ret = " 
                   << storageRet;
        return false;
    }
    bool success = true;
    // submit task
    for (auto fileInfo : fileInfos) {
        if ((fileInfo.filetype() == FileType::INODE_CLONE_PAGEFILE) &&
            (fileInfo.filestatus() == FileStatus::kFileFlattening)) {
            std::string fileName = fileInfo.originalfullpathname();
            FileInfo fileInfoNew = fileInfo;
            StatusCode ret = LookUpCloneChain(fileInfo, &fileInfoNew);
            if (ret != StatusCode::kOK) {
                LOG(ERROR) << "Flatten look up clone chain error, "
                           << "fileName = " << fileName
                           << ", errCode = " << ret
                           << ", errName = " << StatusCode_Name(ret);
                success = false;
                continue;
            }
            bool success =  flattenManager_->SubmitFlattenJob(
                fileInfo.id(), fileName, 
                fileInfoNew);
            if (!success) {
                LOG(ERROR) << "Flatten submit flatten job failed, fileName = "
                           << fileName
                           << ", fileId = " << fileInfo.id();
                success = false;
                continue;
            }
        }
    }
    return success;
}

CurveFS &kCurveFS = CurveFS::GetInstance();

int ChunkServerRegistInfoBuilderImpl::BuildEpochMap(::google::protobuf::Map<
    ::google::protobuf::uint64,
    ::google::protobuf::uint64> *epochMap) {
    if (cfs_ == nullptr) {
        return -1;
    }
    StatusCode ret = cfs_->BuildEpochMap(epochMap);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "BuildEpochMap failed, statusCode: " << ret
                   << ", StatusCode_Name: " << StatusCode_Name(ret);
        return -1;
    }
    return 0;
}

uint64_t GetOpenFileNum(void *varg) {
    CurveFS *curveFs = reinterpret_cast<CurveFS *>(varg);
    return curveFs->GetOpenFileNum();
}

bvar::PassiveStatus<uint64_t> g_open_file_num_bvar(
                        CURVE_MDS_CURVEFS_METRIC_PREFIX, "open_file_num",
                        GetOpenFileNum, &kCurveFS);

std::string SelectPoolsetByRules(
        const std::string& filename,
        const std::map<std::string, std::string>& rules) {
    if (rules.empty()) {
        return kDefaultPoolsetName;
    }

    // using reverse order, so that we support subdir rules
    //
    // for example
    //   /A/    -> poolset1
    //   /A/B/  -> poolset2
    //
    // if filename is /A/B/C, then we select `poolset2`
    for (auto it = rules.rbegin(); it != rules.rend(); ++it) {
        if (absl::StartsWith(filename, it->first)) {
            return it->second;
        }
    }

    return kDefaultPoolsetName;
}

}   // namespace mds
}   // namespace curve
