/*
 * Project: curve
 * Created Date: Friday September 7th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/nameserver2/curvefs.h"
#include "src/common/string_util.h"
#include "src/common/encode.h"
#include "src/common/timeutility.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/common/mds_define.h"

using curve::common::TimeUtility;

namespace curve {
namespace mds {

bool CurveFS::Init(NameServerStorage* storage,
                InodeIDGenerator* InodeIDGenerator,
                ChunkSegmentAllocator* chunkSegAllocator,
                std::shared_ptr<CleanManagerInterface> cleanManager,
                SessionManager *sessionManager,
                const struct SessionOptions &sessionOptions,
                const struct RootAuthOption &authOptions) {
    storage_ = storage;
    InodeIDGenerator_ = InodeIDGenerator;
    chunkSegAllocator_ = chunkSegAllocator;
    cleanManager_ = cleanManager;
    sessionManager_ = sessionManager;
    rootAuthOptions_ = authOptions;

    InitRootFile();

    if (!sessionManager_->Init(sessionOptions)) {
        return false;
    }

    sessionManager_->Start();

    return true;
}

void CurveFS::Uninit() {
    sessionManager_->Stop();
    sessionManager_ = nullptr;
    chunkSegAllocator_ =  nullptr;
    InodeIDGenerator_ = nullptr;
    storage_ = nullptr;
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

StatusCode CurveFS::CreateFile(const std::string & fileName,
                               const std::string& owner,
                               FileType filetype, uint64_t length) {
    FileInfo parentFileInfo;
    std::string lastEntry;

    // check param
    if (filetype == FileType::INODE_PAGEFILE) {
        if  (length < kMiniFileLength) {
            LOG(ERROR) << "file Length < MinFileLength " << kMiniFileLength
                       << ", length = " << length;
            return StatusCode::kParaError;
        }
    }

    auto ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
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
        fileInfo.set_parentid(parentFileInfo.id());
        fileInfo.set_filetype(filetype);
        fileInfo.set_owner(owner);
        fileInfo.set_chunksize(DefaultChunkSize);
        fileInfo.set_segmentsize(DefaultSegmentSize);
        fileInfo.set_length(length);
        fileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
        fileInfo.set_fullpathname(fileName);
        fileInfo.set_seqnum(kStartSeqNum);
        fileInfo.set_filestatus(FileStatus::kFileCreated);

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

StatusCode CurveFS::MoveFileToRecycle(const FileInfo &fileInfo,
                                                FileInfo *waitDeleteFileInfo) {
    waitDeleteFileInfo->CopyFrom(fileInfo);
    waitDeleteFileInfo->set_filestatus(FileStatus::kFileDeleting);
    waitDeleteFileInfo->set_filetype(INODE_RECYCLE_PAGEFILE);

    auto ret = storage_->PutFile(*waitDeleteFileInfo);
    if (ret != StoreStatus::OK) {
        LOG(ERROR) << "move file to recycle, put recycle file fail, fileName = "
                   << waitDeleteFileInfo->fullpathname();
        return StatusCode::kStorageError;
    }

    ret = storage_->DeleteFile(fileInfo.parentid(), fileInfo.filename());
    if (ret != StoreStatus::OK) {
        LOG(ERROR) << "move file to recycle, delete origin file fail"
                   << ", fileName = " << waitDeleteFileInfo->fullpathname();
        storage_->DeleteRecycleFile(waitDeleteFileInfo->parentid(),
                                    waitDeleteFileInfo->filename());
        if (ret != StoreStatus::OK) {
            LOG(ERROR) << "move file to recycle, delete recycle file fail"
                       << ", fileName = " << waitDeleteFileInfo->fullpathname();
        }

        return StatusCode::kStorageError;
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
        LOG(ERROR) << "list file fail, dir name = " << fileInfo.fullpathname();
        return StatusCode::kStorageError;
    }

    if (fileInfoList.size() ==  0) {
        *result = true;
        return StatusCode::kOK;
    }

    *result = false;
    return StatusCode::kOK;
}

bool CurveFS::isFileHasValidSession(const std::string &fileName) {
    return sessionManager_->isFileHasValidSession(fileName);
}

StatusCode CurveFS::DeleteFile(const std::string & filename) {
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

    if (fileInfo.filetype() == FileType::INODE_DIRECTORY) {
        // 目录下如果有还有文件，则不能删除
        bool isEmpty = false;
        auto ret1 = isDirectoryEmpty(fileInfo, &isEmpty);
        if (ret1 != StatusCode::kOK) {
            LOG(ERROR) << "check is directory empty fail, filename = "
                       << filename << ", ret = " << ret1;
            return ret;
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
        // 检查文件是否有快照
        std::vector<FileInfo> snapshotFileInfos;
        auto ret = ListSnapShotFile(filename, &snapshotFileInfos);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "delete file, list snapshot file fail"
                       << ", filename = " << filename;
            return ret;
        }
        if (snapshotFileInfos.size() != 0) {
            LOG(WARNING) << "delete file, file is under snapshot, cannot delete"
                       << ", filename = " << filename;
            return StatusCode::kFileUnderSnapShot;
        }

        // TODO(hzchenwei7) :删除文件还需考虑克隆的情况

        // 检查文件是否有分配出去的可用session
        if (isFileHasValidSession(filename)) {
            LOG(WARNING) << "delete file, file has valid session, cannot delete"
                       << ", filename = " << filename;
            return StatusCode::kFileOccupied;
        }

        // 把文件移到回收站
        FileInfo toDelteFileInfo;
        ret = MoveFileToRecycle(fileInfo, &toDelteFileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "delete file, move file to recycle fail"
                       << ", filename = " << filename
                       << ", ret = " << ret;
            return StatusCode::KInternalError;
        }

        // 提交一个删除文件的任务
        if (!cleanManager_->SubmitDeleteCommonFileJob(toDelteFileInfo)) {
            LOG(ERROR) << "fileName = " << filename
                    << ", submit delete file job fail.";
            return StatusCode::KInternalError;
        }

        LOG(INFO) << "delete file success, file is pagefile"
                  << ", filename = " << filename;
        return StatusCode::kOK;
    } else {
        // 目前deletefile只支持INODE_DIRECTORY，INODE_PAGEFILE类型文件的删除；
        // INODE_SNAPSHOT_PAGEFILE不调用本接口删除；
        // 其他文件类型，系统暂时不支持。
        LOG(ERROR) << "delete file fail, file type not support delete"
                   << ", filename = " << filename
                   << ", fileType = " << fileInfo.filetype();
        return kNotSupported;
    }
}

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

StatusCode CurveFS::RenameFile(const std::string & oldFileName,
                               const std::string & newFileName) {
    if (!oldFileName.compare(newFileName)) {
        LOG(INFO) << "rename same name, oldFileName = " << oldFileName
                  << ", newFileName = " << newFileName;
        return StatusCode::kFileExists;
    }

    FileInfo  oldFileInfo;
    auto ret1 = GetFileInfo(oldFileName, &oldFileInfo);
    if (ret1 != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret1;
        return  ret1;
    }

    FileInfo parentFileInfo;
    std::string  lastEntry;
    auto ret2 = WalkPath(newFileName, &parentFileInfo, &lastEntry);
    if (ret2 != StatusCode::kOK) {
        LOG(INFO) << "dest middle dir not exist";
        return StatusCode::kFileNotExists;
    }

    FileInfo newFileInfo;
    auto ret3 = LookUpFile(parentFileInfo, lastEntry, &newFileInfo);
    if (ret3 != StatusCode::kFileNotExists) {
        if (StatusCode::kOK == ret3) {
            LOG(INFO) << "dest file LookUpFile file exist";
            return StatusCode::kFileExists;
        } else {
            LOG(INFO) << "dest file LookUpFile return: " << ret3;
            return ret3;
        }
    } else {
        newFileInfo.CopyFrom(oldFileInfo);
        newFileInfo.set_parentid(parentFileInfo.id());
        newFileInfo.set_filename(lastEntry);
        newFileInfo.set_owner(parentFileInfo.owner());
        newFileInfo.set_fullpathname(newFileName);

        auto ret = storage_->RenameFile(oldFileInfo, newFileInfo);
        if ( ret != StoreStatus::OK ) {
            LOG(ERROR) << "storage_ renamefile error, error = " << ret;
            return StatusCode::kStorageError;
        }
        return StatusCode::kOK;
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

StatusCode CurveFS::GetOrAllocateSegment(const std::string & filename,
        offset_t offset, bool allocateIfNoExist,
        PageFileSegment *segment) {
    assert(segment != nullptr);

    FileInfo  fileInfo;
    auto ret = GetFileInfo(filename, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return  ret;
    }

    if (fileInfo.filetype() != FileType::INODE_PAGEFILE) {
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
                            fileInfo.chunksize(), offset, segment);
            if (ifok == false) {
                LOG(ERROR) << "AllocateChunkSegment error";
                return StatusCode::kSegmentAllocateError;
            }
            if (storage_->PutSegment(fileInfo.id(), offset, segment)
                != StoreStatus::OK) {
                LOG(ERROR) << "PutSegment fail, fileInfo.id() = "
                           << fileInfo.id()
                           << ", offset = "
                           << offset;
                return StatusCode::kStorageError;
            }

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
    if (fileInfo.filetype() != FileType::INODE_PAGEFILE) {
        return StatusCode::kNotSupported;
    }

    // check  if snapshot exist
    std::vector<FileInfo> snapShotFiles;
    if (storage_->ListSnapshotFile(fileInfo.id(),
                  fileInfo.id() + 1, &snapShotFiles) != StoreStatus::OK ) {
        LOG(ERROR) << fileName  << "listFile fail";
        return StatusCode::kStorageError;
    }
    if (snapShotFiles.size() != 0) {
        LOG(INFO) << fileName << " exist snapshotfile, num = "
            << snapShotFiles.size();
        return StatusCode::kFileUnderSnapShot;
    }

    // TODO(hzsunjianliang): check if fileis open and session not expire
    // then invalide client

    // do snapshot
    // first new snapshot fileinfo, based on the original fileInfo
    InodeID inodeID;
    if (InodeIDGenerator_->GenInodeID(&inodeID) != true) {
        LOG(ERROR) << fileName << ", createSnapShotFile GenInodeID error";
        return StatusCode::kStorageError;
    }
    *snapshotFileInfo = fileInfo;
    snapshotFileInfo->set_id(inodeID);
    snapshotFileInfo->set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
    snapshotFileInfo->set_parentid(fileInfo.id());
    snapshotFileInfo->set_filename(fileInfo.filename() + "-" +
            std::to_string(fileInfo.seqnum()));
    snapshotFileInfo->set_fullpathname(fileName + "/" +
        snapshotFileInfo->filename());
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

StatusCode CurveFS::ListSnapShotFile(const std::string & fileName,
                            std::vector<FileInfo> *snapshotFileInfos) const {
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

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", LookUpFile error";
        return ret;
    }

    // check file type
    if (fileInfo.filetype() != FileType::INODE_PAGEFILE) {
        LOG(INFO) << fileName << ", filetype not support SnapShot";
        return StatusCode::kNotSupported;
    }

    // list snapshot files
    auto storeStatus =  storage_->ListSnapshotFile(fileInfo.id(),
                                           fileInfo.id() + 1,
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
                        FileSeqType seq, FileInfo *snapshotFileInfo) const {
    std::vector<FileInfo> snapShotFileInfos;
    StatusCode ret =  ListSnapShotFile(fileName, &snapShotFileInfos);
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

StatusCode CurveFS::CheckSnapShotFileStatus(const std::string &fileName,
                                    FileSeqType seq, FileStatus * status,
                                    uint32_t * progress) const {
    FileInfo snapShotFileInfo;
    StatusCode ret =  GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "GetSnapShotFileInfo file fail, fileName = "
                   << fileName << ", seq = " << seq << ", ret = " << ret;
        return ret;
    }

    *status = snapShotFileInfo.filestatus();
    if (snapShotFileInfo.filestatus() == FileStatus::kFileDeleting) {
        TaskIDType taskID = static_cast<TaskIDType>(snapShotFileInfo.id());
        auto task = cleanManager_->GetTask(taskID);
        if (task == nullptr) {
            *progress = 100;
            return StatusCode::kOK;
        }

        TaskStatus taskStatus = task->GetTaskProgress().GetStatus();
        switch (taskStatus) {
            case TaskStatus::PROGRESSING:
                *progress = task->GetTaskProgress().GetProgress();
                break;
            case TaskStatus::FAILED:
                *progress = 0;
                LOG(ERROR) << "snapshot file delete fail, fileName = "
                           << fileName << ", seq = " << seq;
                return StatusCode::kSnapshotFileDeleteError;
            case TaskStatus::SUCCESS:
                *progress = 100;
                break;
        }
    } else {
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
        LOG(ERROR) << "GetSnapShotFileInfo file fail, fileName = "
                   << fileName << ", seq = " << seq << ", ret = " << ret;
        return ret;
    }

    if (offset % snapShotFileInfo.segmentsize() != 0) {
        LOG(ERROR) << "offset not align with segment, fileName = "
                   << fileName << ", seq = " << seq;
        return StatusCode::kParaError;
    }

    if (offset + snapShotFileInfo.segmentsize() > snapShotFileInfo.length()) {
        LOG(ERROR) << "bigger than file length, fileName = "
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
        LOG(ERROR) << "origin file offset not align with segment, fileName = "
                   << fileName << ", offset = " << offset
                   << ", file segmentsize = " << fileInfo.segmentsize();
        return StatusCode::kParaError;
    }

    if (offset + fileInfo.segmentsize() > fileInfo.length()) {
        LOG(ERROR) << "bigger than origin file length, fileName = "
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

StatusCode CurveFS::OpenFile(const std::string &fileName,
                             const std::string &clientIP,
                             ProtoSession *protoSession,
                             FileInfo  *fileInfo) {
    // 检查文件是否存在
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

    ret = sessionManager_->InsertSession(fileName, clientIP, protoSession);
    if (ret == StatusCode::kFileOccupied) {
        LOG(WARNING) << "OpenFile file occupied, fileName = " << fileName
                   << ", clientIP = " << clientIP
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    } else if (ret != StatusCode::kOK) {
        LOG(ERROR) << "OpenFile insert session fail, fileName = " << fileName
                   << ", clientIP = " << clientIP
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::CloseFile(const std::string &fileName,
                              const std::string &sessionID) {
    // 检查文件是否存在
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

    ret = sessionManager_->DeleteSession(fileName, sessionID);
    if (ret == StatusCode::kSessionNotExist) {
        LOG(WARNING) << "CloseFile session not exit, fileName = " << fileName
                   << ", sessionID = " << sessionID
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    } else if (ret != StatusCode::kOK) {
        LOG(ERROR) << "CloseFile delete session fail, fileName = " << fileName
                   << ", sessionID = " << sessionID
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::RefreshSession(const std::string &fileName,
                            const std::string &sessionid,
                            const uint64_t date,
                            const std::string &signature,
                            const std::string &clientIP,
                            FileInfo  *fileInfo) {
    // 检查文件是否存在
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
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return  ret;
    }

    ret = sessionManager_->UpdateSession(fileName, sessionid,
                                            signature, clientIP);
    // 目前UpdateSession只有一种异常情况StatusCode::kSessionNotExist
    if (ret != StatusCode::kOK) {
        LOG(WARNING) << "RefreshSession update session fail, fileName = "
                   << fileName
                   << ", sessionid = " << sessionid
                   << ", date = " << date
                   << ", signature = " << signature
                   << ", clientIP = " << clientIP
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::CreateCloneFile(const std::string &fileName,
                            const std::string& owner,
                            FileType filetype,
                            uint64_t length,
                            FileSeqType seq,
                            ChunkSizeType chunksize,
                            FileInfo *retFileInfo) {
    // 检查基本参数
    if (filetype != FileType::INODE_PAGEFILE) {
        LOG(ERROR) << "CreateCloneFile err, filename = " << fileName
                << ", filetype not support";
        return StatusCode::kParaError;
    }

    if  (length < kMiniFileLength || seq < kStartSeqNum) {
        LOG(ERROR) << "CreateCloneFile err, filename = " << fileName
                    << "file Length < MinFileLength " << kMiniFileLength
                    << ", length = " << length;
        return StatusCode::kParaError;
    }

    // 检查文件是否存在
    FileInfo parentFileInfo;
    std::string lastEntry;
    auto ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
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
        fileInfo.set_fullpathname(fileName);

        fileInfo.set_filetype(filetype);
        fileInfo.set_owner(owner);

        fileInfo.set_chunksize(chunksize);
        fileInfo.set_segmentsize(DefaultSegmentSize);
        fileInfo.set_length(length);
        fileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());

        fileInfo.set_seqnum(seq);

        fileInfo.set_filestatus(FileStatus::kFileCloning);

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
            return StatusCode::kFileNotExists;
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
                if (fileStatus == kFileCloned) {
                    // noop
                } else {
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

    // 根目录不允许进行owner校验
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
            LOG(ERROR) << paths[i] << " not exist";
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

    // 如果是root用户，需要用signature校验root用户身份。
    // root用户身份通过signature校验之后，不需要进行后续校验。
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

    // 非root用户不允许rename文件到根目录下
    if (parentID == rootFileInfo_.id() && owner != GetRootOwner()) {
        return StatusCode::kOwnerAuthFail;
    }

    return StatusCode::kOK;
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

    // 如果是root用户，需要用signature校验root用户身份。
    // root用户身份通过signature校验之后，不需要进行后续校验。
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

    // 如果是root用户，需要用signature校验root用户身份。
    // root用户身份通过signature校验之后，不需要进行后续校验。
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

// kStaledRequestTimeIntervalUs表示request的过期时间，防止request被截取并回放
bool CurveFS::CheckDate(uint64_t date) {
    uint64_t current = TimeUtility::GetTimeofDayUs();

    // 防止机器间时间漂移
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

CurveFS &kCurveFS = CurveFS::GetInstance();

}   // namespace mds
}   // namespace curve

