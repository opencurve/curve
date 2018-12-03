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

namespace curve {
namespace mds {

void CurveFS::Init(NameServerStorage* storage,
                InodeIDGenerator* InodeIDGenerator,
                ChunkSegmentAllocator* chunkSegAllocator) {
    storage_ = storage;
    InodeIDGenerator_ = InodeIDGenerator;
    chunkSegAllocator_ = chunkSegAllocator;
    InitRootFile();
}

void CurveFS::InitRootFile(void) {
    rootFileInfo_.set_id(ROOTINODEID);
    rootFileInfo_.set_filename(ROOTFILENAME);
    rootFileInfo_.set_filetype(FileType::INODE_DIRECTORY);
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
        auto storeKey = EncodeFileStoreKey(parentID, paths[i]);
        auto ret = storage_->GetFile(storeKey, fileInfo);

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


    std::string storeKey = EncodeFileStoreKey(parentFileInfo.id(), fileName);

    auto ret = storage_->GetFile(storeKey, fileInfo);

    if (ret == StoreStatus::OK) {
        return StatusCode::kOK;
    } else if  (ret == StoreStatus::KeyNotExist) {
        return StatusCode::kFileNotExists;
    } else {
        return StatusCode::kStorageError;
    }
}


StatusCode CurveFS::PutFile(const FileInfo & fileInfo) {
    auto storeKey =
        EncodeFileStoreKey(fileInfo.parentid(), fileInfo.filename());
    if (storage_->PutFile(storeKey, fileInfo) != StoreStatus::OK) {
        return StatusCode::kStorageError;
    } else {
        return StatusCode::kOK;
    }
}

StatusCode CurveFS::CreateFile(const std::string & fileName, FileType filetype,
                               uint64_t length) {
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
        fileInfo.set_chunksize(DefaultChunkSize);
        fileInfo.set_segmentsize(DefaultSegmentSize);
        fileInfo.set_length(length);
        fileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
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
        return ret;
    }

    // TODO(hzsunjianliang): can not delete dir/snapshot father file

    auto storeKey = EncodeFileStoreKey(parentFileInfo.id(), lastEntry);
    if (storage_->DeleteFile(storeKey) != StoreStatus::OK) {
        return StatusCode::kStorageError;
    }
    return StatusCode::kOK;
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

    auto startKey  = EncodeFileStoreKey(fileInfo.id(), "");
    auto endKey    = EncodeFileStoreKey(fileInfo.id() + 1, "");

    if ( storage_->ListFile(startKey, endKey, files) != StoreStatus::OK ) {
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

        std::string oldStoreKey =
            EncodeFileStoreKey(oldFileInfo.parentid(), oldFileInfo.filename());
        std::string newStoreKey =
            EncodeFileStoreKey(newFileInfo.parentid(), newFileInfo.filename());

        auto ret = storage_->RenameFile(oldStoreKey, oldFileInfo,
                                        newStoreKey, newFileInfo);
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

    auto storeKey = EncodeSegmentStoreKey(fileInfo.id(), offset);
    auto storeRet = storage_->GetSegment(storeKey, segment);
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
            if (storage_->PutSegment(storeKey, segment) != StoreStatus::OK) {
                return StatusCode::kStorageError;
            }
            return StatusCode::kOK;
        }
    }  else {
        return StatusCode::KInternalError;
    }
}



StatusCode CurveFS::DeleteSegment(const std::string &filename,
                                  offset_t offset) {
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

    auto storeKey = EncodeSegmentStoreKey(fileInfo.id(), offset);
    PageFileSegment segment;
    auto storeRet = storage_->GetSegment(storeKey, &segment);
    if (storeRet == StoreStatus::KeyNotExist) {
        return StatusCode::kSegmentNotAllocated;
    } else if (storeRet == StoreStatus::OK) {
        if (segment.startoffset() != offset) {
            LOG(ERROR) << "get segment offset is not same, startoffset = "
                       << segment.startoffset();
            return StatusCode::KInternalError;
        }
        if (storage_->DeleteSegment(storeKey) != StoreStatus::OK) {
            return StatusCode::kStorageError;
        }
        return StatusCode::kOK;
    } else {
        return StatusCode::kStorageError;
    }
}

CurveFS &kCurveFS = CurveFS::GetInstance();

}   // namespace mds
}   // namespace curve

