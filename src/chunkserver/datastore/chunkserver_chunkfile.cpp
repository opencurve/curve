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
 * File Created: Thursday, 6th September 2018 10:49:53 am
 * Author: yangyaokai
 */
#include <fcntl.h>
#include <algorithm>
#include <memory>

#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/chunkserver_chunkfile.h"
#include "src/common/crc32.h"

namespace curve {
namespace chunkserver {

ChunkFileMetaPage::ChunkFileMetaPage(const ChunkFileMetaPage& metaPage) {
    version = metaPage.version;
    sn = metaPage.sn;
    correctedSn = metaPage.correctedSn;
    location = metaPage.location;
    if (metaPage.bitmap != nullptr) {
        bitmap = std::make_shared<Bitmap>(metaPage.bitmap->Size(),
                                          metaPage.bitmap->GetBitmap());
    } else {
        bitmap = nullptr;
    }

    cloneNo = metaPage.cloneNo;
    virtualId = metaPage.virtualId;
    fileId = metaPage.fileId;
}

ChunkFileMetaPage& ChunkFileMetaPage::operator =(
    const ChunkFileMetaPage& metaPage) {
    if (this == &metaPage)
        return *this;
    version = metaPage.version;
    sn = metaPage.sn;
    correctedSn = metaPage.correctedSn;
    location = metaPage.location;
    if (metaPage.bitmap != nullptr) {
        bitmap = std::make_shared<Bitmap>(metaPage.bitmap->Size(),
                                          metaPage.bitmap->GetBitmap());
    } else {
        bitmap = nullptr;
    }

    cloneNo = metaPage.cloneNo;
    virtualId = metaPage.virtualId;
    fileId = metaPage.fileId;
    return *this;
}

void ChunkFileMetaPage::encode(char* buf) {
    size_t len = 0;
    memcpy(buf, &version, sizeof(version));
    len += sizeof(version);
    memcpy(buf + len, &sn, sizeof(sn));
    len += sizeof(sn);
    memcpy(buf + len, &correctedSn, sizeof(correctedSn));
    len += sizeof(correctedSn);
    memcpy(buf + len, &cloneNo, sizeof(cloneNo));
    len += sizeof(cloneNo);
    memcpy(buf + len, &virtualId, sizeof(virtualId));
    len += sizeof(virtualId);
    memcpy(buf + len, &fileId, sizeof(fileId));
    len += sizeof(fileId);
    size_t loc_size = location.size();
    memcpy(buf + len, &loc_size, sizeof(loc_size));
    len += sizeof(loc_size);
    // CloneChunk need serialized location information and bitmap information
    if (loc_size > 0) {
        memcpy(buf + len, location.c_str(), loc_size);
        len += loc_size;
        uint32_t bits = bitmap->Size();
        memcpy(buf + len, &bits, sizeof(bits));
        len += sizeof(bits);
        size_t bitmapBytes = (bits + 8 - 1) >> 3;
        memcpy(buf + len, bitmap->GetBitmap(), bitmapBytes);
        len += bitmapBytes;
    } else if (cloneNo > 0) {
        int32_t bMark = 0;
        if (nullptr == bitmap) {
            bMark = 1; //means bitmap is full set
        } else {
            bMark = 0;
        }
        memcpy(buf + len, &bMark, sizeof(bMark));
        len += sizeof(bMark);

        if (bitmap != nullptr) {
            uint32_t bits = bitmap->Size();
            memcpy(buf + len, &bits, sizeof(bits));
            len += sizeof(bits);
            size_t bitmapBytes = (bits + 8 - 1) >> 3;
            memcpy(buf + len, bitmap->GetBitmap(), bitmapBytes);
            len += bitmapBytes;
        }
    }
    uint32_t crc = ::curve::common::CRC32(buf, len);
    memcpy(buf + len, &crc, sizeof(crc));
}

CSErrorCode ChunkFileMetaPage::decode(const char* buf) {
    size_t len = 0;
    memcpy(&version, buf, sizeof(version));
    len += sizeof(version);
    memcpy(&sn, buf + len, sizeof(sn));
    len += sizeof(sn);
    memcpy(&correctedSn, buf + len, sizeof(correctedSn));
    len += sizeof(correctedSn);
    memcpy(&cloneNo, buf + len, sizeof(cloneNo));
    len += sizeof(cloneNo);
    memcpy(&virtualId, buf + len, sizeof(virtualId));
    len += sizeof(virtualId);
    memcpy(&fileId, buf + len, sizeof(fileId));
    len += sizeof(fileId);
    size_t loc_size;
    memcpy(&loc_size, buf + len, sizeof(loc_size));
    len += sizeof(loc_size);
    if (loc_size > 0) {
        location = string(buf + len, loc_size);
        len += loc_size;
        uint32_t bits = 0;
        memcpy(&bits, buf + len, sizeof(bits));
        len += sizeof(bits);
        bitmap = std::make_shared<Bitmap>(bits, buf + len);
        size_t bitmapBytes = (bitmap->Size() + 8 - 1) >> 3;
        len += bitmapBytes;
    } else if (cloneNo > 0) {
        int32_t bMark = 0;
        memcpy(&bMark, buf + len, sizeof(bMark));
        len += sizeof(bMark);

        if (0 == bMark) {
            uint32_t bits = 0;
            memcpy(&bits, buf + len, sizeof(bits));
            len += sizeof(bits);
            if (nullptr != bitmap) {
                int lcount = bitmap->initialize((char*) (buf+len));
                assert(lcount == ((bits + 8 -1) >> 3)); //sanity check
                len += lcount;
            }  else {
                bitmap = std::make_shared<Bitmap>(bits, buf + len);
                size_t bitmapBytes = (bitmap->Size() + 8 - 1) >> 3;
                len += bitmapBytes;
            }
        } else {
            if (nullptr != bitmap) {
                bitmap = nullptr;
            }
        }
    }
    uint32_t crc =  ::curve::common::CRC32(buf, len);
    uint32_t recordCrc;
    memcpy(&recordCrc, buf + len, sizeof(recordCrc));
    // check crc
    if (crc != recordCrc) {
        LOG(ERROR) << "Checking Crc32 failed.";
        return CSErrorCode::CrcCheckError;
    }

    // TODO(yyk) check version compatibility, currrent simple error handing,
    // need detailed implementation later
    if (version != FORMAT_VERSION) {
        LOG(ERROR) << "File format version incompatible."
                    << "file version: "
                    << static_cast<uint32_t>(version)
                    << ", format version: "
                    << static_cast<uint32_t>(FORMAT_VERSION);
        return CSErrorCode::IncompatibleError;
    }
    return CSErrorCode::Success;
}

uint64_t CSChunkFile::syncChunkLimits_ = 2 * 1024 * 1024;
uint64_t CSChunkFile::syncThreshold_ = 64 * 1024;

CSChunkFile::CSChunkFile(std::shared_ptr<LocalFileSystem> lfs,
                         std::shared_ptr<FilePool> chunkFilePool,
                         const ChunkOptions& options)
    : cvar_(nullptr),
      chunkrate_(nullptr),
      fd_(-1),
      size_(options.chunkSize),
      blockSize_(options.blockSize),
      metaPageSize_(options.metaPageSize),
      blockSize_shift_(options.blockSize_shift),
      chunkId_(options.id),
      baseDir_(options.baseDir),
      isCloneChunk_(false),
      snapshots_(std::make_shared<CSSnapshots>(options.blockSize)),
      chunkFilePool_(chunkFilePool),
      lfs_(lfs),
      metric_(options.metric),
      enableOdsyncWhenOpenChunkFile_(options.enableOdsyncWhenOpenChunkFile) {
    CHECK(!baseDir_.empty()) << "Create chunk file failed";
    CHECK(lfs_ != nullptr) << "Create chunk file failed";
    metaPage_.sn = options.sn;
    metaPage_.correctedSn = options.correctedSn;
    metaPage_.location = options.location;
    metaPage_.virtualId = options.virtualId;
    metaPage_.cloneNo = options.cloneNo;
    metaPage_.fileId = options.fileId;

    //if blockSize_ = 0, use the default value 4096
    if (0 == blockSize_) {
        blockSize_ = 4096;
        blockSize_shift_ = 12;
        DVLOG(3) << "CSChunkFile() blockSize_ is 0, use the default value 4096";
    }

    //if blockSize_ = 0, use the default value 4096
    if (0 == blockSize_) {
        blockSize_ = 4096;
        DVLOG(3) << "CSChunkFile() blockSize_ is 0, use the default value 4096";
    }

    // If location is not empty, it is CloneChunk,
    //     and Bitmap needs to be initialized
    if ((!metaPage_.location.empty()) || (0 != metaPage_.cloneNo)) {
        uint32_t bits = size_ / blockSize_;
        metaPage_.bitmap = std::make_shared<Bitmap>(bits);
        isCloneChunk_ = true;

        if (metric_ != nullptr) {
            metric_->cloneChunkCount << 1;
        }
    }

    if (metric_ != nullptr) {
        metric_->chunkFileCount << 1;
    }
}

CSChunkFile::~CSChunkFile() {
    if (fd_ >= 0) {
        lfs_->Close(fd_);
    }

    if (metric_ != nullptr) {
        metric_->chunkFileCount << -1;
        if (isCloneChunk_) {
            metric_->cloneChunkCount << -1;
        }
    }
}

CSErrorCode CSChunkFile::Open(bool createFile) {
    WriteLockGuard writeGuard(rwLock_);
    string chunkFilePath = path();
    // Create a new file, if the chunk file already exists, no need to create
    // The existence of chunk files may be caused by two situations:
    // 1. getchunk succeeded, but failed in stat or load metapage last time;
    // 2. Two write requests concurrently create new chunk files
    if (createFile
        && !lfs_->FileExists(chunkFilePath)
        && metaPage_.sn > 0) {
        std::unique_ptr<char[]> buf(new char[metaPageSize_]);
        memset(buf.get(), 0, metaPageSize_);
        metaPage_.encode(buf.get());

        int rc = chunkFilePool_->GetFile(chunkFilePath, buf.get());
        // When creating files concurrently, the previous thread may have been
        // created successfully, then -EEXIST will be returned here. At this
        // point, you can continue to open the generated file
        // But the current operation of the same chunk is serial, this problem
        // will not occur
        if (rc != 0  && rc != -EEXIST) {
            LOG(ERROR) << "Error occured when create file."
                       << " filepath = " << chunkFilePath;
            return CSErrorCode::InternalError;
        }
    }
    int rc = -1;
    if (enableOdsyncWhenOpenChunkFile_) {
        rc = lfs_->Open(chunkFilePath, O_RDWR|O_NOATIME|O_DSYNC);
    } else {
        rc = lfs_->Open(chunkFilePath, O_RDWR|O_NOATIME);
    }
    if (rc < 0) {
        LOG(ERROR) << "Error occured when opening file."
                   << " filepath = " << chunkFilePath;
        return CSErrorCode::InternalError;
    }
    fd_ = rc;
    struct stat fileInfo;
    rc = lfs_->Fstat(fd_, &fileInfo);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when stating file."
                   << " filepath = " << chunkFilePath;
        return CSErrorCode::InternalError;
    }

    if (fileInfo.st_size != fileSize()) {
        LOG(ERROR) << "Wrong file size."
                   << " filepath = " << chunkFilePath
                   << ", real filesize = " << fileInfo.st_size
                   << ", expect filesize = " << fileSize();
        return CSErrorCode::FileFormatError;
    }

    CSErrorCode errCode = loadMetaPage();
    // After restarting, only after reopening and loading the metapage,
    // can we know whether it is a clone chunk
    // if the cloneNo is not 0, the set the isCloneChunk_
    if ((!metaPage_.location.empty() || (metaPage_.cloneNo > 0)) && !isCloneChunk_) {
        if (metric_ != nullptr) {
            metric_->cloneChunkCount << 1;
        }
        isCloneChunk_ = true;
    }

    if (true == isCloneChunk_) {
        if (nullptr == metaPage_.bitmap) {
            if (metric_ != nullptr) {
                metric_->cloneChunkCount << -1;
            }
            isCloneChunk_ = false;
        }
    }

    return errCode;
}

CSErrorCode CSChunkFile::LoadSnapshot(SequenceNum sn) {
    WriteLockGuard writeGuard(rwLock_);
    return loadSnapshot(sn);
}

CSErrorCode CSChunkFile::loadSnapshot(SequenceNum sn) {
    if (snapshots_->contains(sn)) {
        LOG(ERROR) << "Multiple snapshot file found with same SeqNum."
                   << " ChunkID: " << chunkId_
                   << " Snapshot sn: " << sn;
        return CSErrorCode::SnapshotConflictError;
    }
    ChunkOptions options;
    options.id = chunkId_;
    options.sn = sn;
    options.baseDir = baseDir_;
    options.chunkSize = size_;
    options.blockSize = blockSize_;
    options.metaPageSize = metaPageSize_;
    options.metric = metric_;
    options.cloneNo = metaPage_.cloneNo;
    CSSnapshot *snapshot_ = new(std::nothrow) CSSnapshot(lfs_,
                                            chunkFilePool_,
                                            options);
    CHECK(snapshot_ != nullptr) << "Failed to new CSSnapshot!"
                                << "ChunkID:" << chunkId_
                                << ",snapshot sn:" << sn;
    CSErrorCode errorCode = snapshot_->Open(false);
    if (errorCode != CSErrorCode::Success) {
        delete snapshot_;
        snapshot_ = nullptr;
        LOG(ERROR) << "Load snapshot failed."
                   << "ChunkID: " << chunkId_
                   << ",snapshot sn: " << sn;
        return errorCode;
    }
    snapshots_->insert(snapshot_);
    return errorCode;
}

CSErrorCode CSChunkFile::Write(SequenceNum sn,
                               const butil::IOBuf& buf,
                               off_t offset,
                               size_t length,
                               uint32_t* cost,
                               std::shared_ptr<SnapContext> ctx) {
    WriteLockGuard writeGuard(rwLock_);
    if (!CheckOffsetAndLength(offset, length)) {
        LOG(ERROR) << "Write chunk failed, invalid offset or length."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", page size: " << metaPageSize_
                   << ", chunk size: " << size_
                   << ", block size: " << blockSize_;
        return CSErrorCode::InvalidArgError;
    }
    // Curve will ensure that all previous requests arrive or time out
    // before issuing new requests after user initiate a snapshot request.
    // Therefore, this is only a log recovery request, and it must have been
    // executed, and an error code can be returned here.
    if (sn < metaPage_.sn || sn < metaPage_.correctedSn) {
        LOG(WARNING) << "Backward write request."
                     << "ChunkID: " << chunkId_
                     << ",request sn: " << sn
                     << ",chunk sn: " << metaPage_.sn
                     << ",correctedSn: " << metaPage_.correctedSn;
        return CSErrorCode::BackwardRequestError;
    }

    // Determine whether to create a snapshot file
    if (needCreateSnapshot(sn, ctx)) {
        CSErrorCode err = createSnapshot(ctx->getLatest());
        if (err != CSErrorCode::Success) {
            return err;
        }
    }
    // If the requested sequence number is greater than the current chunk
    // sequence number, the metapage needs to be updated
    if (sn > metaPage_.sn) {
        ChunkFileMetaPage tempMeta = metaPage_;
        tempMeta.sn = sn;
        CSErrorCode errorCode = updateMetaPage(&tempMeta);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Update metapage failed."
                       << "ChunkID: " << chunkId_
                       << ",request sn: " << sn
                       << ",chunk sn: " << metaPage_.sn;
            return errorCode;
        }
        metaPage_.sn = tempMeta.sn;
    }
    // If it is cow, copy the data to the snapshot file first
    if (needCow(sn, ctx)) {
        CSErrorCode errorCode = copy2Snapshot(offset, length);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Copy data to snapshot failed."
                        << "ChunkID: " << chunkId_
                        << ",request sn: " << sn
                        << ",chunk sn: " << metaPage_.sn;
            return errorCode;
        }
    }
    int rc = writeData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Write data to chunk file failed."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ",chunk sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    // If it is a clone chunk, the bitmap will be updated
    CSErrorCode errorCode = flush();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Write data to chunk file failed."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ",chunk sn: " << metaPage_.sn;
        return errorCode;
    }

    if (chunkrate_.get() && cvar_.get()) {
        *chunkrate_ += length;
        uint64_t res = *chunkrate_;
        // if single write size > syncThreshold, for cache friend to
        // delay to sync.
        auto actualSyncChunkLimits = MayUpdateWriteLimits(res);
        if (*chunkrate_ >= actualSyncChunkLimits &&
                chunkrate_->compare_exchange_weak(res, 0)) {
            cvar_->notify_one();
        }
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::writeDataDirect(const butil::IOBuf& buf, off_t offset, size_t length) {

    CSErrorCode errorCode = CSErrorCode::Success;
    int rc = writeData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Write data to chunk file failed."
                << "ChunkID: " << chunkId_
                << ",chunk sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }

    // If it is a clone chunk, the bitmap will be updated
    errorCode = flush();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Write data to chunk file failed."
                << "ChunkID: " << chunkId_
                << ",chunk sn: " << metaPage_.sn;
        return errorCode;
    }

    return errorCode;
}

CSErrorCode CSChunkFile::writeSnapData(SequenceNum sn,
                        const butil::IOBuf& buf,
                        off_t offset,
                        size_t length,
                        uint32_t* cost,
                        std::shared_ptr<SnapContext> ctx) {
    
    CSErrorCode errorCode = CSErrorCode::Success;
    // Determine whether to create a snapshot file
    if (needCreateSnapshot(sn, ctx)) {
        errorCode = createSnapshot(ctx->getLatest());
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        DVLOG(3) << "Create snapshotChunk success, "
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ",chunk sn: " << metaPage_.sn;
    }

    // If the requested sequence number is greater than the current chunk
    // sequence number, the metapage needs to be updated
    if (sn > metaPage_.sn) {
        ChunkFileMetaPage tempMeta = metaPage_;
        tempMeta.sn = sn;
        errorCode = updateMetaPage(&tempMeta);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Update metapage failed."
                       << "ChunkID: " << chunkId_
                       << ",request sn: " << sn
                       << ",chunk sn: " << metaPage_.sn;
            return errorCode;
        }
        metaPage_.sn = tempMeta.sn;
    }

    errorCode = copy2Snapshot(offset, length);
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Copy data to snapshot failed."
                    << "ChunkID: " << chunkId_
                    << ",request sn: " << sn
                    << ",chunk sn: " << metaPage_.sn;
        return errorCode;
    }

    int rc = writeData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Write data to chunk file failed."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ",chunk sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    // If it is a clone chunk, the bitmap will be updated
    errorCode = flush();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Write data to chunk file failed."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ",chunk sn: " << metaPage_.sn;
        return errorCode;
    }

    return errorCode;
}

int CSChunkFile::FindExtraReadFromParent(std::vector<File_ObjectInfoPtr>& objIns, CSChunkFilePtr& chunkFile, off_t offset, size_t length) {

    int buf_size = 0;

    for (auto& tmpo : objIns) {
        if (tmpo->fileptr != chunkFile) {//for any data that not in chunk itself
            for (auto iter = tmpo->obj_infos.begin(); iter != tmpo->obj_infos.end();) {
                struct ObjectInfo& tmpi = *iter;
                if (tmpi.offset < offset) {
                    if ((tmpi.offset + tmpi.length >= offset) && (tmpi.offset + tmpi.length <= offset + length)) {
                        tmpi.length = offset - tmpi.offset;
                        buf_size += tmpi.length;
                        iter++;
                    } else if ((tmpi.offset + tmpi.length >= offset) && (tmpi.offset + tmpi.length > offset + length)) {
                        int tlength = tmpi.length;
                        tmpi.length = offset - tmpi.offset;
                        buf_size += tmpi.length;

                        struct ObjectInfo tmpj = tmpi;
                        tmpj.offset = offset + length;
                        tmpj.length = tmpi.offset + tlength - tmpj.offset;
                        iter = tmpo->obj_infos.insert(iter + 1, tmpj);
                        buf_size += tmpj.length;
                        iter++;
                    } else {
                        buf_size += tmpi.length;
                        iter++;
                    }
                } else if (tmpi.offset == offset) {
                    if (tmpi.offset + tmpi.length <= offset + length) {
                        iter = tmpo->obj_infos.erase(iter);
                    } else {
                        int toff = tmpi.offset;
                        int tlen = tmpi.length;
                        tmpi.offset = offset + length;
                        tmpi.length = toff + tlen - tmpi.offset;
                        buf_size += tmpi.length;
                        iter++;
                    }
                } else {
                    if (tmpi.offset + tmpi.length <= offset + length) {
                        iter = tmpo->obj_infos.erase(iter);
                    } else if (tmpi.offset >= offset + length){
                        buf_size += tmpi.length;
                        iter++;
                    } else {
                        int toff = tmpi.offset;
                        int tlen = tmpi.length;
                        tmpi.offset = offset + length;
                        tmpi.length = toff + tlen - tmpi.offset;
                        buf_size += tmpi.length;
                        iter++;
                    }
                }
            }
        }
    }

    return buf_size;

}

void CSChunkFile::MergeObjectForRead(std::map<int32_t, Offset_InfoPtr>& objmap, 
                                     std::vector<File_ObjectInfoPtr>& objIns, 
                                     CSChunkFilePtr& chunkFile) {

    for (auto& tmpo : objIns) {
        if (tmpo->fileptr != chunkFile) {
            for (auto& tmpi : tmpo->obj_infos) {
                auto it_upper = objmap.upper_bound(tmpi.offset);
                auto it_lower = it_upper;
                if (it_lower != objmap.begin()) {
                    it_lower--;
                } else {
                    it_lower = objmap.end();
                }

                if (it_lower != objmap.end()) {//find smaller offset
                    if (it_lower->second->offset + it_lower->second->length == tmpi.offset) {
                        struct File_Object fobj(tmpo->fileptr, tmpi);

                        it_lower->second->length = tmpi.length + it_lower->second->length;
                        it_lower->second->objs.push_back(fobj);

                        if (it_upper != objmap.end()) {
                            if (it_lower->second->offset + it_lower->second->length == it_upper->second->offset) {
                                for (auto& tmp : it_upper->second->objs) {
                                    it_lower->second->objs.push_back(tmp);
                                }

                                it_lower->second->length = it_upper->second->length + it_lower->second->length;
                                objmap.erase(it_upper);                                    
                            }
                        }
                    } else {
                        if (it_upper != objmap.end()) {//find bigger offset
                            if (tmpi.offset + tmpi.length == it_upper->second->offset) {
                                Offset_InfoPtr tptr = std::move(it_upper->second);
                                struct File_Object fobj(tmpo->fileptr, tmpi);
                                tptr->objs.push_back(fobj);
                                tptr->offset = tmpi.offset;
                                tptr->length = tmpi.length + tptr->length;
                                objmap.erase(it_upper);
                                objmap.insert(std::pair<uint32_t, Offset_InfoPtr>(tmpi.offset, std::move(tptr)));                                    
                            } else {
                                struct File_Object fobj(tmpo->fileptr, tmpi);

                                Offset_InfoPtr infoptr(new Offset_Info());
                                infoptr->offset = tmpi.offset;
                                infoptr->length = tmpi.length;
                                infoptr->objs.push_back(fobj);
                                objmap.insert(std::pair<uint32_t, Offset_InfoPtr>(tmpi.offset, std::move(infoptr))); 
                            }
                        } else {
                            struct File_Object fobj(tmpo->fileptr, tmpi);

                            Offset_InfoPtr infoptr(new Offset_Info());
                            infoptr->offset = tmpi.offset;
                            infoptr->length = tmpi.length;
                            infoptr->objs.push_back(fobj);
                            objmap.insert(std::pair<uint32_t, Offset_InfoPtr>(tmpi.offset, std::move(infoptr))); 
                        }
                    }
                } else {
                    if (it_upper != objmap.end()) { //find bigger offset
                        if (tmpi.offset + tmpi.length == it_upper->second->offset) {//merge the objs
                            Offset_InfoPtr tptr = std::move(it_upper->second);
                            struct File_Object fobj(tmpo->fileptr, tmpi);
                            tptr->objs.push_back(fobj);
                            tptr->offset = tmpi.offset;
                            tptr->length = tmpi.length + tptr->length;
                            objmap.erase(it_upper);
                            objmap.insert(std::pair<uint32_t, Offset_InfoPtr>(tmpi.offset, std::move(tptr)));
                        } else {
                            struct File_Object fobj(tmpo->fileptr, tmpi);

                            Offset_InfoPtr infoptr(new Offset_Info());
                            infoptr->offset = tmpi.offset;
                            infoptr->length = tmpi.length;
                            infoptr->objs.push_back(fobj);
                            objmap.insert(std::pair<uint32_t, Offset_InfoPtr>(tmpi.offset, std::move(infoptr))); 
                        }
                    } else {
                        struct File_Object fobj(tmpo->fileptr, tmpi);

                        Offset_InfoPtr infoptr(new Offset_Info());
                        infoptr->offset = tmpi.offset;
                        infoptr->length = tmpi.length;
                        infoptr->objs.push_back(fobj);
                        objmap.insert(std::pair<uint32_t, Offset_InfoPtr>(tmpi.offset, std::move(infoptr))); 
                    }
                }
            }
        }
        
    }


    return;
}

CSErrorCode CSChunkFile::cloneWrite(SequenceNum sn,
                            const butil::IOBuf& buf,
                            off_t offset,
                            size_t length,
                            uint32_t* cost,
                            std::unique_ptr<CloneContext>& cloneCtx,
                            CSDataStore& datastore,
                            std::shared_ptr<SnapContext> ctx) {

    CSErrorCode errorCode = CSErrorCode::Success;
    WriteLockGuard writeGuard(rwLock_);
    
    if (!CheckOffsetAndLength(
            offset, length)) {
        LOG(ERROR) << "Write chunk failed, invalid offset or length."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", block size: " << blockSize_
                   << ", page size: " << metaPageSize_
                   << ", chunk size: " << size_;
        return CSErrorCode::InvalidArgError;
    }

    CSChunkFilePtr chunkFile = datastore.GetChunkFile(chunkId_);
    assert(chunkFile != nullptr);

    //asume that the clone chunk use the object unit is OBJ_SIZE which is multiple of page size
    //and use the OBJ_SIZE to split the offset and length into several parts
    //and to check if the parts is in the chunk by bitmap
    //the default OBJ_SIZE is 64KB

    //if the offset is align with OBJ_SIZE then the objNum is length / OBJ_SIZE
    //else the objNum is length / OBJ_SIZE + 1
    uint32_t beginIndex = offset >> blockSize_shift_;
    uint32_t endIndex = (offset + length - 1) >> blockSize_shift_;

    std::vector<File_ObjectInfoPtr> objIns;

    // Curve will ensure that all previous requests arrive or time out
    // before issuing new requests after user initiate a snapshot request.
    // Therefore, this is only a log recovery request, and it must have been
    // executed, and an error code can be returned here.
    if (sn < metaPage_.sn || sn < metaPage_.correctedSn) {
        LOG(WARNING) << "Backward write request."
                     << "ChunkID: " << chunkId_
                     << ",request sn: " << sn
                     << ",chunk sn: " << metaPage_.sn
                     << ",correctedSn: " << metaPage_.correctedSn;
        return CSErrorCode::BackwardRequestError;
    }

    if ((nullptr != ctx) && (sn > metaPage_.sn)) { //just modify the metapage_.sn = sn and updata metapage
        ChunkFileMetaPage tempMeta = metaPage_;
        tempMeta.sn = sn;
        CSErrorCode errorCode = updateMetaPage(&tempMeta);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Update metapage failed."
                       << "ChunkID: " << chunkId_
                       << ",request sn: " << sn
                       << ",chunk sn: " << metaPage_.sn;
            return errorCode;
        }
        metaPage_.sn = tempMeta.sn;
    }
    
    if ((nullptr == ctx) || (false == needCow(sn, ctx))) {//not write to the snapshot but the chunk its self    
        //no need to do read from other clone volume
        if (((endIndex - beginIndex + 1) << blockSize_shift_) == length) {
            // write chunk file

            DVLOG(9) << "Write chunk file directly."
                       << "ChunkID = " << this->chunkId_
                       << ", offset = " << offset
                       << ", length = " << length;

            errorCode = writeDataDirect(buf, offset, length);
            if (errorCode != CSErrorCode::Success) {
                LOG(WARNING) << "Write chunk file failed."
                            << "ChunkID = " << this->chunkId_;
                return errorCode;
            }

            return errorCode;           
        }

        //Judge that the clonefile has the data in <beginIndex, endIndex>
        //if exist just pass to the next step
        //if not read the uncover data and 1. write to the clone file  2. write to snapshot
        CSDataStore::SplitDataIntoObjs (sn, objIns, (beginIndex << blockSize_shift_), ((endIndex - beginIndex + 1) << blockSize_shift_), cloneCtx, datastore, true);

        auto objit = objIns.begin();
        File_ObjectInfoPtr& fileobj = *objit;
        if ((1 == objIns.size()) && (fileobj->fileptr.get() == this)) {//all map is in the chunk
            // write chunk file
            DVLOG(9) << "Write chunk file directly."
                       << "ChunkID = " << this->chunkId_
                       << ", offset = " << offset
                       << ", length = " << length;
            errorCode = writeDataDirect(buf, offset, length);
            if (errorCode != CSErrorCode::Success) {
                LOG(WARNING) << "Write chunk file failed."
                            << "ChunkID = " << chunkId_;
                return errorCode;
            }

            return errorCode;
        }

        int buf_size = 0;
        buf_size += length;

        buf_size += FindExtraReadFromParent(objIns, chunkFile, offset, length);

        std::map<int32_t, Offset_InfoPtr> objmap;

        Offset_InfoPtr winfoptr(new Offset_Info());
        ObjectInfo oinfo;
        oinfo.offset = offset;
        oinfo.length = length;
        oinfo.sn = 0;
        oinfo.snapptr = nullptr;

        File_Object fobj(chunkFile, oinfo);
        winfoptr->offset = offset;
        winfoptr->length = length;
        winfoptr->objs.push_back(fobj);
        
        objmap.insert(std::pair<int32_t, Offset_InfoPtr>(offset, std::move(winfoptr)));
        MergeObjectForRead(objmap, objIns, chunkFile);

        char* tmpbuf = nullptr;
        tmpbuf = new char[(endIndex - beginIndex + 1) << blockSize_shift_];

        for (auto iter = objmap.begin(); iter != objmap.end(); iter++) {
            for (auto& tmpj: iter->second->objs) {
                if (tmpj.fileptr == chunkFile) {
#ifdef MEMORY_SANITY_CHECK
                    //check if the memory is overflow
                    CHECK((tmpj.obj.offset - iter->second->offset + tmpj.obj.length) <= length) 
                            << "offset: " << offset << ", length: " << length
                            << ", tmpj.obj.offset: " << tmpj.obj.offset << ", tmpj.obj.length: " << tmpj.obj.length
                            << ", iter->second->offset: " << iter->second->offset;
#endif
                    buf.copy_to(tmpbuf + (tmpj.obj.offset - iter->second->offset), tmpj.obj.length, 0);
                } else {
#ifdef MEMORY_SANITY_CHECK
                    //check if the memory is overflow
                    CHECK((tmpj.obj.offset - iter->second->offset + tmpj.obj.length) <= length)
                            << "offset: " << offset << ", length: " << length
                            << ", tmpj.obj.offset: " << tmpj.obj.offset << ", tmpj.obj.length: " << tmpj.obj.length
                            << ", iter->second->offset: " << iter->second->offset;

                    CHECK((tmpj.obj.offset - iter->second->offset) >= 0)
                            << "offset: " << offset << ", length: " << length
                            << ", tmpj.obj.offset: " << tmpj.obj.offset << ", tmpj.obj.length: " << tmpj.obj.length
                            << ", iter->second->offset: " << iter->second->offset;
#endif
                    errorCode = CSDataStore::ReadByObjInfo(tmpj.fileptr, tmpbuf + (tmpj.obj.offset - iter->second->offset), tmpj.obj);
                    if (errorCode != CSErrorCode::Success) {
                        LOG(WARNING) << "Write chunk file failed."
                                    << "ChunkID = " << chunkId_;
                        delete [] tmpbuf;
                        return errorCode;
                    }
                }             
            }

            DVLOG(9) << "writeData chunkid = " << chunkId_ 
                       << "  offset: " << iter->second->offset 
                       << ", length: " << iter->second->length
                       << ", tmpbuf: " << static_cast<const void*>(tmpbuf)
                       << ", tmpbuf size " << (endIndex - beginIndex + 1) << blockSize_shift_;
            int rc = writeData(tmpbuf, iter->second->offset, iter->second->length);
            if (rc != iter->second->length) {
                errorCode = CSErrorCode::InternalError;
                delete [] tmpbuf;
                return errorCode;
            }
        }

        delete [] tmpbuf;

        //now need to update the file metadata
        flush();

        return errorCode;

    } else {//snapshot need, so need to do write to the clone chunk which not set

        //Judge that the clonefile has the data in <beginIndex, endIndex>
        //if exist just pass to the next step
        //if not read the uncover data and 1. write to the clone file  2. write to snapshot
        CSDataStore::SplitDataIntoObjs (sn, objIns, (beginIndex << blockSize_shift_), ((endIndex - beginIndex + 1) << blockSize_shift_), cloneCtx, datastore, true);

        auto objit = objIns.begin();
        File_ObjectInfoPtr& fileobj = *objit;
        if ((1 == objIns.size()) && (fileobj->fileptr == chunkFile)) {//all map is in the chunk
            // write chunk file
            DVLOG(9) << "writeSnapData. "
                       << ", sn = " << sn
                       << ", ChunkID = " << this->chunkId_
                       << ", offset = " << offset
                       << ", length = " << length;
            errorCode = writeSnapData(sn, buf, offset, length, cost, ctx);
            if (errorCode != CSErrorCode::Success) {
                LOG(WARNING) << "Write chunk file failed."
                            << "ChunkID = " << chunkId_;
                return errorCode;
            }

            return errorCode;
        }

        //first read the data and write data
        int buf_size = 0;
 
        std::map<int32_t, Offset_InfoPtr> objmap;

        MergeObjectForRead(objmap, objIns, chunkFile);

        char* tmpbuf = new char[(endIndex - beginIndex + 1) << blockSize_shift_];

        for (auto iter = objmap.begin(); iter != objmap.end(); iter++) {
            for (auto& tmpj: iter->second->objs) {
#ifdef MEMORY_SANITY_CHECK
                //check if the memory is overflow
                CHECK((tmpj.obj.offset - iter->second->offset + tmpj.obj.length) <= length)
                        << "offset: " << offset << ", length: " << length
                        << ", tmpj.obj.offset: " << tmpj.obj.offset << ", tmpj.obj.length: " << tmpj.obj.length
                        << ", iter->second->offset: " << iter->second->offset;

                CHECK((tmpj.obj.offset - iter->second->offset) >= 0)
                        << "offset: " << offset << ", length: " << length
                        << ", tmpj.obj.offset: " << tmpj.obj.offset << ", tmpj.obj.length: " << tmpj.obj.length
                        << ", iter->second->offset: " << iter->second->offset;
#endif
                errorCode = CSDataStore::ReadByObjInfo(tmpj.fileptr, tmpbuf + (tmpj.obj.offset - iter->second->offset), tmpj.obj);
                if (errorCode != CSErrorCode::Success) {
                    LOG(WARNING) << "Write chunk file failed."
                                << "ChunkID = " << chunkId_;
                    delete [] tmpbuf;
                    return errorCode;
                }                    
            }

            DVLOG(9) << "writeData chunkid = " << chunkId_ 
                      << "  offset: " << iter->second->offset 
                      << ", length: " << iter->second->length
                      << ", tmpbuf: " << static_cast<const void*>(tmpbuf)
                      << ", tmpbuf size " << (endIndex - beginIndex + 1) << blockSize_shift_;
            int rc = writeData(tmpbuf, iter->second->offset, iter->second->length);
            if (rc != iter->second->length) {
                errorCode = CSErrorCode::InternalError;
                delete [] tmpbuf;
                return errorCode;
            }
        }

        delete [] tmpbuf;

        //now need to update the file metadata
        flush();
        
        DVLOG(9) << "writeSnapData. "
                   << ", sn = " << sn
                   << ", ChunkID = " << this->chunkId_
                   << ", offset = " << offset
                   << ", length = " << length;
        errorCode = writeSnapData(sn, buf, offset, length, cost, ctx);
        if (errorCode != CSErrorCode::Success) {
            LOG(WARNING) << "Write chunk file failed."
                        << "ChunkID = " << chunkId_;
            return errorCode;
        }

        return errorCode;

    }
    return CSErrorCode::Success;

}

//flattenWrite flatten fixed obj_size every time
//the fixed obj_size is 512KB every flatten operations
CSErrorCode CSChunkFile::flattenWrite(SequenceNum sn,
                    off_t offset, size_t length,
                    std::unique_ptr<CloneContext>& cloneCtx,
                    CSDataStore& datastore) {
    CSErrorCode errorCode = CSErrorCode::Success;
    WriteLockGuard writeGuard(rwLock_);

    CSChunkFilePtr chunkFile = datastore.GetChunkFile(chunkId_);
    assert(chunkFile != nullptr);
    assert(cloneCtx->cloneNo > 0);

    //now Decide the offset and length to flatten, according to the 512KB obj_size
    if (nullptr == chunkFile->metaPage_.bitmap) {//clone chunk already full, no need to flatten
        return errorCode;
    }

    uint32_t startIndex, endIndex;
    startIndex = offset >> blockSize_shift_;
    endIndex = (offset + length - 1) >> blockSize_shift_;

    //test if the request is already flattened
    if (Bitmap::NO_POS == chunkFile->metaPage_.bitmap->NextClearBit(startIndex, endIndex)) {//chunk already full, no need to flatten
        return errorCode;
    }

#if 0
    //now Decide the length according to the OBJ_SIZE
    //OBJ_SHIFT is 19, PAGE_SHIFT is 12
    off_t  offset = startIndex << 12;
    size_t length = 0;
    length = ((((startIndex << 12) >> 19) + 1) << 19) - (startIndex << 12);
#endif

    std::vector<File_ObjectInfoPtr> objIns;
    CSDataStore::SplitDataIntoObjs (sn, objIns, offset, length, cloneCtx, datastore, true);
    //print obs ins
    {
        for (auto& mm: objIns) {
            DVLOG(9) << " the fileptr = " << mm->fileptr.get() << " the obj_infos size = " << mm->obj_infos.size();
            for (auto& tt: mm->obj_infos) {
                DVLOG(9) << " the offset = " << tt.offset << " the length = " << tt.length << " the sn = " << tt.sn;
            }
        }
    }

    std::map<int32_t, Offset_InfoPtr> objmap;

    MergeObjectForRead(objmap, objIns, chunkFile);

    char* tmpbuf = new char[length];
    if (nullptr == tmpbuf) {
        LOG(WARNING) << "Flatten chunk file failed not enough memory."
                    << "ChunkID = " << chunkId_;
        return CSErrorCode::InternalError;
    }

    for (auto iter = objmap.begin(); iter != objmap.end(); iter++) {
        for (auto& tmpj: iter->second->objs) {
#ifdef MEMORY_SANITY_CHECK
            //check if the memory is overflow
            CHECK((tmpj.obj.offset - iter->second->offset + tmpj.obj.length) <= length)
                    << "offset: " << offset << ", length: " << length
                    << ", tmpj.obj.offset: " << tmpj.obj.offset << ", tmpj.obj.length: " << tmpj.obj.length
                    << ", iter->second->offset: " << iter->second->offset;

            CHECK((tmpj.obj.offset - iter->second->offset) >= 0)
                    << "offset: " << offset << ", length: " << length
                    << ", tmpj.obj.offset: " << tmpj.obj.offset << ", tmpj.obj.length: " << tmpj.obj.length
                    << ", iter->second->offset: " << iter->second->offset;
#endif
            errorCode = CSDataStore::ReadByObjInfo(tmpj.fileptr, tmpbuf + (tmpj.obj.offset - iter->second->offset), tmpj.obj);
            if (errorCode != CSErrorCode::Success) {
                LOG(WARNING) << "Flatten chunk file failed."
                            << "ChunkID = " << chunkId_;
                delete [] tmpbuf;
                return errorCode;
            }                    
        }

        DVLOG(9) << "writeData chunkid = " << chunkId_ 
                    << "  offset: " << iter->second->offset 
                    << ", length: " << iter->second->length;

        int rc = writeData(tmpbuf, iter->second->offset, iter->second->length);
        if (rc != iter->second->length) {
            LOG(WARNING) << "writeData chunkid = " << chunkId_ 
                    << "  offset: " << iter->second->offset 
                    << ", length: " << iter->second->length
                    << ", rc: " << rc;
            errorCode = CSErrorCode::InternalError;
            delete [] tmpbuf;
            return errorCode;
        }
    }

    delete [] tmpbuf;

    //now need to update the file metadata
    flush();

    return errorCode;
}

CSErrorCode CSChunkFile::createSnapshot(SequenceNum sn) {
    // clone chunk does not allow to create snapshot
    /*
    if (isCloneChunk_) {
        LOG(ERROR) << "Clone chunk can't create snapshot."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ",chunk sn: " << metaPage_.sn;
        return CSErrorCode::StatusConflictError;
    }
    */

    if (snapshots_->contains(sn)) {
        return CSErrorCode::Success;
    }

    // create snapshot
    ChunkOptions options;
    options.id = chunkId_;
    options.sn = sn;
    options.baseDir = baseDir_;
    options.chunkSize = size_;
    options.blockSize = blockSize_;
    options.metaPageSize = metaPageSize_;
    options.metric = metric_;
    auto snapshot_ = new (std::nothrow) CSSnapshot(lfs_,
                                                   chunkFilePool_,
                                                   options);
    CHECK(snapshot_ != nullptr) << "Failed to new CSSnapshot!";
    CSErrorCode errorCode = snapshot_->Open(true);
    if (errorCode != CSErrorCode::Success) {
        delete snapshot_;
        snapshot_ = nullptr;
        LOG(ERROR) << "Create snapshot failed."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ",chunk sn: " << metaPage_.sn;
        return errorCode;
    }

    snapshots_->insert(snapshot_);
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::Sync() {
    WriteLockGuard writeGuard(rwLock_);
    int rc = SyncData();
    if (rc < 0) {
        LOG(ERROR) << "Sync data failed, "
                   << "ChunkID:" << chunkId_;
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::Paste(const char * buf, off_t offset, size_t length) {
    WriteLockGuard writeGuard(rwLock_);
    if (!CheckOffsetAndLength(offset, length)) {
        LOG(ERROR) << "Paste chunk failed, invalid offset or length."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", page size: " << metaPageSize_
                   << ", chunk size: " << size_
                   << ", block size: " << blockSize_;
        return CSErrorCode::InvalidArgError;
    }
    // If it is not a clone chunk, return success directly
    if (!isCloneChunk_) {
        return CSErrorCode::Success;
    }

    // The request above must be blocksize aligned
    // the starting block index number of the paste area
    uint32_t beginIndex = offset / blockSize_;
    // the last block index number of the paste area
    uint32_t endIndex = (offset + length - 1) / blockSize_;
    // Get the unwritten range of the current file
    std::vector<BitRange> uncopiedRange;
    metaPage_.bitmap->Divide(beginIndex,
                             endIndex,
                             &uncopiedRange,
                             nullptr);

    // For the unwritten range, write the corresponding data
    off_t pasteOff;
    size_t pasteSize;
    for (auto& range : uncopiedRange) {
        pasteOff = range.beginIndex * blockSize_;
        pasteSize = (range.endIndex - range.beginIndex + 1) * blockSize_;
        int rc = writeData(buf + (pasteOff - offset), pasteOff, pasteSize);
        if (rc < 0) {
            LOG(ERROR) << "Paste data to chunk failed."
                       << "ChunkID: " << chunkId_
                       << ", offset: " << offset
                       << ", length: " << length;
            return CSErrorCode::InternalError;
        }
    }

    // Update bitmap
    CSErrorCode errorCode = flush();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Paste data to chunk failed."
                    << "ChunkID: " << chunkId_
                    << ", offset: " << offset
                    << ", length: " << length;
        return errorCode;
    }
    return CSErrorCode::Success;
}

bool CSChunkFile::DivideObjInfoByIndex (SequenceNum sn, std::vector<BitRange>& range, std::vector<BitRange>& notInRanges, 
                                        std::vector<ObjectInfo>& objInfos) {

    //to protect the bitmap of the clone chunk and snapshots, get the newest meta page
    ReadLockGuard readGuard(rwLock_);

    bool isFinish = false;

    isFinish = DivideObjInfoByIndexLockless(sn, range, notInRanges, objInfos);

    return isFinish;
}

bool CSChunkFile::DivideObjInfoByIndexLockless (SequenceNum sn, std::vector<BitRange>& range, std::vector<BitRange>& notInRanges, 
                                        std::vector<ObjectInfo>& objInfos) {

    bool isFinish = false;
    //sn == 1 , means that it is original chunk, have no snapshot
    isFinish = DivideSnapshotObjInfoByIndex(sn, range, notInRanges, objInfos);
    if (true == isFinish) {
        return true;
    }

    if (nullptr == metaPage_.bitmap) { //not bitmap means that this chunk is not clone chunk
        for (auto& r : notInRanges) {
            ObjectInfo objInfo;
            objInfo.sn = sn;
            objInfo.snapptr = nullptr;
            objInfo.offset = r.beginIndex << blockSize_shift_;
            objInfo.length = (r.endIndex - r.beginIndex + 1) << blockSize_shift_;
            objInfos.push_back(objInfo);
        }

        return true;
    }

    std::vector<BitRange> setRanges;
    std::vector<BitRange> clearRanges;
    std::vector<BitRange> dataRanges;

    dataRanges = notInRanges;
    
    notInRanges.clear();
    for(auto& r : dataRanges) {
        setRanges.clear();
        clearRanges.clear();

        metaPage_.bitmap->Divide(r.beginIndex, r.endIndex, &clearRanges, &setRanges);
        for (auto& tmpc : clearRanges) {
            notInRanges.push_back (tmpc);
        }

        for (auto& tmpr : setRanges) {
            ObjectInfo objInfo;
            objInfo.sn = sn;
            objInfo.snapptr = nullptr;
            objInfo.offset = tmpr.beginIndex << blockSize_shift_;
            objInfo.length = (tmpr.endIndex - tmpr.beginIndex + 1) << blockSize_shift_;
            objInfos.push_back(objInfo);
        }
    }

    if (notInRanges.empty()) {
        isFinish = true;
    }

    return isFinish;
}

bool CSChunkFile::DivideSnapshotObjInfoByIndex (SequenceNum sn, std::vector<BitRange>& range, 
                                                std::vector<BitRange>& notInRanges, 
                                                std::vector<ObjectInfo>& objInfos) {
    return snapshots_->DivideSnapshotObjInfoByIndex(sn, range, notInRanges, objInfos);
}

//Just read data from specified snapshot
CSErrorCode CSChunkFile::ReadSpecifiedSnap (SequenceNum sn, CSSnapshot* snap, 
                                            char* buff, off_t offset, size_t length) {

    CSErrorCode rc;

    if (nullptr == snap) {
        rc = ReadSpecifiedChunk (sn, buff, offset, length);
        if (rc != CSErrorCode::Success) {
            LOG(ERROR) << "Read specified chunk failed."
                       << "ChunkID: " << chunkId_
                       << ",chunk sn: " << metaPage_.sn;
            return rc;
        }
    }

    ReadLockGuard readGuard(rwLock_);

    rc = snap->Read(buff, offset, length);
    if (rc != CSErrorCode::Success) {
        LOG(ERROR) << "Read specified snapshot failed."
                   << "ChunkID: " << chunkId_
                   << ",chunk sn: " << metaPage_.sn;
        return rc;
    }

    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::Read(char * buf, off_t offset, size_t length) {
    ReadLockGuard readGuard(rwLock_);
    if (!CheckOffsetAndLength(offset, length)) {
        LOG(ERROR) << "Read chunk failed, invalid offset or length."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", page size: " << metaPageSize_
                   << ", chunk size: " << size_
                   << ", block size: " << blockSize_;
        return CSErrorCode::InvalidArgError;
    }

    // If it is clonechunk, ensure that the read area has been written,
    // otherwise an error is returned
    if (isCloneChunk_) {
        // The request above must be blocksize aligned
        // the starting block index number of the paste area
        uint32_t beginIndex = offset / blockSize_;
        // the last block index number of the paste area
        uint32_t endIndex = (offset + length - 1) / blockSize_;
        if ((metaPage_.bitmap != nullptr) && (metaPage_.bitmap->NextClearBit(beginIndex, endIndex)
            != Bitmap::NO_POS)) {
            LOG(ERROR) << "Read chunk file failed, has page never written."
                       << "ChunkID: " << chunkId_
                       << ", offset: " << offset
                       << ", length: " << length;
            return CSErrorCode::PageNerverWrittenError;
        }
    }

    int rc = readData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Read chunk file failed."
                   << "ChunkID: " << chunkId_
                   << ",chunk sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::ReadSpecifiedChunk(SequenceNum sn,
                                            char * buf,
                                            off_t offset,
                                            size_t length)  {
    ReadLockGuard readGuard(rwLock_);
    if (!CheckOffsetAndLength(offset, length)) {
        LOG(ERROR) << "Read specified chunk failed, invalid offset or length."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", page size: " << metaPageSize_
                   << ", chunk size: " << size_
                   << ", block size: " << blockSize_;
        return CSErrorCode::InvalidArgError;
    }
    // If the sequence equals the sequence of the current chunk,
    // read the current chunk file
    if (sn == metaPage_.sn) {
        int rc = readData(buf, offset, length);
        if (rc < 0) {
            LOG(ERROR) << "Read chunk file failed."
                       << "ChunkID: " << chunkId_
                       << ",chunk sn: " << metaPage_.sn;
            return CSErrorCode::InternalError;
        }
        return CSErrorCode::Success;
    }

    std::vector<BitRange> uncopiedRange;
    CSErrorCode errCode = snapshots_->Read(sn, buf, offset, length, &uncopiedRange);
    if (errCode != CSErrorCode::Success) {
        return errCode;
    }

    errCode = CSErrorCode::Success;
    off_t readOff;
    size_t readSize;
    // For uncopied extents, read chunk data
    for (auto& range : uncopiedRange) {
        readOff = range.beginIndex * blockSize_;
        readSize = (range.endIndex - range.beginIndex + 1) * blockSize_;
        DVLOG(9) << "Read real chunk file, offset: " << readOff
                  << ", length: " << readSize
                  << ", chunkID: " << chunkId_
                  << ", buf: " << static_cast<const void*>(buf + (readOff - offset)) ;
#ifdef MEMORY_SANITY_CHECK
        //check if memory is overflow
        CHECK((readOff - offset + readSize) <= length)
                << "offset: " << offset << ", length: " << length
                << ", readOff: " << readOff << ", readSize: " << readSize;
        CHECK((readOff - offset) >= 0)
                << "offset: " << offset << ", length: " << length
                << ", readOff: " << readOff << ", readSize: " << readSize;
#endif
        int rc = readData(buf + (readOff - offset),
                          readOff,
                          readSize);
        if (rc < 0) {
            LOG(ERROR) << "Read chunk file failed. "
                       << "ChunkID: " << chunkId_
                       << ", chunk sn: " << metaPage_.sn;
            return CSErrorCode::InternalError;
        }
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::Delete(SequenceNum sn)  {
    WriteLockGuard writeGuard(rwLock_);
    // If sn is less than the current sequence of the chunk, can not be deleted
    if (sn < metaPage_.sn) {
        LOG(WARNING) << "Delete chunk failed, backward request."
                     << "ChunkID: " << chunkId_
                     << ", request sn: " << sn
                     << ", chunk sn: " << metaPage_.sn;
        return CSErrorCode::BackwardRequestError;
    }

    // There should be no snapshots
    if (snapshots_->getCurrentSn() != 0) {
        LOG(WARNING) << "Delete chunk not allowed. There is snapshot."
                     << "ChunkID: " << chunkId_
                     << ", request sn: " << sn
                     << ", snapshot sn: " << snapshots_->getCurrentSn();
        return CSErrorCode::SnapshotExistError;
    }

    if (fd_ >= 0) {
        lfs_->Close(fd_);
        fd_ = -1;
    }
    int ret = chunkFilePool_->RecycleFile(path());
    if (ret < 0)
        return CSErrorCode::InternalError;

    DVLOG(9) << "Chunk deleted."
              << "ChunkID: " << chunkId_
              << ", request sn: " << sn
              << ", chunk sn: " << metaPage_.sn;
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::DeleteSnapshot(SequenceNum snapSn, std::shared_ptr<SnapContext> ctx) {
    WriteLockGuard writeGuard(rwLock_);

    /*
    // If it is a clone chunk, theoretically this interface should not be called
    if (isCloneChunk_) {
        LOG(ERROR) << "Delete snapshot failed, this is a clone chunk."
                   << "ChunkID: " << chunkId_;
        return CSErrorCode::StatusConflictError;
    }
    */

    /*
     * If chunk.sn>snap.sn, then this snapshot is either a historical snapshot,
     * or a snapshot of the current sequence of the chunk,
     * in this case the snapshot is allowed to be deleted.
     * If chunk.sn<=snap.sn, then this snapshot must be generated after the
     * current delete operation. The current delete operation is the historical
     * log of playback, and deletion is not allowed in this case.
     */
    if(snapshots_->contains(snapSn) && metaPage_.sn > snapshots_->getCurrentSn()){
        return snapshots_->Delete(this, snapSn, ctx);
    }
    return CSErrorCode::Success;
}

void CSChunkFile::GetInfo(CSChunkInfo* info)  {
    ReadLockGuard readGuard(rwLock_);
    info->chunkId = chunkId_;
    info->metaPageSize = metaPageSize_;
    info->chunkSize = size_;
    info->blockSize = blockSize_;
    info->curSn = metaPage_.sn;
    info->correctedSn = metaPage_.correctedSn;
    info->snapSn = snapshots_->getCurrentSn();
    info->isClone = isCloneChunk_;
    info->location = metaPage_.location;
    // There will be a memcpy, otherwise you need to lock the bitmap operation.
    // This step exists on the critical path of ReadChunk, which has certain
    // requirements for performance.
    // TODO(yyk) needs to evaluate which method performs better.
    if (metaPage_.bitmap != nullptr)
        info->bitmap = std::make_shared<Bitmap>(metaPage_.bitmap->Size(),
                                                metaPage_.bitmap->GetBitmap());
    else
        info->bitmap = nullptr;
}

void CSChunkFile::GetCloneInfo(uint64_t& virtualId, uint64_t& cloneNo) {
    ReadLockGuard readGuard(rwLock_);
    virtualId = metaPage_.virtualId;
    cloneNo = metaPage_.cloneNo;

    return;
}

CSErrorCode CSChunkFile::GetHash(off_t offset,
                                 size_t length,
                                 std::string* hash)  {
    ReadLockGuard readGuard(rwLock_);
    uint32_t crc32c = 0;

    char *buf = new(std::nothrow) char[length];
    if (nullptr == buf) {
        return CSErrorCode::InternalError;
    }

    int rc = lfs_->Read(fd_, buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Read chunk file failed."
                   << "ChunkID: " << chunkId_
                   << ",chunk sn: " << metaPage_.sn;
        delete[] buf;
        return CSErrorCode::InternalError;
    }

    crc32c = curve::common::CRC32(crc32c, buf, length);
    *hash = std::to_string(crc32c);

    delete[] buf;

    return CSErrorCode::Success;
}

bool CSChunkFile::needCreateSnapshot(SequenceNum sn, std::shared_ptr<SnapContext> ctx) {
    // ad-hoc hack.  clone chunk cannot create snapshot
    /*
    if (isCloneChunk_)
        return sn > std::max(metaPage_.correctedSn, metaPage_.sn);
    */
    return !ctx->empty() && !snapshots_->contains(ctx->getLatest());
}

bool CSChunkFile::needCow(SequenceNum sn, std::shared_ptr<SnapContext> ctx) {
    // There is no snapshots thus no need to do cow
    if (ctx->empty())
        return false;

    SequenceNum chunkSn = std::max(ctx->getLatest(), metaPage_.sn);
    // Requests smaller than chunkSn will be rejected directly
    if (sn < chunkSn)
        return false;

    // The preceding logic ensures that the sn here must be equal to metaPage.sn
    // Because if sn<metaPage_.sn, the request will be rejected
    // When sn>metaPage_.sn, metaPage.sn will be updated to sn first
    // And because snapSn is normally smaller than metaPage_.sn, snapSn should
    // also be smaller than sn
    // There may be several situations where metaPage_.sn <= snap.sn
    // Scenario 1: DataStore restarts to restore historical logs,
    // metaPage_.sn==snap.sn may appear
    // There was a request to generate a snapshot file before the restart,
    // but it restarted before the metapage was updated
    // After restarting, the previous operation is played back, and the sn of
    // this operation is equal to the sn of the current chunk
    // Scenario 2: The follower downloads a snapshot of the raft through the
    // leader when restoring the raft
    // During the download process, the chunk on the leader is also taking a
    // snapshot of the chunk, and the follower will do log recovery after
    // downloading
    // Since follower downloads the chunk file first, and then downloads the
    // snapshot file, so at this time metaPage_.sn<=snap.sn
    if (sn != metaPage_.sn || metaPage_.sn <= snapshots_->getCurrentSn()) {
        LOG(WARNING) << "May be a log repaly opt after an unexpected restart."
                     << "Request sn: " << sn
                     << ", chunk sn: " << metaPage_.sn
                     << ", snapshot sn: " << snapshots_->getCurrentSn();
        return false;
    }
    return true;
}

CSErrorCode CSChunkFile::updateMetaPage(ChunkFileMetaPage* metaPage) {
    std::unique_ptr<char[]> buf(new char[metaPageSize_]);
    memset(buf.get(), 0, metaPageSize_);
    metaPage->encode(buf.get());
    int rc = writeMetaPage(buf.get());
    if (rc < 0) {
        LOG(ERROR) << "Update metapage failed."
                   << "ChunkID: " << chunkId_
                   << ",chunk sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::loadMetaPage() {
    std::unique_ptr<char[]> buf(new char[metaPageSize_]);
    memset(buf.get(), 0, metaPageSize_);
    int rc = readMetaPage(buf.get());
    if (rc < 0) {
        LOG(ERROR) << "Error occured when reading metaPage_."
                   << " filepath = " << path();
        return CSErrorCode::InternalError;
    }
    return metaPage_.decode(buf.get());
}

CSErrorCode CSChunkFile::copy2Snapshot(off_t offset, size_t length) {
    // Get the uncopied area in the snapshot file
    uint32_t pageBeginIndex = offset / blockSize_;
    uint32_t pageEndIndex = (offset + length - 1) / blockSize_;
    std::vector<BitRange> uncopiedRange;
    CSSnapshot* snapshot_ = snapshots_->getCurrentSnapshot();
    std::shared_ptr<const Bitmap> snapBitmap = snapshot_->GetPageStatus();
    snapBitmap->Divide(pageBeginIndex,
                       pageEndIndex,
                       &uncopiedRange,
                       nullptr);

    CSErrorCode errorCode = CSErrorCode::Success;
    off_t copyOff;
    size_t copySize;
    // Read the uncopied area from the chunk file
    // and write it to the snapshot file
    for (auto& range : uncopiedRange) {
        copyOff = range.beginIndex * blockSize_;
        copySize = (range.endIndex - range.beginIndex + 1) * blockSize_;
        std::unique_ptr<char[]> buf(new char[copySize]);
        int rc = readData(buf.get(),
                          copyOff,
                          copySize);
        if (rc < 0) {
            LOG(ERROR) << "Read from chunk file failed."
                       << "ChunkID: " << chunkId_
                       << ",chunk sn: " << metaPage_.sn;
            return CSErrorCode::InternalError;
        }
        errorCode = snapshot_->Write(buf.get(), copyOff, copySize);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Write to snapshot failed."
                       << "ChunkID: " << chunkId_
                       << ",chunk sn: " << metaPage_.sn
                       << ",snapshot sn: " << snapshot_->GetSn();
            return errorCode;
        }
    }
    // If the snapshot file has been written,
    // you need to call Flush to persist the metapage
    if (uncopiedRange.size() > 0) {
        errorCode = snapshot_->Flush();
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Flush snapshot metapage failed."
                        << "ChunkID: " << chunkId_
                        << ",chunk sn: " << metaPage_.sn
                        << ",snapshot sn: " << snapshot_->GetSn();
            return errorCode;
        }
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::flush() {
    ChunkFileMetaPage tempMeta = metaPage_;
    bool needUpdateMeta = dirtyPages_.size() > 0;

    for (auto pageIndex : dirtyPages_) {
        tempMeta.bitmap->Set(pageIndex);
    }
    if (isCloneChunk_) {
        // If all pages have been written, mark the Chunk as a non-clone chunk
        if (tempMeta.bitmap->NextClearBit(0) == Bitmap::NO_POS) {
            tempMeta.location = "";
            tempMeta.bitmap = nullptr;
            needUpdateMeta = true;
        }
    }
    if (needUpdateMeta) {
        CSErrorCode errorCode = updateMetaPage(&tempMeta);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Update metapage failed."
                        << "ChunkID: " << chunkId_
                        << ",chunk sn: " << metaPage_.sn;
            return errorCode;
        }

        metaPage_.location = tempMeta.location;
        metaPage_.bitmap = tempMeta.bitmap;

        if (isCloneChunk_) {
            if (metaPage_.bitmap == nullptr) {
                if (metric_ != nullptr) {
                    metric_->cloneChunkCount << -1;
                }
                isCloneChunk_ = false;
            }
        }

        dirtyPages_.clear();
    }
    return CSErrorCode::Success;
}

}  // namespace chunkserver
}  // namespace curve
