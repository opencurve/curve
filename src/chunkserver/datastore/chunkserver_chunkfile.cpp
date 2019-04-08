/*
 * Project: curve
 * File Created: Thursday, 6th September 2018 10:49:53 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#include <fcntl.h>
#include <algorithm>

#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/chunkserver_chunkfile.h"

namespace curve {
namespace chunkserver {

void ChunkFileMetaPage::encode(char* buf) {
    size_t len = 0;
    memcpy(buf, &version, sizeof(version));
    len += sizeof(version);
    memcpy(buf + len, &sn, sizeof(sn));
    len += sizeof(sn);
    memcpy(buf + len, &correctedSn, sizeof(correctedSn));
    len += sizeof(correctedSn);
    uint32_t crc = ::curve::common::CRC32(buf, len);
    memcpy(buf + len, &crc, sizeof(crc));
}

CSErrorCode ChunkFileMetaPage::decode(const char* buf) {
    size_t len = 0;
    memcpy(&version, buf, sizeof(version));
    // TODO(yyk) 判断版本兼容性，当前简单处理，后续详细实现
    if (version != FORMAT_VERSION) {
        LOG(ERROR) << "File format version incompatible."
                    << "file version: "
                    << static_cast<uint32_t>(version)
                    << ", format version: "
                    << static_cast<uint32_t>(FORMAT_VERSION);
        return CSErrorCode::IncompatibleError;
    }
    len += sizeof(version);
    memcpy(&sn, buf + len, sizeof(sn));
    len += sizeof(sn);
    memcpy(&correctedSn, buf + len, sizeof(correctedSn));
    len += sizeof(correctedSn);
    uint32_t crc =  ::curve::common::CRC32(buf, len);
    uint32_t recordCrc;
    memcpy(&recordCrc, buf + len, sizeof(recordCrc));
    // 校验crc，校验失败返回错误码
    if (crc != recordCrc) {
        LOG(ERROR) << "Checking Crc32 failed.";
        return CSErrorCode::CrcCheckError;  // 需定义crc校验失败的错误码
    }
    return CSErrorCode::Success;
}

CSChunkFile::CSChunkFile(std::shared_ptr<LocalFileSystem> lfs,
                         std::shared_ptr<ChunkfilePool> ChunkfilePool,
                         const ChunkOptions& options)
    : fd_(-1),
      size_(options.chunkSize),
      pageSize_(options.pageSize),
      chunkId_(options.id),
      baseDir_(options.baseDir),
      snapshot_(nullptr),
      chunkfilePool_(ChunkfilePool),
      lfs_(lfs) {
    CHECK(!baseDir_.empty()) << "Create chunk file failed";
    CHECK(lfs_ != nullptr) << "Create chunk file failed";
    metaPage_.sn = options.sn;
}

CSChunkFile::~CSChunkFile() {
    if (snapshot_ != nullptr) {
        delete snapshot_;
        snapshot_ = nullptr;
    }

    if (fd_ >= 0) {
        lfs_->Close(fd_);
    }
}

CSErrorCode CSChunkFile::Open(bool createFile) {
    WriteLockGuard writeGuard(rwLock_);
    string chunkFilePath = path();
    // 创建新文件,如果chunk文件已经存在则不用再创建
    // chunk文件存在可能有两种情况引起:
    // 1.getchunk成功，但是后面stat或者loadmetapage时失败，下载再open的时候；
    // 2.两个写请求并发创建新的chunk文件
    if (createFile
        && !lfs_->FileExists(chunkFilePath)
        && metaPage_.sn > 0) {
        char buf[pageSize_] = {0};
        metaPage_.encode(buf);
        int rc = chunkfilePool_->GetChunk(chunkFilePath, buf);
        if (rc != 0) {
            LOG(ERROR) << "Error occured when create file."
                   << " filepath = " << chunkFilePath;
            return CSErrorCode::InternalError;
        }
    }
    int rc = lfs_->Open(chunkFilePath, O_RDWR|O_NOATIME|O_DSYNC);
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
                   << ",filesize = " << fileInfo.st_size;
        return CSErrorCode::FileFormatError;
    }
    return loadMetaPage();
}

CSErrorCode CSChunkFile::LoadSnapshot(SequenceNum sn) {
    WriteLockGuard writeGuard(rwLock_);
    if (snapshot_ != nullptr) {
        LOG(ERROR) << "Snapshot conflict."
                   << " ChunkID: " << chunkId_
                   << " Exist snapshot sn: " << snapshot_->GetSn()
                   << " Request snapshot sn: " << sn;
        return CSErrorCode::SnapshotConflictError;
    }
    ChunkOptions options;
    options.id = chunkId_;
    options.sn = sn;
    options.baseDir = baseDir_;
    options.chunkSize = size_;
    options.pageSize = pageSize_;
    snapshot_ = new(std::nothrow) CSSnapshot(lfs_,
                                            chunkfilePool_,
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
    }
    return errorCode;
}

CSErrorCode CSChunkFile::Write(SequenceNum sn,
                       const char * buf,
                       off_t offset,
                       size_t length,
                       uint32_t* cost) {
    WriteLockGuard writeGuard(rwLock_);
    if (offset + length > size_) {
        LOG(ERROR) << "Write chunk out of range."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", chunk size: " << size_;
        return CSErrorCode::OutOfRangeError;
    }
    // 用户快照以后会保证之前的请求全部到达或者超时以后才会下发新的请求
    // 因此此处只可能是日志恢复的请求，且一定已经执行，此处可返回错误码
    if (sn < metaPage_.sn || sn < metaPage_.correctedSn) {
        LOG(ERROR) << "Backward write request."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ",chunk sn: " << metaPage_.sn
                   << ",correctedSn: " << metaPage_.correctedSn;
        return CSErrorCode::BackwardRequestError;
    }
    // 判断是否需要创建快照文件
    if (needCreateSnapshot(sn)) {
        // 存在历史快照未被删掉
        if (snapshot_ != nullptr) {
            LOG(WARNING) << "Exists old snapshot."
                         << "ChunkID: " << chunkId_
                         << ",request sn: " << sn
                         << ",chunk sn: " << metaPage_.sn
                         << ",old snapshot sn: "
                         << snapshot_->GetSn();
            return CSErrorCode::SnapshotConflictError;
        }
        ChunkOptions options;
        options.id = chunkId_;
        options.sn = metaPage_.sn;
        options.baseDir = baseDir_;
        options.chunkSize = size_;
        options.pageSize = pageSize_;
        snapshot_ = new(std::nothrow) CSSnapshot(lfs_,
                                                 chunkfilePool_,
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
    }
    // 如果请求版本号大于当前chunk版本号，需要更新metapage
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
    // 判断是否要cow,若是先将数据拷贝到快照文件
    if (needCow(sn)) {
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
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::Read(char * buf, off_t offset, size_t length) {
    ReadLockGuard readGuard(rwLock_);
    if (offset + length > size_) {
        LOG(ERROR) << "Read chunk out of range."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", chunk size: " << size_;
        return CSErrorCode::OutOfRangeError;
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
    if (offset + length > size_) {
        LOG(ERROR) << "Read snapshot out of range."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", chunk size: " << size_;
        return CSErrorCode::OutOfRangeError;
    }
    // 版本为当前chunk的版本，则读当前chunk文件
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
    // 快照文件不存在或者版本不为快照文件的版本，返回ChunkNotExist错误
    if (snapshot_ == nullptr || sn != snapshot_->GetSn()) {
        return CSErrorCode::ChunkNotExistError;
    }

    // 版本等于快照文件的版本
    vector<Extent> copiedExtents;
    vector<Extent> uncopiedExtents;
    partExtents(offset, length, &copiedExtents, &uncopiedExtents);
    CSErrorCode errorCode = CSErrorCode::Success;
    // 对于未拷贝的extent，读chunk的数据
    for (auto& extent : uncopiedExtents) {
        int rc = readData(buf + (extent.offset - offset),
                          extent.offset,
                          extent.length);
        if (rc < 0) {
            LOG(ERROR) << "Read chunk file failed. "
                       << "ChunkID: " << chunkId_
                       << ", chunk sn: " << metaPage_.sn;
            return CSErrorCode::InternalError;
        }
    }
    // 对于以拷贝的extent，读snapshot的数据
    for (auto& extent : copiedExtents) {
        errorCode = snapshot_->Read(buf + (extent.offset - offset),
                                    extent.offset,
                                    extent.length);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Read chunk file failed."
                       << "ChunkID: " << chunkId_
                       << ",chunk sn: " << metaPage_.sn;
            return errorCode;
        }
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::Delete()  {
    WriteLockGuard writeGuard(rwLock_);
    if (snapshot_ != nullptr) {
        LOG(ERROR) << "Delete chunk failed, has snapshot. "
                   << "ChunkID: " << chunkId_
                   << ", chunk sn: " << metaPage_.sn;
        return CSErrorCode::SnapshotExistError;
    }
    if (fd_ >= 0) {
        lfs_->Close(fd_);
        fd_ = -1;
    }
    int ret = chunkfilePool_->RecycleChunk(path());
    if (ret < 0)
        return CSErrorCode::InternalError;
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::DeleteSnapshot(SequenceNum fileSn)  {
    WriteLockGuard writeGuard(rwLock_);

    // delete snapshot if exists
    // 快照文件存在时，判断fileSn与当前sn_的大小
    // 正常情况下fileSn等于sn_,表示当前chunk的快照是此次转储过程中产生的
    // 特殊情况下如果fileSn大于sn_,说明快照是历史快照产生的没有删除掉
    // 但如果fileSn小于sn_，一般发生在日志恢复的时候，且恢复之前又一个版本号更加新
    // 的写请求，那么快照文件应当是在删除操作后产生的，此时不能删除快照文件
    if (snapshot_ != nullptr && fileSn >= metaPage_.sn) {
        CSErrorCode errorCode = snapshot_->Delete();
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Delete snapshot failed."
                       << "ChunkID: " << chunkId_
                       << ",snapshot sn: " << snapshot_->GetSn();
            return errorCode;
        }
        delete snapshot_;
        snapshot_ = nullptr;
    }

    // 当快照系统调用DeleteSnapshotChunk以后，就不需要再cow了
    // 所以设置correctSn_为fileSn,写入时判断请求的sn小于等于correctSn_
    // 就不需要再创建快照做cow
    // TODO(yyk) 后面考虑将修改correctedSn的逻辑和删除快照的逻辑分开
    SequenceNum chunkSn = std::max(metaPage_.correctedSn, metaPage_.sn);
    if (fileSn > chunkSn) {
        ChunkFileMetaPage tempMeta = metaPage_;
        tempMeta.correctedSn = fileSn;
        CSErrorCode errorCode = updateMetaPage(&tempMeta);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Update metapage failed."
                       << "ChunkID: " << chunkId_
                       << ",chunk sn: " << metaPage_.sn;
            return errorCode;
        }
        metaPage_.correctedSn = tempMeta.correctedSn;
    }

    return CSErrorCode::Success;
}

void CSChunkFile::GetInfo(CSChunkInfo* info)  {
    ReadLockGuard readGuard(rwLock_);
    info->chunkId = chunkId_;
    info->chunkSize = size_;
    info->curSn = metaPage_.sn;
    info->correctedSn = metaPage_.correctedSn;
    info->snapSn = (snapshot_ == nullptr
                        ? 0
                        : snapshot_->GetSn());
}

bool CSChunkFile::needCreateSnapshot(SequenceNum sn) {
    // correctSn_和sn_中最大值可以表示chunk文件的真实版本号
    SequenceNum chunkSn = std::max(metaPage_.correctedSn, metaPage_.sn);
    // 对于小于chunk版本号的请求会拒绝写入，因此不会产生快照
    // 对于等于chunk版本号的请求，说明chunk之前有被相同版本号的请求写过
    // 之前必然已经生成过快照文件，因此也不需要创建新的快照
    if (sn <= chunkSn)
        return false;
    // 请求版本大于chunk，且chunk存在快照文件，可能有两种原因：
    // 1.上次写请求产生了快照文件，但是metapage更新失败；
    // 2.有以前的历史快照文件未被删除
    // 对于第1种情况，sn_一定等于快照的版本号，可以直接使用当前快照文件
    // 对于第2种情况，理论不会发生，应当报错
    if (nullptr != snapshot_ && metaPage_.sn == snapshot_->GetSn()) {
        return false;
    }
    return true;
}

bool CSChunkFile::needCow(SequenceNum sn) {
    SequenceNum chunkSn = std::max(metaPage_.correctedSn, metaPage_.sn);
    // 对于小于chunkSn的请求，会直接拒绝
    if (sn < chunkSn)
        return false;
    // 这种情况说明当前chunk已经转储成功了，无需再做cow
    if (nullptr == snapshot_ || sn == metaPage_.correctedSn)
        return false;
    // 如果快照文件已损坏，也不需要再做cow
    if (snapshot_->IsDamaged())
        return false;
    // 此时，当前chunk一定存在快照文件，且sn等于chunk的sn,大于快照的sn
    if (sn != metaPage_.sn || sn <= snapshot_->GetSn()) {
        snapshot_->SetDamaged();
        LOG(ERROR) << "Can not process the sequence num."
                   << "request sn: " << sn
                   << ",chunk sn:" << metaPage_.sn
                   << ",snapshot sn: " << snapshot_->GetSn();
        return false;
    }
    return true;
}

CSErrorCode CSChunkFile::updateMetaPage(ChunkFileMetaPage* metaPage) {
    char buf[pageSize_] = {0};
    metaPage->encode(buf);
    int rc = writeMetaPage(buf);
    if (rc < 0) {
        LOG(ERROR) << "Update metapage failed."
                   << "ChunkID: " << chunkId_
                   << ",chunk sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::loadMetaPage() {
    char buf[pageSize_] = {0};
    int rc = readMetaPage(buf);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when reading metaPage_."
                   << " filepath = " << path();
        return CSErrorCode::InternalError;
    }
    return metaPage_.decode(buf);
}

CSErrorCode CSChunkFile::copy2Snapshot(off_t offset, size_t length) {
    CSErrorCode errorCode = CSErrorCode::Success;
    vector<Extent> uncopiedExtents;
    partExtents(offset, length, nullptr, &uncopiedExtents);
    // 将未拷贝过的区域从chunk文件读取出来，写入到snapshot文件
    for (auto& extent : uncopiedExtents) {
        char buf[extent.length] = {0};
        int rc = readData(buf,
                          extent.offset,
                          extent.length);
        if (rc < 0) {
            LOG(ERROR) << "Read from chunk file failed."
                       << "ChunkID: " << chunkId_
                       << ",chunk sn: " << metaPage_.sn;
            return CSErrorCode::InternalError;
        }
        errorCode = snapshot_->Write(buf, extent.offset, extent.length);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Write to snapshot failed."
                       << "ChunkID: " << chunkId_
                       << ",chunk sn: " << metaPage_.sn
                       << ",snapshot sn: " << snapshot_->GetSn();
            return errorCode;
        }
    }
    // 如果快照文件被写过，需要调用Flush持久化metapage
    if (uncopiedExtents.size() > 0) {
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

void CSChunkFile::partExtents(off_t offset,
                              size_t length,
                              vector<Extent>* copiedExtents,
                              vector<Extent>* uncopiedExtents) {
    if (length == 0)
        return;
    uint32_t pageBeginIndex = offset / pageSize_;
    uint32_t pageEndIndex = (offset + length - 1) / pageSize_;
    Extent ext;
    ext.offset = pageBeginIndex * pageSize_;
    ext.length = 0;
    // copiedFlag表示当前extent属于已经拷贝过的extent还是未拷贝的extent
    bool copiedFlag = snapshot_->IsPageWritten(pageBeginIndex);
    bool hasCopiedPtr = copiedExtents != nullptr;
    bool hasUncopiedPtr = uncopiedExtents != nullptr;
    for (uint32_t i = pageBeginIndex; i <= pageEndIndex; ++i) {
        if (copiedFlag != snapshot_->IsPageWritten(i)) {
            // 将当前extent加到对应的vector
            if (copiedFlag && hasCopiedPtr) {
                copiedExtents->push_back(ext);
            } else if (!copiedFlag && hasUncopiedPtr) {
                uncopiedExtents->push_back(ext);
            }
            // 重新初始化extent
            ext.offset = i * pageSize_;
            ext.length = pageSize_;
            // 标记反转
            copiedFlag = !copiedFlag;
        } else {
            ext.length += pageSize_;
        }
    }
    // 将最后一个extent加到对应的vector
    if (copiedFlag && hasCopiedPtr) {
        copiedExtents->push_back(ext);
    } else if (!copiedFlag && hasUncopiedPtr) {
        uncopiedExtents->push_back(ext);
    }
}

}  // namespace chunkserver
}  // namespace curve
