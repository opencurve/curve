/*
 * Project: curve
 * File Created: Thursday, 6th September 2018 10:49:53 am
 * Author: yangyaokai
 * Copyright (c) 2018 NetEase
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
    size_t loc_size = location.size();
    memcpy(buf + len, &loc_size, sizeof(loc_size));
    len += sizeof(loc_size);
    // CloneChunk需要序列化位置信息和bitmap信息
    if (loc_size > 0) {
        memcpy(buf + len, location.c_str(), loc_size);
        len += loc_size;
        uint32_t bits = bitmap->Size();
        memcpy(buf + len, &bits, sizeof(bits));
        len += sizeof(bits);
        size_t bitmapBytes = (bits + 8 - 1) >> 3;
        memcpy(buf + len, bitmap->GetBitmap(), bitmapBytes);
        len += bitmapBytes;
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
    }
    uint32_t crc =  ::curve::common::CRC32(buf, len);
    uint32_t recordCrc;
    memcpy(&recordCrc, buf + len, sizeof(recordCrc));
    // 校验crc，校验失败返回错误码
    if (crc != recordCrc) {
        LOG(ERROR) << "Checking Crc32 failed.";
        return CSErrorCode::CrcCheckError;  // 需定义crc校验失败的错误码
    }

    // TODO(yyk) 判断版本兼容性，当前简单处理，后续详细实现
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

CSChunkFile::CSChunkFile(std::shared_ptr<LocalFileSystem> lfs,
                         std::shared_ptr<ChunkfilePool> chunkfilePool,
                         const ChunkOptions& options)
    : fd_(-1),
      size_(options.chunkSize),
      pageSize_(options.pageSize),
      chunkId_(options.id),
      baseDir_(options.baseDir),
      isCloneChunk_(false),
      snapshot_(nullptr),
      chunkfilePool_(chunkfilePool),
      lfs_(lfs),
      metric_(options.metric) {
    CHECK(!baseDir_.empty()) << "Create chunk file failed";
    CHECK(lfs_ != nullptr) << "Create chunk file failed";
    metaPage_.sn = options.sn;
    metaPage_.correctedSn = options.correctedSn;
    metaPage_.location = options.location;
    // 如果location不为空，则为CloneChunk，需要初始化Bitmap
    if (!metaPage_.location.empty()) {
        uint32_t bits = size_ / pageSize_;
        metaPage_.bitmap = std::make_shared<Bitmap>(bits);
    }
    if (metric_ != nullptr) {
        metric_->chunkFileCount << 1;
    }
}

CSChunkFile::~CSChunkFile() {
    if (snapshot_ != nullptr) {
        delete snapshot_;
        snapshot_ = nullptr;
    }

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
    // 创建新文件,如果chunk文件已经存在则不用再创建
    // chunk文件存在可能有两种情况引起:
    // 1.getchunk成功，但是后面stat或者loadmetapage时失败，下次再open的时候；
    // 2.两个写请求并发创建新的chunk文件
    if (createFile
        && !lfs_->FileExists(chunkFilePath)
        && metaPage_.sn > 0) {
        char buf[pageSize_] = {0};
        metaPage_.encode(buf);
        int rc = chunkfilePool_->GetChunk(chunkFilePath, buf);
        // 并发创建文件时，可能前面线程已经创建成功，那么这里会返回-EEXIST
        // 此时可以继续open已经生成的文件
        // 不过当前同一个chunk的操作是串行的，不会出现这个问题
        if (rc != 0  && rc != -EEXIST) {
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
                   << ", real filesize = " << fileInfo.st_size
                   << ", expect filesize = " << fileSize();
        return CSErrorCode::FileFormatError;
    }

    CSErrorCode errCode = loadMetaPage();
    // 重启后，只有重新open加载metapage后，才能知道是否为clone chunk
    if (!metaPage_.location.empty() && !isCloneChunk_) {
        if (metric_ != nullptr) {
            metric_->cloneChunkCount << 1;
        }
        isCloneChunk_ = true;
    }
    return errCode;
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
    options.metric = metric_;
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
    if (!CheckOffsetAndLength(offset, length)) {
        LOG(ERROR) << "Write chunk failed, invalid offset or length."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", page size: " << pageSize_
                   << ", chunk size: " << size_;
        return CSErrorCode::InvalidArgError;
    }
    // 用户快照以后会保证之前的请求全部到达或者超时以后才会下发新的请求
    // 因此此处只可能是日志恢复的请求，且一定已经执行，此处可返回错误码
    if (sn < metaPage_.sn || sn < metaPage_.correctedSn) {
        LOG(WARNING) << "Backward write request."
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
            LOG(ERROR) << "Exists old snapshot."
                       << "ChunkID: " << chunkId_
                       << ",request sn: " << sn
                       << ",chunk sn: " << metaPage_.sn
                       << ",old snapshot sn: "
                       << snapshot_->GetSn();
            return CSErrorCode::SnapshotConflictError;
        }

        // clone chunk不允许创建快照
        if (isCloneChunk_) {
            LOG(ERROR) << "Clone chunk can't create snapshot."
                       << "ChunkID: " << chunkId_
                       << ",request sn: " << sn
                       << ",chunk sn: " << metaPage_.sn;
            return CSErrorCode::StatusConflictError;
        }

        // 创建快照
        ChunkOptions options;
        options.id = chunkId_;
        options.sn = metaPage_.sn;
        options.baseDir = baseDir_;
        options.chunkSize = size_;
        options.pageSize = pageSize_;
        options.metric = metric_;
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
    // 如果是clone chunk会更新bitmap
    CSErrorCode errorCode = flush();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Write data to chunk file failed."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ",chunk sn: " << metaPage_.sn;
        return errorCode;
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
                   << ", page size: " << pageSize_
                   << ", chunk size: " << size_;
        return CSErrorCode::InvalidArgError;
    }
    // 如果不是clone chunk直接返回成功
    if (!isCloneChunk_) {
        return CSErrorCode::Success;
    }

    // 上面下来的请求必须是pagesize对齐的
    // 请求paste区域的起始page索引号
    uint32_t beginIndex = offset / pageSize_;
    // 请求paste区域的最后一个page索引号
    uint32_t endIndex = (offset + length - 1) / pageSize_;
    // 获取当前文件未被写过的range
    std::vector<BitRange> uncopiedRange;
    metaPage_.bitmap->Divide(beginIndex,
                             endIndex,
                             &uncopiedRange,
                             nullptr);

    // 对于未被写过的range，将相应的数据写入
    off_t pasteOff;
    size_t pasteSize;
    for (auto& range : uncopiedRange) {
        pasteOff = range.beginIndex * pageSize_;
        pasteSize = (range.endIndex - range.beginIndex + 1) * pageSize_;
        int rc = writeData(buf + (pasteOff - offset), pasteOff, pasteSize);
        if (rc < 0) {
            LOG(ERROR) << "Paste data to chunk failed."
                       << "ChunkID: " << chunkId_
                       << ", offset: " << offset
                       << ", length: " << length;
            return CSErrorCode::InternalError;
        }
    }

    // 更新bitmap
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

CSErrorCode CSChunkFile::Read(char * buf, off_t offset, size_t length) {
    ReadLockGuard readGuard(rwLock_);
    if (!CheckOffsetAndLength(offset, length)) {
        LOG(ERROR) << "Read chunk failed, invalid offset or length."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", page size: " << pageSize_
                   << ", chunk size: " << size_;
        return CSErrorCode::InvalidArgError;
    }

    // 如果是 clonechunk ,要保证读取区域已经被写过，否则返回错误
    if (isCloneChunk_) {
        // 上面下来的请求必须是pagesize对齐的
        // 请求paste区域的起始page索引号
        uint32_t beginIndex = offset / pageSize_;
        // 请求paste区域的最后一个page索引号
        uint32_t endIndex = (offset + length - 1) / pageSize_;
        if (metaPage_.bitmap->NextClearBit(beginIndex, endIndex)
            != Bitmap::NO_POS) {
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
                   << ", page size: " << pageSize_
                   << ", chunk size: " << size_;
        return CSErrorCode::InvalidArgError;
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

    // 获取快照文件中已拷贝过和未被拷贝过的区域
    uint32_t pageBeginIndex = offset / pageSize_;
    uint32_t pageEndIndex = (offset + length - 1) / pageSize_;
    std::vector<BitRange> copiedRange;
    std::vector<BitRange> uncopiedRange;
    std::shared_ptr<const Bitmap> snapBitmap = snapshot_->GetPageStatus();
    snapBitmap->Divide(pageBeginIndex,
                       pageEndIndex,
                       &uncopiedRange,
                       &copiedRange);

    CSErrorCode errorCode = CSErrorCode::Success;
    off_t readOff;
    size_t readSize;
    // 对于未拷贝的extent，读chunk的数据
    for (auto& range : uncopiedRange) {
        readOff = range.beginIndex * pageSize_;
        readSize = (range.endIndex - range.beginIndex + 1) * pageSize_;
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
    // 对于已拷贝的range，读snapshot的数据
    for (auto& range : copiedRange) {
        readOff = range.beginIndex * pageSize_;
        readSize = (range.endIndex - range.beginIndex + 1) * pageSize_;
        errorCode = snapshot_->Read(buf + (readOff - offset),
                                    readOff,
                                    readSize);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Read chunk file failed."
                       << "ChunkID: " << chunkId_
                       << ",chunk sn: " << metaPage_.sn;
            return errorCode;
        }
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::Delete(SequenceNum sn)  {
    WriteLockGuard writeGuard(rwLock_);
    // 如果 sn 小于当前chunk的版本号，不允许删除
    if (sn < metaPage_.sn) {
        LOG(WARNING) << "Delete chunk failed, backward request."
                     << "ChunkID: " << chunkId_
                     << ", request sn: " << sn
                     << ", chunk sn: " << metaPage_.sn;
        return CSErrorCode::BackwardRequestError;
    }

    // 如果存在快照，就先删除快照，正常是不会出现这种调用的
    // Delete语义就是将chunk删除，不管存不存在快照
    if (snapshot_ != nullptr) {
        CSErrorCode errorCode = snapshot_->Delete();
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Delete snapshot failed."
                       << "ChunkID: " << chunkId_
                       << ",snapshot sn: " << snapshot_->GetSn();
            return errorCode;
        }
        LOG(INFO) << "Snapshot deleted."
                  << "ChunkID: " << chunkId_
                  << ", snapshot sn: " << snapshot_->GetSn()
                  << ", chunk sn: " << metaPage_.sn;
        delete snapshot_;
        snapshot_ = nullptr;
    }

    if (fd_ >= 0) {
        lfs_->Close(fd_);
        fd_ = -1;
    }
    int ret = chunkfilePool_->RecycleChunk(path());
    if (ret < 0)
        return CSErrorCode::InternalError;

    LOG(INFO) << "Chunk deleted."
              << "ChunkID: " << chunkId_
              << ", request sn: " << sn
              << ", chunk sn: " << metaPage_.sn;
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::DeleteSnapshotOrCorrectSn(SequenceNum correctedSn)  {
    WriteLockGuard writeGuard(rwLock_);

    // 如果是clone chunk， 理论上不应该会调这个接口，返回错误
    if (isCloneChunk_) {
        LOG(ERROR) << "Delete snapshot failed, this is a clone chunk."
                   << "ChunkID: " << chunkId_;
        return CSErrorCode::StatusConflictError;
    }

    // 如果correctedSn小于当前chunk的sn或者correctedSn
    // 那么该请求要么是游离请求，要么是已经执行过了然后日志恢复时重放了
    if (correctedSn < metaPage_.sn || correctedSn < metaPage_.correctedSn) {
        LOG(WARNING) << "Backward delete snapshot request."
                     << "ChunkID: " << chunkId_
                     << ", correctedSn: " << correctedSn
                     << ", chunk.sn: " << metaPage_.sn
                     << ", chunk.correctedSn: " << metaPage_.correctedSn;
        return CSErrorCode::BackwardRequestError;
    }

    /*
     * 由于上一步的判断，此时correctedSn>=metaPage_.sn && metaPage_.correctedSn
     * 此时通过版本判断当前快照文件与当前chunk文件的关系
     * 如果chunk.sn>snap.sn，那么这个快照要么就是历史快照，
     * 要么就是当前版本chunk的快照，这种情况允许删除快照
     * 如果chunk.sn<=snap.sn，则这个快照一定是在当前删除操作之后产生的
     * 当前的删除操作时回放的历史日志，这种情况不允许删除
     */
    if (snapshot_ != nullptr && metaPage_.sn > snapshot_->GetSn()) {
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

    /*
     * 写数据时，会比较metapage中的sn和correctedSn的最大值
     * 如果写请求的版本大于这个最大值就会产生快照
     * 如果调用了DeleteSnapshotChunkOrCorrectSn，在没有新快照的情况下，就不需要再cow了
     * 1.所以当发现参数中的correctedSn大于最大值，需要更新metapage中的correctedSn
     *   这样下次如果有数据写入就不会产生快照
     * 2.如果等于最大值，要么就是此次快照转储过程中chunk被写过，要么就是重复调用了此接口
     *   此时不需要更改metapage
     * 3.如果小于最大值，正常情况只有raft日志恢复时才会出现
     */
    SequenceNum chunkSn = std::max(metaPage_.correctedSn, metaPage_.sn);
    if (correctedSn > chunkSn) {
        ChunkFileMetaPage tempMeta = metaPage_;
        tempMeta.correctedSn = correctedSn;
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
    info->pageSize = pageSize_;
    info->chunkSize = size_;
    info->curSn = metaPage_.sn;
    info->correctedSn = metaPage_.correctedSn;
    info->snapSn = (snapshot_ == nullptr
                        ? 0
                        : snapshot_->GetSn());
    info->isClone = isCloneChunk_;
    info->location = metaPage_.location;
    // 这里会有一次memcpy，否则需要对bitmap操作加锁
    // 这一步存在ReadChunk关键路径上，对性能会有一定要求
    // TODO(yyk) 需要评估哪种方法性能更好
    if (metaPage_.bitmap != nullptr)
        info->bitmap = std::make_shared<Bitmap>(metaPage_.bitmap->Size(),
                                                metaPage_.bitmap->GetBitmap());
    else
        info->bitmap = nullptr;
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

bool CSChunkFile::needCreateSnapshot(SequenceNum sn) {
    // correctSn_和sn_中最大值可以表示chunk文件的真实版本号
    SequenceNum chunkSn = std::max(metaPage_.correctedSn, metaPage_.sn);
    // 对于小于chunk版本号的请求会拒绝写入，因此不会产生快照
    // 对于等于chunk版本号的请求，说明chunk之前有被相同版本号的请求写过
    // 之前必然已经生成过快照文件，因此也不需要创建新的快照
    if (sn <= chunkSn)
        return false;
    // 请求版本大于chunk，且chunk存在快照文件，可能有多种原因：
    // 1.上次写请求产生了快照文件，但是metapage更新失败；
    // 2.有以前的历史快照文件未被删除
    // 3.raft的follower恢复时从leader下载raft快照，同时chunk也在做快照
    //   由于先下载的chunk文件，下载过程中chunk可能做了多次快照
    //   下载以后follower做日志恢复，可能出现
    // 对于第1种情况，sn_一定等于快照的版本号，可以直接使用当前快照文件
    // 对于第2种情况，理论不会发生，应当报错
    // 对于第3种情况，快照的版本一定大于或者等于chunk的版本
    if (nullptr != snapshot_ && metaPage_.sn <= snapshot_->GetSn()) {
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
    // 前面的逻辑保证了这里的sn一定是等于metaPage.sn的
    // 因为sn<metaPage_.sn时，请求会被拒绝
    // sn>metaPage_.sn时，前面会先更新metaPage.sn为sn
    // 又因为snapSn正常情况下是小于metaPage_.sn，所以snapSn也应当小于sn
    // 有几种情况可能出现metaPage_.sn <= snap.sn
    // 场景一：DataStore重启恢复历史日志，可能出现metaPage_.sn==snap.sn
    // 重启前有一个请求产生了快照文件，但是还没更新metapage时就重启了
    // 重启后又回放了先前的一个操作，这个操作的sn等于当前chunk的sn
    // 场景二：follower在做raft的恢复时通过leader下载raft快照
    // 下载过程中，Leader上的chunk也在做chunk的快照，下载以后follower再做日志恢复
    // 由于follower先下载chunk文件，后下载快照文件，所以此时metaPage_.sn<=snap.sn
    if (sn != metaPage_.sn || metaPage_.sn <= snapshot_->GetSn()) {
        LOG(WARNING) << "May be a log repaly opt after an unexpected restart."
                     << "Request sn: " << sn
                     << ", chunk sn: " << metaPage_.sn
                     << ", snapshot sn: " << snapshot_->GetSn();
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
    // 获取快照文件中未被拷贝过的区域
    uint32_t pageBeginIndex = offset / pageSize_;
    uint32_t pageEndIndex = (offset + length - 1) / pageSize_;
    std::vector<BitRange> uncopiedRange;
    std::shared_ptr<const Bitmap> snapBitmap = snapshot_->GetPageStatus();
    snapBitmap->Divide(pageBeginIndex,
                       pageEndIndex,
                       &uncopiedRange,
                       nullptr);

    CSErrorCode errorCode = CSErrorCode::Success;
    off_t copyOff;
    size_t copySize;
    // 将未拷贝过的区域从chunk文件读取出来，写入到snapshot文件
    for (auto& range : uncopiedRange) {
        copyOff = range.beginIndex * pageSize_;
        copySize = (range.endIndex - range.beginIndex + 1) * pageSize_;
        std::shared_ptr<char> buf(new char[copySize],
                                  std::default_delete<char[]>());
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
    // 如果快照文件被写过，需要调用Flush持久化metapage
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
    bool clearClone = false;
    for (auto pageIndex : dirtyPages_) {
        tempMeta.bitmap->Set(pageIndex);
    }
    if (isCloneChunk_) {
        // 如果所有的page都被写过,将Chunk标记为非clone chunk
        if (tempMeta.bitmap->NextClearBit(0) == Bitmap::NO_POS) {
            tempMeta.location = "";
            tempMeta.bitmap = nullptr;
            needUpdateMeta = true;
            clearClone = true;
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
        metaPage_.bitmap = tempMeta.bitmap;
        metaPage_.location = tempMeta.location;
        dirtyPages_.clear();
        if (clearClone) {
            if (metric_ != nullptr) {
                metric_->cloneChunkCount << -1;
            }
            isCloneChunk_ = false;
        }
    }
    return CSErrorCode::Success;
}

}  // namespace chunkserver
}  // namespace curve
