/*
 * Project: curve
 * Created Date: Wednesday November 28th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/chunkserver_snapshot.h"

namespace curve {
namespace chunkserver {

void SnapshotMetaPage::encode(char* buf) {
    size_t len = 0;
    memcpy(buf, &version, sizeof(version));
    len += sizeof(version);
    memcpy(buf + len, &damaged, sizeof(damaged));
    len += sizeof(damaged);
    memcpy(buf + len, &sn, sizeof(sn));
    len += sizeof(sn);
    uint32_t bits = bitmap->Size();
    memcpy(buf + len, &bits, sizeof(bits));
    len += sizeof(bits);
    size_t bitmapBytes = (bits + 8 - 1) / 8;
    memcpy(buf + len, bitmap->GetBitmap(), bitmapBytes);
    len += bitmapBytes;
    uint32_t crc = ::curve::common::CRC32(buf, len);
    memcpy(buf + len, &crc, sizeof(crc));
}

CSErrorCode SnapshotMetaPage::decode(const char* buf) {
    size_t len = 0;
    memcpy(&version, buf, sizeof(version));
    len += sizeof(version);
    memcpy(&damaged, buf + len, sizeof(damaged));
    len += sizeof(damaged);
    memcpy(&sn, buf + len, sizeof(sn));
    len += sizeof(sn);
    uint32_t bits = 0;
    memcpy(&bits, buf + len, sizeof(bits));
    len += sizeof(bits);
    bitmap = std::make_shared<Bitmap>(bits, buf + len);
    size_t bitmapBytes = (bitmap->Size() + 8 - 1) / 8;
    len += bitmapBytes;
    uint32_t crc =  ::curve::common::CRC32(buf, len);
    uint32_t recordCrc;
    memcpy(&recordCrc, buf + len, sizeof(recordCrc));
    // 校验crc，校验失败返回错误码
    if (crc != recordCrc) {
        LOG(ERROR) << "Checking Crc32 failed.";
        return CSErrorCode::CrcCheckError;
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

SnapshotMetaPage::SnapshotMetaPage(const SnapshotMetaPage& metaPage) {
    version = metaPage.version;
    damaged = metaPage.damaged;
    sn = metaPage.sn;
    std::shared_ptr<Bitmap> newMap =
        std::make_shared<Bitmap>(metaPage.bitmap->Size(),
                                 metaPage.bitmap->GetBitmap());
    bitmap = newMap;
}

SnapshotMetaPage& SnapshotMetaPage::operator =(
    const SnapshotMetaPage& metaPage) {
    if (this == &metaPage)
        return *this;
    version = metaPage.version;
    damaged = metaPage.damaged;
    sn = metaPage.sn;
    std::shared_ptr<Bitmap> newMap =
        std::make_shared<Bitmap>(metaPage.bitmap->Size(),
                                 metaPage.bitmap->GetBitmap());
    bitmap = newMap;
    return *this;
}

CSSnapshot::CSSnapshot(std::shared_ptr<LocalFileSystem> lfs,
                       std::shared_ptr<ChunkfilePool> ChunkfilePool,
                       const ChunkOptions& options)
    : fd_(-1),
      chunkId_(options.id),
      size_(options.chunkSize),
      pageSize_(options.pageSize),
      baseDir_(options.baseDir),
      lfs_(lfs),
      chunkfilePool_(ChunkfilePool) {
    CHECK(!baseDir_.empty()) << "Create snapshot failed";
    CHECK(lfs_ != nullptr) << "Create snapshot failed";
    uint32_t bits = size_ / pageSize_;
    metaPage_.bitmap = std::make_shared<Bitmap>(bits);
    metaPage_.sn = options.sn;
}

CSSnapshot::~CSSnapshot() {
    if (fd_ >= 0) {
        lfs_->Close(fd_);
    }
}

CSErrorCode CSSnapshot::Open(bool createFile) {
    string snapshotPath = path();
    // 创建新文件,如果快照文件已经存在则不用再创建
    // 快照文件存在可能由以下情况引起:
    // getchunk成功，但是后面stat或者loadmetapage时失败，下载再open的时候；
    if (createFile
        && !lfs_->FileExists(snapshotPath)
        && metaPage_.sn > 0) {
        char buf[pageSize_] = {0};
        metaPage_.encode(buf);
        int ret = chunkfilePool_->GetChunk(snapshotPath, buf);
        if (ret != 0) {
            LOG(ERROR) << "Error occured when create snapshot."
                   << " filepath = " << snapshotPath;
            return CSErrorCode::InternalError;
        }
    }
    int rc = lfs_->Open(snapshotPath, O_RDWR|O_NOATIME|O_DSYNC);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when opening file."
                   << " filepath = "<< snapshotPath;
        return CSErrorCode::InternalError;
    }
    fd_ = rc;
    struct stat fileInfo;
    rc = lfs_->Fstat(fd_, &fileInfo);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when stating file."
                   << " filepath = " << snapshotPath;
        return CSErrorCode::InternalError;
    }
    if (fileInfo.st_size != fileSize()) {
        LOG(ERROR) << "Wrong file size."
                   << " filepath = " << snapshotPath
                   << ",filesize = " << fileInfo.st_size;
        return CSErrorCode::FileFormatError;
    }
    return loadMetaPage();
}

CSErrorCode CSSnapshot::Read(char * buf, off_t offset, size_t length) {
    // TODO(yyk) 是否需要对比偏移对应bit状态
    int rc = readData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when reading snapshot."
                   << " filepath = "<< path();
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::Delete() {
    if (fd_ >= 0) {
        lfs_->Close(fd_);
        fd_ = -1;
    }
    int ret = chunkfilePool_->RecycleChunk(path());
    if (ret < 0)
        return CSErrorCode::InternalError;
    return CSErrorCode::Success;
}

SequenceNum CSSnapshot::GetSn() const {
    return metaPage_.sn;
}

std::shared_ptr<const Bitmap> CSSnapshot::GetPageStatus() const {
    return metaPage_.bitmap;
}

CSErrorCode CSSnapshot::Write(const char * buf, off_t offset, size_t length) {
    int rc = writeData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Write snapshot failed."
                   << "ChunkID: " << chunkId_
                   << ",snapshot sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    uint32_t pageBeginIndex = offset / pageSize_;
    uint32_t pageEndIndex = (offset + length - 1) / pageSize_;
    for (uint32_t i = pageBeginIndex; i <= pageEndIndex; ++i) {
        dirtyPages_.insert(i);
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::Flush() {
    SnapshotMetaPage tempMeta = metaPage_;
    for (auto pageIndex : dirtyPages_) {
        tempMeta.bitmap->Set(pageIndex);
    }
    CSErrorCode errorCode = updateMetaPage(&tempMeta);
    if (errorCode == CSErrorCode::Success)
        metaPage_.bitmap = tempMeta.bitmap;
    dirtyPages_.clear();
    return errorCode;
}

CSErrorCode CSSnapshot::updateMetaPage(SnapshotMetaPage* metaPage) {
    char buf[pageSize_] = {0};
    metaPage->encode(buf);
    int rc = writeMetaPage(buf);
    if (rc < 0) {
        LOG(ERROR) << "Update metapage failed."
                   << "ChunkID: " << chunkId_
                   << ",snapshot sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::loadMetaPage() {
    char buf[pageSize_] = {0};
    int rc = readMetaPage(buf);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when reading metaPage_."
                   << " filepath = " << path();
        return CSErrorCode::InternalError;
    }
    return metaPage_.decode(buf);
}

}  // namespace chunkserver
}  // namespace curve
