/*
 * Project: curve
 * Created Date: 18-12-20
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/conf_epoch_file.h"

#include "src/common/crc32.h"

namespace curve {
namespace chunkserver {

int ConfEpochFile::Load(const std::string &path,
                        LogicPoolID *logicPoolID,
                        CopysetID *copysetID,
                        uint64_t *epoch) {
    int fd = fs_->Open(path.c_str(), O_RDWR);
    if (0 > fd) {
        LOG(ERROR) << "LoadConfEpoch failed open file " << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        return -1;
    }

    /* 1. 解析 head  */
    size_t kHeadLength = sizeof(size_t) + sizeof(uint32_t);
    char head[kHeadLength];
    ::memset(head, 0, kHeadLength);
    if (kHeadLength != fs_->Read(fd, head, 0, kHeadLength)) {
        LOG(ERROR) << "LoadConfEpoch failed read head of" << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        fs_->Close(fd);
        return -1;
    }
    size_t len = *reinterpret_cast<size_t *>(head);
    uint32_t loadCrc32 = *reinterpret_cast<uint32_t *>(head + sizeof(size_t));

    /* 2. 解析出 epoch 值 */
    char buff[128];
    ::memset(buff, 0, 128);
    if ((len - sizeof(uint32_t))
        != fs_->Read(fd, buff, kHeadLength, len - sizeof(uint32_t))) {
        LOG(ERROR) << "LoadConfEpoch failed read data of " << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        fs_->Close(fd);
        return -1;
    }

    uint32_t crc32 = curve::common::CRC32(buff, len - sizeof(uint32_t));
    if (loadCrc32 != crc32) {
        LOG(ERROR) << "conf.epoch crc error";
        fs_->Close(fd);
        return -1;
    }

    int ret = ::sscanf(buff,
                       ":%u:%u:%lu",
                       logicPoolID,
                       copysetID,
                       epoch);
    if (3 != ret) {
        LOG(ERROR) << "fail LoadConfEpoch when sscanf"
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno) << " " << ret;
        fs_->Close(fd);
        return -1;
    }

    fs_->Close(fd);
    return 0;
}

int ConfEpochFile::Save(const std::string &path,
                        const LogicPoolID logicPoolID,
                        const CopysetID copysetID,
                        const uint64_t epoch) {
    int fd = fs_->Open(path.c_str(), O_RDWR | O_CREAT);
    if (0 > fd) {
        LOG(ERROR) << "LoadConfEpoch failed open file " << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        return -1;
    }

    /* 1. 格式化 conf epoch */
    char buff[128];
    ::memset(buff, 0, 128);
    if (0 > ::snprintf(buff,
                       128,
                       ":%u:%u:%lu",
                       logicPoolID,
                       copysetID,
                       epoch)) {
        LOG(ERROR) << "fail SaveConfEpoch when sprintf"
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        fs_->Close(fd);
        return -1;
    }

    /* 2. 格式化 head */
    char head[sizeof(size_t)];
    ::memset(head, 0, sizeof(size_t));
    size_t len = sizeof(uint32_t) + strlen(buff);
    ::memcpy(head, &len, sizeof(size_t));

    /* 3. 持久化 */
    char epochBuff[128];
    ::memset(epochBuff, 0, 128);
    /* 拷贝 len */
    ::memcpy(epochBuff, head, sizeof(size_t));
    /* 拷贝 crc32 */
    uint32_t crc32 = curve::common::CRC32(buff, len - sizeof(uint32_t));
    ::memcpy(epochBuff + sizeof(size_t),
             &crc32,
             sizeof(uint32_t));
    /* 拷贝数据 */
    ::memcpy(epochBuff + sizeof(size_t) + sizeof(uint32_t),
             buff,
             len - sizeof(uint32_t));

    if ((sizeof(size_t) + len)
        != fs_->Write(fd, epochBuff, 0, sizeof(size_t) + len)) {
        LOG(ERROR) << "SaveConfEpoch write failed, path: " << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        fs_->Close(fd);
        return -1;
    }

    /* 4. 落盘 */
    if (0 != fs_->Fsync(fd)) {
        LOG(ERROR) << "SaveConfEpoch sync failed, path: " << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        fs_->Close(fd);
        return -1;
    }
    fs_->Close(fd);

    return 0;
}

}  // namespace chunkserver
}  // namespace curve
