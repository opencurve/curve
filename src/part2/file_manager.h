/*
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_FILE_MANAGER_H_
#define SRC_PART2_FILE_MANAGER_H_

#include <unordered_map>
#include <memory>
#include <thread>  // NOLINT
#include <string>

#include "src/part2/define.h"
#include "src/part2/metafile_manager.h"

namespace nebd {
namespace server {

using MetaFileManagerPtr = std::shared_ptr<NebdMetaFileManager>;

struct NebdFileManagerOption {
    uint32_t heartbeatTimeoutS;
    MetaFileManagerPtr metaFileManager;
};

class NebdFileManager {
 public:
    NebdFileManager();
    virtual ~NebdFileManager();
    virtual int Init(NebdFileManagerOption option);
    virtual int Load();
    virtual int UpdateFileTimestamp(int fd);
    virtual int Open(const std::string& filename);
    virtual int Close(int fd);
    virtual int Extend(int fd, int64_t newsize);
    virtual int GetInfo(int fd, NebdFileInfo* fileInfo);
    virtual int StatFile(int fd, NebdFileInfo* fileInfo);
    virtual int Discard(int fd, NebdServerAioContext* aioctx);
    virtual int AioRead(int fd, NebdServerAioContext* aioctx);
    virtual int AioWrite(int fd, NebdServerAioContext* aioctx);
    virtual int Flush(int fd, NebdServerAioContext* aioctx);
    virtual int InvalidCache(int fd);

 private:
    void CheckTimeoutFunc();

 private:
    using FileRecordMap =
        std::unordered_map<int, std::shared_ptr<NebdFileRecord>>;
    FileRecordMap fileRecordMap_;
    MetaFileManagerPtr metaFileManager_;
    uint32_t heartbeatTimeoutS_;
    std::thread checkTimeoutThread_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_FILE_MANAGER_H_
