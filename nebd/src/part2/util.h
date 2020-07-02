/*
 * Project: nebd
 * Created Date: Sunday January 19th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_UTIL_H_
#define SRC_PART2_UTIL_H_

#include <string>
#include <mutex>  // NOLINT
#include <ostream>

#include "nebd/src/part2/define.h"

namespace nebd {
namespace server {

NebdFileType GetFileType(const std::string& fileName);

std::string NebdFileType2Str(NebdFileType type);

std::string NebdFileStatus2Str(NebdFileStatus status);

std::ostream& operator<<(std::ostream& os, const NebdServerAioContext& c);
std::ostream& operator<<(std::ostream& os, const NebdFileMeta& meta);

bool operator==(const NebdFileMeta& lMeta, const NebdFileMeta& rMeta);
bool operator!=(const NebdFileMeta& lMeta, const NebdFileMeta& rMeta);

class FdAllocator {
 public:
    FdAllocator() : fd_(0) {}
    ~FdAllocator() {}

    // fd的有效值范围为[1, INT_MAX]
    int GetNext();
    // 初始化fd的值
    void InitFd(int fd);

 private:
    std::mutex mtx_;
    int fd_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_UTIL_H_
