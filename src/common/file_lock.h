/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#ifndef SRC_COMMON_FILE_LOCK_H_
#define SRC_COMMON_FILE_LOCK_H_

#include <string>

namespace nebd {
namespace common {

// 文件锁
class FileLock {
 public:
    explicit FileLock(const std::string fileName) : fileName_(fileName),
                                                    fd_(-1) {}
    FileLock() : fileName_(""), fd_(-1) {}
    ~FileLock() = default;

    /**
     * @brief 获取文件锁
     * @return 成功返回0，失败返回-1
     */
    int AcquireFileLock();


    /**
     * @brief 释放文件锁
     */
    void ReleaseFileLock();

 private:
    // 锁文件的文件名
    std::string fileName_;
    // 锁文件的fd
    int fd_;
};

}  // namespace common
}  // namespace nebd

#endif  // SRC_COMMON_FILE_LOCK_H_
