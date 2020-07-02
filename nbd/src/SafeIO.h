/**
 * Project: curve
 * Date: Fri Apr 24 12:35:49 CST 2020
 * Copyright (c) Netease
 */

#ifndef SRC_TOOLS_NBD_SAFEIO_H_
#define SRC_TOOLS_NBD_SAFEIO_H_

#include <cstddef>
#include <cstdio>

namespace curve {
namespace nbd {

// 封装safe_read/write接口
class SafeIO {
 public:
    SafeIO() = default;
    virtual ~SafeIO() = default;

    virtual ssize_t ReadExact(int fd, void* buf, size_t count);
    virtual ssize_t Read(int fd, void* buf, size_t count);
    virtual ssize_t Write(int fd, const void* buf, size_t count);
};

}  // namespace nbd
}  // namespace curve

#endif  // SRC_TOOLS_NBD_SAFEIO_H_
