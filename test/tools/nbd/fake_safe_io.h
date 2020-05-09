/**
 * Project: curve
 * Date: Fri Apr 24 12:35:49 CST 2020
 * Author: wuhanqing
 * Copyright (c) Netease
 */

#ifndef TEST_TOOLS_NBD_FAKE_SAFE_IO_H_
#define TEST_TOOLS_NBD_FAKE_SAFE_IO_H_

#include <functional>
#include "src/tools/nbd/SafeIO.h"

namespace curve {
namespace nbd {

using FuncType = std::function<ssize_t(int, void*, size_t)>;

class FakeSafeIO : public SafeIO {
 public:
    ssize_t ReadExact(int fd, void* buf, size_t count) override {
        return readExactTask_ ? readExactTask_(fd, buf, count) : -1;
    }

    ssize_t Read(int fd, void* buf, size_t count) override {
        return readTask_ ? readTask_(fd, buf, count) : -1;
    }

    ssize_t Write(int fd, const void* buf, size_t count) override {
        return writeTask_ ? writeTask_(fd, const_cast<void*>(buf), count) : -1;
    }

    void SetReadExactTask(FuncType task) {
        readExactTask_ = task;
    }

    void SetReadTask(FuncType task) {
        readTask_ = task;
    }

    void SetWriteTask(FuncType task) {
        writeTask_ = task;
    }

 private:
    FuncType readExactTask_;
    FuncType readTask_;
    FuncType writeTask_;
};

}  // namespace nbd
}  // namespace curve

#endif  // TEST_TOOLS_NBD_FAKE_SAFE_IO_H_
