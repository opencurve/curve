/*
 *     Copyright (c) 2020 NetEase Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/**
 * Project: curve
 * Date: Fri Apr 24 12:35:49 CST 2020
 * Author: wuhanqing
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
