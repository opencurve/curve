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
 */

#ifndef NBD_SRC_SAFEIO_H_
#define NBD_SRC_SAFEIO_H_

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

#endif  // NBD_SRC_SAFEIO_H_
