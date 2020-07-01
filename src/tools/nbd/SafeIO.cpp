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

#include "src/tools/nbd/SafeIO.h"
#include <cerrno>

#include "src/tools/nbd/util.h"

namespace curve {
namespace nbd {

ssize_t SafeIO::ReadExact(int fd, void* buf, size_t count) {
    return safe_read_exact(fd, buf, count);
}

ssize_t SafeIO::Read(int fd, void* buf, size_t count) {
    return safe_read(fd, buf, count);
}

ssize_t SafeIO::Write(int fd, const void* buf, size_t count) {
    return safe_write(fd, buf, count);
}

}  // namespace nbd
}  // namespace curve
