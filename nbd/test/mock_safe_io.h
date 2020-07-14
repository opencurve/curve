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
 * Date: Thu Apr 23 18:50:26 CST 2020
 * Author: wuhanqing
 */

#ifndef NBD_TEST_MOCK_SAFE_IO_H_
#define NBD_TEST_MOCK_SAFE_IO_H_

#include <gmock/gmock.h>

#include "nbd/src/SafeIO.h"

namespace curve {
namespace nbd {

class MockSafeIO : public SafeIO {
 public:
    MockSafeIO() = default;
    ~MockSafeIO() = default;

    MOCK_METHOD3(ReadExact, ssize_t(int, void*, size_t));
    MOCK_METHOD3(Read, ssize_t(int, void*, size_t));
    MOCK_METHOD3(Write, ssize_t(int, const void*, size_t));
};

}  // namespace nbd
}  // namespace curve

#endif  // NBD_TEST_MOCK_SAFE_IO_H_
