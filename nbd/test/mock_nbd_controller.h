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
 * Date: Sun Apr 26 19:35:18 CST 2020
 * Author: wuhanqing
 */

#ifndef NBD_TEST_MOCK_NBD_CONTROLLER_H_
#define NBD_TEST_MOCK_NBD_CONTROLLER_H_

#include <gmock/gmock.h>
#include <string>
#include "nbd/src/NBDController.h"

namespace curve {
namespace nbd {

class MockNBDController : public NBDController {
 public:
    MockNBDController() = default;
    ~MockNBDController() = default;

    MOCK_METHOD1(Resize, int(uint64_t));
    MOCK_METHOD5(SetUp, int(NBDConfig*, int, uint64_t, uint32_t, uint64_t));
    MOCK_METHOD1(DisconnectByPath, int(const std::string&));
};

}  // namespace nbd
}  // namespace curve

#endif  // NBD_TEST_MOCK_NBD_CONTROLLER_H_
