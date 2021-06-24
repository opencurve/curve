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
 * Date: Fri Apr 24 09:41:49 CST 2020
 * Author: wuhanqing
 */

#ifndef NBD_TEST_MOCK_IMAGE_INSTANCE_H_
#define NBD_TEST_MOCK_IMAGE_INSTANCE_H_

#include <gmock/gmock.h>
#include "nbd/src/ImageInstance.h"

namespace curve {
namespace nbd {

class MockImageInstance : public ImageInstance {
 public:
    MockImageInstance() : ImageInstance("/test_image", nullptr) {}
    ~MockImageInstance() = default;

    MOCK_METHOD0(Open, bool());
    MOCK_METHOD0(Close, void());
    MOCK_METHOD1(AioRead, void(NebdClientAioContext*));
    MOCK_METHOD1(AioWrite, void(NebdClientAioContext*));
    MOCK_METHOD1(Trim, void(NebdClientAioContext*));
    MOCK_METHOD1(Flush, void(NebdClientAioContext*));
    MOCK_METHOD0(GetImageSize, int64_t());
};

}  // namespace nbd
}  // namespace curve

#endif  // NBD_TEST_MOCK_IMAGE_INSTANCE_H_
