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
 * Date: Sun Apr 26 19:39:49 CST 2020
 * Author: wuhanqing
 */

#include "nbd/src/NBDWatchContext.h"
#include "nbd/test/mock_image_instance.h"
#include "nbd/test/mock_nbd_controller.h"

namespace curve {
namespace nbd {

using ::testing::_;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::Invoke;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

class NBDWatchContextTest : public ::testing::Test {
 public:
    void SetUp() override {
        image_ = std::make_shared<MockImageInstance>();
        nbdCtrl_ = std::make_shared<MockNBDController>();
        watchCtx_ =
            std::make_shared<NBDWatchContext>(nbdCtrl_, image_, defaultSize);
    }

 protected:
    std::shared_ptr<MockImageInstance> image_;
    std::shared_ptr<MockNBDController> nbdCtrl_;
    std::shared_ptr<NBDWatchContext> watchCtx_;
    uint64_t defaultSize = 10ull * 1024 * 1024 * 1024;
};

TEST_F(NBDWatchContextTest, ImageSizeChangeTest) {
    uint64_t newSize = defaultSize * 2;
    EXPECT_CALL(*image_, GetImageSize()).WillRepeatedly(Return(newSize));
    EXPECT_CALL(*nbdCtrl_, Resize(_)).Times(1);

    ASSERT_NO_THROW(watchCtx_->WatchImageSize());

    std::this_thread::sleep_for(std::chrono::seconds(3));

    ASSERT_NO_THROW(watchCtx_->StopWatch());
}

TEST_F(NBDWatchContextTest, GetImageSizeFailedTest) {
    EXPECT_CALL(*image_, GetImageSize()).WillRepeatedly(Return(-1));
    EXPECT_CALL(*nbdCtrl_, Resize(_)).Times(0);

    ASSERT_NO_THROW(watchCtx_->WatchImageSize());

    std::this_thread::sleep_for(std::chrono::seconds(3));

    ASSERT_NO_THROW(watchCtx_->StopWatch());
}

TEST_F(NBDWatchContextTest, GetImageSizeReturnZero) {
    EXPECT_CALL(*image_, GetImageSize()).WillRepeatedly(Return(0));
    EXPECT_CALL(*nbdCtrl_, Resize(_)).Times(0);

    ASSERT_NO_THROW(watchCtx_->WatchImageSize());

    std::this_thread::sleep_for(std::chrono::seconds(3));

    ASSERT_NO_THROW(watchCtx_->StopWatch());
}

}  // namespace nbd
}  // namespace curve
