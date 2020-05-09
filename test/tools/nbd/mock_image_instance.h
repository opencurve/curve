/**
 * Project: curve
 * Date: Fri Apr 24 09:41:49 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
 */

#ifndef TEST_TOOLS_NBD_MOCK_IMAGE_INSTANCE_H_
#define TEST_TOOLS_NBD_MOCK_IMAGE_INSTANCE_H_

#include <gmock/gmock.h>
#include "src/tools/nbd/ImageInstance.h"

namespace curve {
namespace nbd {

class MockImageInstance : public ImageInstance {
 public:
    MockImageInstance() : ImageInstance("/test_image") {}
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

#endif  // TEST_TOOLS_NBD_MOCK_IMAGE_INSTANCE_H_
