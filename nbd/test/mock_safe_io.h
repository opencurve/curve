/**
 * Project: curve
 * Date: Thu Apr 23 18:50:26 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
 */

#ifndef TEST_TOOLS_NBD_MOCK_SAFE_IO_H_
#define TEST_TOOLS_NBD_MOCK_SAFE_IO_H_

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

#endif  // TEST_TOOLS_NBD_MOCK_SAFE_IO_H_
