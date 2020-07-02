/**
 * Project: curve
 * Date: Sun Apr 26 19:35:18 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
 */

#ifndef TEST_TOOLS_NBD_MOCK_NBD_CONTROLLER_H_
#define TEST_TOOLS_NBD_MOCK_NBD_CONTROLLER_H_

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
    MOCK_METHOD4(SetUp, int(NBDConfig*, int, uint64_t, uint64_t));
    MOCK_METHOD1(DisconnectByPath, int(const std::string&));
};

}  // namespace nbd
}  // namespace curve

#endif  // TEST_TOOLS_NBD_MOCK_NBD_CONTROLLER_H_
