/*************************************************************************
> File Name: test_snapshot_uuidgen.cpp
> Author:
> Created Time: Sat 29 Dec 2018 01:15:12 AM CST
> Copyright (c) 2018 netease
 ************************************************************************/

#include "src/snapshot/UUID_generator.h"
#include <gtest/gtest.h>  //NOLINT
#include <gmock/gmock.h>  //NOLINT

namespace curve {
namespace snapshotserver {

class TestUUIDGenerator : public ::testing::Test {
 public:
    TestUUIDGenerator() {}
    virtual ~TestUUIDGenerator() {}

    void SetUp() {}
    void TearDown() {}
};
TEST_F(TestUUIDGenerator, testIDgen) {
    UUIDGenerator idGen;

    std::string data1 = idGen.GenerateUUID();
    std::string data2 = idGen.GenerateUUIDRandom();
    std::string data3 = idGen.GenerateUUIDTime();
    std::string data4 = idGen.GenerateUUIDTimeSafe();
}
}  // namespace snapshotserver
}  // namespace curve
