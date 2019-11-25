/*
 * Project: curve
 * File Created: 2019-10-29
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#ifndef TEST_TOOLS_MOCK_SNAPSHOT_READ_H_
#define TEST_TOOLS_MOCK_SNAPSHOT_READ_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "src/tools/snapshot_read.h"

namespace curve {
namespace tool {
class MockSnapshotRead : public SnapshotRead {
 public:
    MockSnapshotRead() {}
    ~MockSnapshotRead() {}

    MOCK_METHOD2(Init, int(const std::string&, const std::string&));
    MOCK_METHOD0(UnInit, void());
    MOCK_METHOD3(Read, int(char*, off_t, size_t));
    MOCK_METHOD1(GetSnapshotInfo, void(SnapshotRepoItem*));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_SNAPSHOT_READ_H_
