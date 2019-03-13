/*
 * Project: curve
 * Created Date: Saturday March 30th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#ifndef TEST_CHUNKSERVER_CLONE_MOCK_CLONE_COPYER_H_
#define TEST_CHUNKSERVER_CLONE_MOCK_CLONE_COPYER_H_

#include <gmock/gmock.h>
#include <string>

#include "src/chunkserver/clone_copyer.h"

namespace curve {
namespace chunkserver {

class MockChunkCopyer : public OriginCopyer {
 public:
    MockChunkCopyer() = default;
    ~MockChunkCopyer() = default;
    MOCK_METHOD4(Download, int(const string&, off_t, size_t, char*));
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_CLONE_MOCK_CLONE_COPYER_H_
