/*
 * Project: curve
 * Created Date: Monday October 15th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef TEST_NAMESERVER2_MOCK_CHUNK_ID_GENERATOR_H_
#define TEST_NAMESERVER2_MOCK_CHUNK_ID_GENERATOR_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "src/nameserver2/chunk_id_generator.h"

namespace curve {
namespace mds {

class MockChunkIDGenerator: public ChunkIDGenerator {
 public:
    ~MockChunkIDGenerator() {}
    MOCK_METHOD1(GenChunkID, bool(ChunkID *));
};

}  // namespace mds
}  // namespace curve
#endif  // TEST_NAMESERVER2_MOCK_CHUNK_ID_GENERATOR_H_
