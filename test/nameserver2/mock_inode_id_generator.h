/*
 * Project: curve
 * Created Date: Wednesday September 12th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef  TEST_NAMESERVER2_MOCK_INODE_ID_GENERATOR_H_
#define  TEST_NAMESERVER2_MOCK_INODE_ID_GENERATOR_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "src/nameserver2/inode_id_generator.h"

namespace curve {
namespace mds {

class MockInodeIDGenerator: public InodeIDGenerator {
 public:
    ~MockInodeIDGenerator() {}
    MOCK_METHOD1(GenInodeID, bool(InodeID *));
};
}  // namespace mds
}  // namespace curve
#endif   // TEST_NAMESERVER2_MOCK_INODE_ID_GENERATOR_H_
