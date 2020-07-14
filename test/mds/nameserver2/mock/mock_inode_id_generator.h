/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Wednesday September 12th 2018
 * Author: hzsunjianliang
 */

#ifndef  TEST_MDS_NAMESERVER2_MOCK_MOCK_INODE_ID_GENERATOR_H_
#define  TEST_MDS_NAMESERVER2_MOCK_MOCK_INODE_ID_GENERATOR_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "src/mds/nameserver2/idgenerator/inode_id_generator.h"

namespace curve {
namespace mds {

class MockInodeIDGenerator: public InodeIDGenerator {
 public:
    ~MockInodeIDGenerator() {}
    MOCK_METHOD1(GenInodeID, bool(InodeID *));
};
}  // namespace mds
}  // namespace curve
#endif   // TEST_MDS_NAMESERVER2_MOCK_MOCK_INODE_ID_GENERATOR_H_
