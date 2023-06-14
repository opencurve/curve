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
 * Created Date: 2023-06-28
 * Author: xuchaojie
 */

#ifndef TEST_MDS_NAMESERVER2_MOCK_MOCK_CLONE_ID_GENERATOR_H_
#define TEST_MDS_NAMESERVER2_MOCK_MOCK_CLONE_ID_GENERATOR_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "src/mds/nameserver2/idgenerator/clone_id_generator.h"

namespace curve {
namespace mds {

class MockCloneIDGenerator: public CloneIDGenerator {
 public:
    ~MockCloneIDGenerator() {}
    MOCK_METHOD1(GenCloneID, bool(uint64_t *));
};

}  // namespace mds
}  // namespace curve


#endif  // TEST_MDS_NAMESERVER2_MOCK_MOCK_CLONE_ID_GENERATOR_H_
