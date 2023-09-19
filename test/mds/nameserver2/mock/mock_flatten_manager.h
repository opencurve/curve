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
 * Created Date: 2023-08-18
 * Author: xuchaojie
 */

#ifndef TEST_MDS_NAMESERVER2_MOCK_MOCK_FLATTEN_MANAGER_H_
#define TEST_MDS_NAMESERVER2_MOCK_MOCK_FLATTEN_MANAGER_H_

#include <gmock/gmock.h>

#include <string>
#include <memory>

#include "src/mds/nameserver2/flatten_manager.h"

namespace curve {
namespace mds {
class MockFlattenManager : public FlattenManager {
 public:
    MOCK_METHOD4(SubmitFlattenJob, bool(uint64_t uniqueId,
                                        const std::string &fileName,
                                        const FileInfo &fileInfo,
                                        const FileInfo &snapFileInfo));

    MOCK_METHOD1(GetFlattenTask,
        std::shared_ptr<FlattenTask>(uint64_t uniqueId));
};

}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_NAMESERVER2_MOCK_MOCK_FLATTEN_MANAGER_H_
