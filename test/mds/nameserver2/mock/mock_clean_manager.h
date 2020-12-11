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
 * Created Date: Tuesday December 11th 2018
 * Author: hzsunjianliang
 */

#ifndef TEST_MDS_NAMESERVER2_MOCK_MOCK_CLEAN_MANAGER_H_
#define TEST_MDS_NAMESERVER2_MOCK_MOCK_CLEAN_MANAGER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include "src/mds/nameserver2/clean_manager.h"
#include "src/mds/nameserver2/async_delete_snapshot_entity.h"


namespace curve {
namespace mds {
class MockCleanManager: public CleanManagerInterface {
 public:
    ~MockCleanManager() {}
    MOCK_METHOD2(SubmitDeleteSnapShotFileJob, bool(const FileInfo&,
        std::shared_ptr<AsyncDeleteSnapShotEntity>));
    MOCK_METHOD1(GetTask, std::shared_ptr<Task>(TaskIDType id));
    MOCK_METHOD1(SubmitDeleteCommonFileJob, bool(const FileInfo&));
    MOCK_METHOD2(SubmitCleanDiscardSegmentJob,
                 bool(const std::string&, const DiscardSegmentInfo&));
};

}  // namespace mds
}  // namespace curve


#endif  // TEST_MDS_NAMESERVER2_MOCK_MOCK_CLEAN_MANAGER_H_
