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
 * Created Date: Tue Nov  3 22:23:04 CST 2020
 * Author: wuhanqing
 */

#ifndef TEST_MDS_NAMESERVER2_MOCK_MOCK_FILE_RECORD_MANAGER_H_
#define TEST_MDS_NAMESERVER2_MOCK_MOCK_FILE_RECORD_MANAGER_H_

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "src/mds/nameserver2/file_record.h"

namespace curve {
namespace mds {

class MockFileRecordManager : public FileRecordManager {
 public:
    MockFileRecordManager() = default;
    ~MockFileRecordManager() = default;

    MOCK_CONST_METHOD0(GetFileRecordExpiredTimeUs, uint32_t());
    MOCK_CONST_METHOD2(FindFileMountPoint,
                       bool(const std::string&, std::vector<butil::EndPoint>*));
};

}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_NAMESERVER2_MOCK_MOCK_FILE_RECORD_MANAGER_H_
