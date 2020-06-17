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
 * Created Date: 2020-06-17
 * Author: charisu
 */

#ifndef TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_SNAPSHOT_ATTACHMENT_H_
#define TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_SNAPSHOT_ATTACHMENT_H_

#include <gmock/gmock.h>
#include <gmock/gmock-generated-function-mockers.h>
#include <gmock/internal/gmock-generated-internal-utils.h>
#include <string>
#include <vector>
#include "src/chunkserver/raftsnapshot/curve_snapshot_attachment.h"
#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

class MockSnapshotAttachment : public CurveSnapshotAttachment {
 public:
    MockSnapshotAttachment() : CurveSnapshotAttachment(nullptr) {}
    virtual ~MockSnapshotAttachment() {}
    MOCK_METHOD2(list_attach_files, void(std::vector<std::string>*,
                                 const std::string&));
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_RAFTSNAPSHOT_MOCK_SNAPSHOT_ATTACHMENT_H_
