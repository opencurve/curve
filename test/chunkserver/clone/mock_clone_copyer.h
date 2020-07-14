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
 * Created Date: Saturday March 30th 2019
 * Author: yangyaokai
 */

#ifndef TEST_CHUNKSERVER_CLONE_MOCK_CLONE_COPYER_H_
#define TEST_CHUNKSERVER_CLONE_MOCK_CLONE_COPYER_H_

#include <gmock/gmock.h>
#include <string>

#include "src/chunkserver/clone_copyer.h"

namespace curve {
namespace chunkserver {

class DownloadClosure;
class MockChunkCopyer : public OriginCopyer {
 public:
    MockChunkCopyer() = default;
    ~MockChunkCopyer() = default;
    MOCK_METHOD1(DownloadAsync, void(DownloadClosure*));
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_CLONE_MOCK_CLONE_COPYER_H_
