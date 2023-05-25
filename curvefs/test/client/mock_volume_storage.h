/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Thursday Mar 24 09:53:32 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_CLIENT_MOCK_VOLUME_STORAGE_H_
#define CURVEFS_TEST_CLIENT_MOCK_VOLUME_STORAGE_H_

#include <gmock/gmock.h>

#include "curvefs/src/client/volume/volume_storage.h"
#include "curvefs/src/client/filesystem/meta.h"

namespace curvefs {
namespace client {

using ::curvefs::client::filesystem::FileOut;

class MockVolumeStorage : public VolumeStorage {
 public:
    MOCK_METHOD4(Read, CURVEFS_ERROR(uint64_t, off_t, size_t, char*));
    MOCK_METHOD5(Write,
                 CURVEFS_ERROR(uint64_t, off_t, size_t, const char*, FileOut*));
    MOCK_METHOD1(Flush, CURVEFS_ERROR(uint64_t));
    MOCK_METHOD0(Shutdown, bool());
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_VOLUME_STORAGE_H_
