/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-09-18
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include <vector>
#include <string>

#include "curvefs/src/client/vfs/meta.h"
#include "curvefs/test/client/vfs/helper/helper.h"

namespace curvefs {
namespace client {
namespace vfs {

class MetaTest : public ::testing::Test {};

TEST_F(MetaTest, RootIno) {
    ASSERT_EQ(ROOT_INO, 1);
}

TEST_F(MetaTest, DirStream) {
    typedef struct {
        uint64_t ino;
        uint64_t fh;
        uint64_t offset;
    } dir_stream_t;

    DirStream dirStream{ 1, 2, 3 };
    dir_stream_t* dir_stream = reinterpret_cast<dir_stream_t*>(&dirStream);
    ASSERT_EQ(dir_stream->ino, 1);
    ASSERT_EQ(dir_stream->fh, 2);
    ASSERT_EQ(dir_stream->offset, 3);
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
