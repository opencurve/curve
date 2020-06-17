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

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include "src/chunkserver/raftsnapshot/curve_snapshot_writer.h"

namespace curve {
namespace chunkserver {

class CurveSnapshotWriterTest : public testing::Test {
};

TEST_F(CurveSnapshotWriterTest, success_normal_file) {
    auto fs = new braft::PosixFileSystemAdaptor();
    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }
    CurveSnapshotWriter writer("./data", fs);
    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
}

}  // namespace chunkserver
}  // namespace curve
