/*
 * Project: curve
 * Created Date: 2020-06-17
 * Author: charisu
 * Copyright (c) 2018 netease
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
