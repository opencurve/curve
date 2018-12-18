/*
 * Project: curve
 * Created Date: 18-11-14
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include <brpc/server.h>
#include <gmock/gmock-generated-function-mockers.h>

#include <memory>
#include <cstdio>
#include <cstdlib>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/copyset_node.h"
#include "test/chunkserver/mock_cs_data_store.h"

namespace curve {
namespace chunkserver {

class FakeSnapshotReader : public braft::SnapshotReader {
 public:
    std::string get_path() {
        /* 返回一个不存在的 path */
        return std::string("/1002093939/temp/238408034");
    }
    void list_files(std::vector<std::string> *files) {
        return;
    }
    int load_meta(braft::SnapshotMeta *meta) {
        return 1;
    }
    std::string generate_uri_for_copy() {
        return std::string("");
    }
};

class FakeClosure : public braft::Closure {
 public:
    void Run() {
        std::cerr << "FakeClosure run" << std::endl;
    }
};

TEST(CopysetNodeTest, error_test) {
    int port = 9000;
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.ip = "127.0.0.1";
    copysetNodeOptions.port = port;
    copysetNodeOptions.snapshotIntervalS = 30;
    copysetNodeOptions.catchupMargin = 50;
    copysetNodeOptions.chunkDataUri = "local://.";
    copysetNodeOptions.chunkSnapshotUri = "local://.";
    copysetNodeOptions.logUri = "local://.";
    copysetNodeOptions.raftMetaUri = "local://.";
    copysetNodeOptions.raftSnapshotUri = "local://.";

    /* on_snapshot_save error branch - chunkDataApath_ 不存在*/
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::unique_ptr<CSDataStore> dsPtr = std::make_unique<CSDataStore>();
        CopysetNode copysetNode(logicPoolID, copysetID, conf, std::move(dsPtr));
        copysetNode.Init(copysetNodeOptions);
        FakeClosure done;
        ::system("rm -fr ./4294967297/data");
        copysetNode.on_snapshot_save(nullptr, &done);
        ASSERT_EQ(ENOENT, done.status().error_code());
    }
    /* on_snapshot_load - snapshot path 不存在 */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::unique_ptr<CSDataStore> dsPtr = std::make_unique<CSDataStore>();
        CopysetNode copysetNode(logicPoolID, copysetID, conf, std::move(dsPtr));
        copysetNode.Init(copysetNodeOptions);
        FakeSnapshotReader snapshotReader;
        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&snapshotReader));
    }
    /* on_snapshot_load - snapshot path 存在 */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::unique_ptr<CSDataStore> dsPtr = std::make_unique<CSDataStore>();
        CopysetNode copysetNode(logicPoolID, copysetID, conf, std::move(dsPtr));
        copysetNode.Init(copysetNodeOptions);
        FakeSnapshotReader snapshotReader;
        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&snapshotReader));
    }
    /* on_error */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::unique_ptr<CSDataStore> dsPtr = std::make_unique<CSDataStore>();
        CopysetNode copysetNode(logicPoolID, copysetID, conf, std::move(dsPtr));
        braft::Error error;
        copysetNode.on_error(error);
        ASSERT_EQ(1, 1);
    }
    /* Fini, raftNode is null */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::unique_ptr<CSDataStore> dsPtr = std::make_unique<CSDataStore>();
        CopysetNode copysetNode(logicPoolID, copysetID, conf, std::move(dsPtr));
        copysetNode.Fini();
    }
    /* Fini, rfatNode is not null */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::unique_ptr<CSDataStore> dsPtr = std::make_unique<CSDataStore>();
        CopysetNode copysetNode(logicPoolID, copysetID, conf, std::move(dsPtr));
        ASSERT_EQ(0, copysetNode.Init(copysetNodeOptions));
        copysetNode.Fini();
    }
}

}  // namespace chunkserver
}  // namespace curve
