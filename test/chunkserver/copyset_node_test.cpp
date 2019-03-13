/*
 * Project: curve
 * Created Date: 18-11-14
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include <brpc/server.h>
#include <gmock/gmock-more-actions.h>
#include <gmock/gmock-generated-function-mockers.h>

#include <memory>
#include <cstdio>
#include <vector>
#include <string>
#include <cstdlib>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/copyset_node.h"
#include "test/chunkserver/fake_datastore.h"
#include "test/chunkserver/mock_local_file_system.h"
#include "src/chunkserver/conf_epoch_file.h"

namespace curve {
namespace chunkserver {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::InSequence;
using ::testing::AtLeast;
using ::testing::SaveArgPointee;

using curve::fs::FileSystemType;

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

class FakeSnapshotWriter : public braft::SnapshotWriter {
 public:
    std::string get_path() {
        /* 返回一个不存在的 path */
        return std::string(".");
    }
    void list_files(std::vector<std::string> *files) {
        return;
    }
    virtual int save_meta(const braft::SnapshotMeta &meta) {
        return 0;
    }

    virtual int add_file(const std::string &filename) {
        return 0;
    }

    virtual int add_file(const std::string &filename,
                         const ::google::protobuf::Message *file_meta) {
        return 0;
    }

    virtual int remove_file(const std::string &filename) {
        return 0;
    }
};

class FakeClosure : public braft::Closure {
 public:
    void Run() {
        std::cerr << "FakeClosure run" << std::endl;
    }
};

TEST(CopysetNodeTest, error_test) {
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    int port = 9000;
    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    std::string rmCmd("rm -f ");
    rmCmd += kCurveConfEpochFilename;
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
    copysetNodeOptions.maxChunkSize = kMaxChunkSize;
    copysetNodeOptions.concurrentapply = new ConcurrentApplyModule();
    copysetNodeOptions.concurrentapply->Init(2, 1);
    copysetNodeOptions.localFileSystem = fs;
    copysetNodeOptions.chunkfilePool =
        std::make_shared<ChunkfilePool>(fs);

    // on_snapshot_save: List failed
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::vector<std::string> files;
        files.push_back("test-1.txt");
        files.push_back("test-2.txt");

        char buff[128] = {0};
        ::snprintf(buff,
                   128,
                   ":%u:%u:%lu",
                   logicPoolID,
                   copysetID,
                   0);
        int writeLen = strlen(buff) + sizeof(size_t) + sizeof(uint32_t);

        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotWriter writer;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);

        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*mockfs, Write(_, _, _, _)).Times(1)
            .WillOnce(Return(writeLen));
        EXPECT_CALL(*mockfs, Fsync(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, Close(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, Rename(_, _)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, List(_, _)).Times(1).WillOnce(Return(-1));

        copysetNode.on_snapshot_save(&writer, &closure);
        LOG(INFO) << closure.status().error_cstr();
    }
    // on_snapshot_save: save conf success, rename failed
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::vector<std::string> files;
        files.push_back("test-1.txt");
        files.push_back("test-2.txt");

        char buff[128] = {0};
        ::snprintf(buff,
                   128,
                   ":%u:%u:%lu",
                   logicPoolID,
                   copysetID,
                   0);
        int writeLen = strlen(buff) + sizeof(size_t) + sizeof(uint32_t);

        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotWriter writer;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);

        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*mockfs, Write(_, _, _, _)).Times(1)
            .WillOnce(Return(writeLen));
        EXPECT_CALL(*mockfs, Fsync(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, Close(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, Rename(_, _)).Times(1).WillOnce(Return(-1));

        copysetNode.on_snapshot_save(&writer, &closure);
        LOG(INFO) << closure.status().error_cstr();
    }

    // on_snapshot_save: save conf open failed
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::vector<std::string> files;
        files.push_back("test-1.txt");
        files.push_back("test-2.txt");

        char buff[128] = {0};
        ::snprintf(buff,
                   128,
                   ":%u:%u:%lu",
                   logicPoolID,
                   copysetID,
                   0);
        int writeLen = strlen(buff) + sizeof(size_t) + sizeof(uint32_t);

        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotWriter writer;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);

        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1).WillOnce(Return(-1));

        copysetNode.on_snapshot_save(&writer, &closure);
        LOG(INFO) << closure.status().error_cstr();
    }
    // on_snapshot_save: success
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::vector<std::string> files;
        files.push_back("test-1.txt");
        files.push_back("test-2.txt");

        char buff[128] = {0};
        ::snprintf(buff,
                   128,
                   ":%u:%u:%lu",
                   logicPoolID,
                   copysetID,
                   0);
        int writeLen = strlen(buff) + sizeof(size_t) + sizeof(uint32_t);

        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotWriter writer;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);

        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*mockfs, Write(_, _, _, _)).Times(1)
            .WillOnce(Return(writeLen));
        EXPECT_CALL(*mockfs, Fsync(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, Close(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, Rename(_, _)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, List(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));

        copysetNode.on_snapshot_save(&writer, &closure);
    }
    // on_snapshot_load: Dir not exist, File not exist, data init success
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        DataStoreOptions options;
        options.baseDir = "./test-temp";
        options.chunkSize = 16 * 1024 * 1024;
        options.pageSize = 4 * 1024;
        std::shared_ptr<FakeCSDataStore> dataStore =
            std::make_shared<FakeCSDataStore>(options, fs);
        copysetNode.SetCSDateStore(dataStore);

        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(false));
        EXPECT_CALL(*mockfs, FileExists(_)).Times(1).WillOnce(Return(false));

        ASSERT_EQ(0, copysetNode.on_snapshot_load(&reader));
    }
    // on_snapshot_load: Dir not exist, File not exist, data init failed
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        DataStoreOptions options;
        options.baseDir = "./test-temp";
        options.chunkSize = 16 * 1024 * 1024;
        options.pageSize = 4 * 1024;
        std::shared_ptr<FakeCSDataStore> dataStore =
            std::make_shared<FakeCSDataStore>(options, fs);
        copysetNode.SetCSDateStore(dataStore);
        dataStore->InjectError();

        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(false));
        EXPECT_CALL(*mockfs, FileExists(_)).Times(1).WillOnce(Return(false));

        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&reader));
    }
    // on_snapshot_load: Dir not exist, File exist, load conf.epoch failed
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));

        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(false));
        EXPECT_CALL(*mockfs, FileExists(_)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1).WillOnce(Return(-1));

        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&reader));
    }

    // on_snapshot_load: Dir exist, List failed
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*mockfs, List(_, _)).Times(1).WillOnce(Return(-1));

        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&reader));
    }
    // on_snapshot_load: Dir exist, List success, rename success
    // file exist, open failed
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::vector<std::string> files;
        files.push_back("test-1.txt");

        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*mockfs, List(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
        EXPECT_CALL(*mockfs, Rename(_, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(*mockfs, FileExists(_)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1).WillOnce(Return(-1));

        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&reader));
    }
    // on_snapshot_load: Dir exist, List success, rename failed
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::vector<std::string> files;
        files.push_back("test-1.txt");

        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*mockfs, List(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
        EXPECT_CALL(*mockfs, Rename(_, _)).Times(1)
            .WillOnce(Return(1));

        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&reader));
    }
    /* on_error */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        braft::Error error;
        copysetNode.on_error(error);
        ASSERT_EQ(1, 1);
    }
    /* Fini, raftNode is null */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        copysetNode.Fini();
    }
    /* Fini, raftNode is not null */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        ASSERT_EQ(0, copysetNode.Init(copysetNodeOptions));
        copysetNode.Fini();
    }
    /* Load/SaveConfEpoch */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        copysetNodeOptions.localFileSystem = fs;
        ASSERT_EQ(0, copysetNode.Init(copysetNodeOptions));
        ASSERT_EQ(0, copysetNode.SaveConfEpoch(kCurveConfEpochFilename));
        ASSERT_EQ(0, copysetNode.LoadConfEpoch(kCurveConfEpochFilename));
        ASSERT_EQ(0, copysetNode.GetConfEpoch());
        copysetNode.Fini();
        ::system(rmCmd.c_str());
    }
    /* load: ConfEpochFile load failed*/
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        copysetNodeOptions.localFileSystem = fs;
        ASSERT_EQ(0, copysetNode.Init(copysetNodeOptions));
        ASSERT_NE(0, copysetNode.LoadConfEpoch(kCurveConfEpochFilename));
        copysetNode.Fini();
        ::system(rmCmd.c_str());
    }
    /* load: logic pool id 错误 */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        uint64_t epoch = 12;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        auto fs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        ConfEpochFile confEpochFile(fs);
        ASSERT_EQ(0,
                  confEpochFile.Save(kCurveConfEpochFilename,
                                     logicPoolID + 1,
                                     copysetID,
                                     epoch));
        copysetNodeOptions.localFileSystem = fs;
        ASSERT_EQ(0, copysetNode.Init(copysetNodeOptions));
        ASSERT_NE(0, copysetNode.LoadConfEpoch(kCurveConfEpochFilename));
        copysetNode.Fini();
        ::system(rmCmd.c_str());
    }
    /* load: copyset id 错误 */
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        uint64_t epoch = 12;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        auto fs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        ConfEpochFile confEpochFile(fs);
        ASSERT_EQ(0,
                  confEpochFile.Save(kCurveConfEpochFilename,
                                     logicPoolID,
                                     copysetID + 1,
                                     epoch));
        copysetNodeOptions.localFileSystem = fs;
        ASSERT_EQ(0, copysetNode.Init(copysetNodeOptions));
        ASSERT_NE(0, copysetNode.LoadConfEpoch(kCurveConfEpochFilename));
        copysetNode.Fini();
        ::system(rmCmd.c_str());
    }
}

}  // namespace chunkserver
}  // namespace curve
