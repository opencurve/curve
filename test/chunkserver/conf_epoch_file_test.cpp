/*
 * Project: curve
 * Created Date: 18-12-20
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>

#include <memory>

#include "src/chunkserver/conf_epoch_file.h"
#include "test/fs/mock_local_filesystem.h"
#include "src/fs/fs_common.h"
#include "src/chunkserver/copyset_node.h"

namespace curve {
namespace chunkserver {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;
using ::testing::SetArgReferee;
using ::testing::InSequence;
using ::testing::AtLeast;
using ::testing::SaveArgPointee;

using curve::fs::MockLocalFileSystem;
using curve::fs::FileSystemType;

TEST(ConfEpochFileTest, load_save) {
    LogicPoolID logicPoolID = 123;
    CopysetID copysetID = 1345;
    std::string path = kCurveConfEpochFilename;
    uint64_t epoch = 0;
    std::string rmCmd("rm -f ");
    rmCmd += kCurveConfEpochFilename;

    // normal load/save
    {
        auto fs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        ConfEpochFile confEpochFile(fs);
        ASSERT_EQ(0, confEpochFile.Save(path, logicPoolID, copysetID, epoch));

        LogicPoolID loadLogicPoolID;
        CopysetID loadCopysetID;
        uint64_t loadEpoch;
        ASSERT_EQ(0, confEpochFile.Load(path,
                                        &loadLogicPoolID,
                                        &loadCopysetID,
                                        &loadEpoch));
        ASSERT_EQ(logicPoolID, loadLogicPoolID);
        ASSERT_EQ(copysetID, loadCopysetID);
        ASSERT_EQ(epoch, loadEpoch);

        ::system(rmCmd.c_str());
    }

    // load: open failed
    {
        std::shared_ptr<MockLocalFileSystem> fs
            = std::make_shared<MockLocalFileSystem>();
        ConfEpochFile confEpochFile(fs);
        LogicPoolID loadLogicPoolID;
        CopysetID loadCopysetID;
        uint64_t loadEpoch;
        EXPECT_CALL(*fs, Open(_, _)).Times(1).WillOnce(Return(-1));
        ASSERT_EQ(-1, confEpochFile.Load(path,
                                        &loadLogicPoolID,
                                        &loadCopysetID,
                                        &loadEpoch));
    }
    // load: open success, read failed
    {
        std::shared_ptr<MockLocalFileSystem> fs
            = std::make_shared<MockLocalFileSystem>();
        ConfEpochFile confEpochFile(fs);
        LogicPoolID loadLogicPoolID;
        CopysetID loadCopysetID;
        uint64_t loadEpoch;
        EXPECT_CALL(*fs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*fs, Read(_, _, _, _)).Times(1).WillOnce(Return(-1));
        EXPECT_CALL(*fs, Close(_)).Times(1).WillOnce(Return(0));
        ASSERT_EQ(-1, confEpochFile.Load(path,
                                         &loadLogicPoolID,
                                         &loadCopysetID,
                                         &loadEpoch));
    }
    // load: open success, read success, decode success, crc32c right
    {
        char *json = "{\"logicPoolId\":123,\"copysetId\":1345,\"epoch\":0,\"checksum\":599727352}";  // NOLINT
        std::string jsonStr(json);
        std::shared_ptr<MockLocalFileSystem> fs
            = std::make_shared<MockLocalFileSystem>();
        ConfEpochFile confEpochFile(fs);
        LogicPoolID loadLogicPoolID;
        CopysetID loadCopysetID;
        uint64_t loadEpoch;
        EXPECT_CALL(*fs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*fs, Read(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArrayArgument<1>(json, json + jsonStr.size()),
                            Return(jsonStr.size())));
        EXPECT_CALL(*fs, Close(_)).Times(1).WillOnce(Return(0));
        ASSERT_EQ(0, confEpochFile.Load(path,
                                         &loadLogicPoolID,
                                         &loadCopysetID,
                                         &loadEpoch));
    }
    // load: open success, read success, decode failed, crc32c right
    {
        char *json = "{\"logicPoolId";
        std::string jsonStr(json);
        std::shared_ptr<MockLocalFileSystem> fs
            = std::make_shared<MockLocalFileSystem>();
        ConfEpochFile confEpochFile(fs);
        LogicPoolID loadLogicPoolID;
        CopysetID loadCopysetID;
        uint64_t loadEpoch;
        EXPECT_CALL(*fs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*fs, Read(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArrayArgument<1>(json, json + jsonStr.size()),
                            Return(jsonStr.size())));
        EXPECT_CALL(*fs, Close(_)).Times(1).WillOnce(Return(0));
        ASSERT_EQ(-1, confEpochFile.Load(path,
                                         &loadLogicPoolID,
                                         &loadCopysetID,
                                         &loadEpoch));
    }
    // load: open success, read success, decode success, crc32c not right
    {
        char *json = "{\"logicPoolId\":123,\"copysetId\":1345,\"epoch\":0,\"checksum\":123}";  // NOLINT
        std::string jsonStr(json);
        std::shared_ptr<MockLocalFileSystem> fs
            = std::make_shared<MockLocalFileSystem>();
        ConfEpochFile confEpochFile(fs);
        LogicPoolID loadLogicPoolID;
        CopysetID loadCopysetID;
        uint64_t loadEpoch;
        EXPECT_CALL(*fs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*fs, Read(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArrayArgument<1>(json, json + jsonStr.size()),
                            Return(jsonStr.size())));
        EXPECT_CALL(*fs, Close(_)).Times(1).WillOnce(Return(0));
        ASSERT_EQ(-1, confEpochFile.Load(path,
                                        &loadLogicPoolID,
                                        &loadCopysetID,
                                        &loadEpoch));
    }
    // save: open failed
    {
        std::shared_ptr<MockLocalFileSystem> fs
            = std::make_shared<MockLocalFileSystem>();
        ConfEpochFile confEpochFile(fs);
        LogicPoolID loadLogicPoolID;
        CopysetID loadCopysetID;
        uint64_t loadEpoch;
        EXPECT_CALL(*fs, Open(_, _)).Times(1).WillOnce(Return(-1));
        ASSERT_EQ(-1, confEpochFile.Save(path,
                                         loadLogicPoolID,
                                         loadCopysetID,
                                         loadEpoch));
    }
    // save: open success, write failed
    {
        std::shared_ptr<MockLocalFileSystem> fs
            = std::make_shared<MockLocalFileSystem>();
        ConfEpochFile confEpochFile(fs);
        LogicPoolID loadLogicPoolID;
        CopysetID loadCopysetID;
        uint64_t loadEpoch;
        EXPECT_CALL(*fs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*fs, Write(_, _, _, _)).Times(1)
            .WillOnce(Return(-1));
        EXPECT_CALL(*fs, Close(_)).Times(1).WillOnce(Return(0));
        ASSERT_EQ(-1, confEpochFile.Save(path,
                                         loadLogicPoolID,
                                         loadCopysetID,
                                         loadEpoch));
    }
    // save: open success, write success, fsync failed
    {
        char *json = "{\"logicPoolId\":123,\"copysetId\":1345,\"epoch\":0,\"checksum\":599727352}";  // NOLINT
        std::string jsonStr(json);
        std::shared_ptr<MockLocalFileSystem> fs
            = std::make_shared<MockLocalFileSystem>();
        ConfEpochFile confEpochFile(fs);
        EXPECT_CALL(*fs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*fs, Write(_, _, _, _)).Times(1)
            .WillOnce(Return(jsonStr.size()));
        EXPECT_CALL(*fs, Close(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*fs, Fsync(_)).Times(1).WillOnce(Return(-1));
        ASSERT_EQ(-1, confEpochFile.Save(path,
                                         logicPoolID,
                                         copysetID,
                                         epoch));
    }
}

}  // namespace chunkserver
}  // namespace curve
