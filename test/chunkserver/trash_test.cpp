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
 * Created Date: Mon Apr 27th 2019
 * Author: lixiaocui
 */

#include "src/chunkserver/trash.h"

#include <gtest/gtest.h>

#include <memory>

#include "src/chunkserver/copyset_node.h"
#include "test/chunkserver/datastore/mock_file_pool.h"
#include "test/fs/mock_local_filesystem.h"

using ::testing::_;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Mock;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;
using ::testing::Truly;

using curve::fs::MockLocalFileSystem;

namespace curve {
namespace chunkserver {
class TrashTest : public ::testing::Test {
 protected:
    void SetUp() {
        lfs = std::make_shared<MockLocalFileSystem>();
        pool = std::make_shared<MockFilePool>(lfs);
        walPool = std::make_shared<MockFilePool>(lfs);
        ops.localFileSystem = lfs;
        ops.chunkFilePool = pool;
        ops.walPool = walPool;
        ops.trashPath = "local://./runlog/trash_test0/trash";
        ops.expiredAfterSec = 1;
        ops.scanPeriodSec = 1;

        trash = std::make_shared<Trash>();

        EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash", _))
            .WillOnce(Return(0));
        trash->Init(ops);
        ASSERT_EQ(0, trash->GetChunkNum());
    }

 public:
    void RecycleCopyset50Times() {
        EXPECT_CALL(*lfs, DirExists(_)).WillRepeatedly(Return(false));
        EXPECT_CALL(*lfs, Mkdir(_)).WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs, Rename(_, _, 0)).WillRepeatedly(Return(0));
        for (int i = 0; i < 50; i++) {
            ASSERT_EQ(0, trash->RecycleCopySet(
                             "./runlog/trash_test0/copysets/12345678"));
        }
    }

    void CleanFiles() {
        std::vector<std::string> files{"12345678"};
        std::string trashPath = "./runlog/trash_test0/trash";
        std::string copysetDir = "./runlog/trash_test0/trash/12345678";
        std::vector<std::string> raftfiles{
            RAFT_LOG_DIR, RAFT_SNAP_DIR, RAFT_META_DIR, RAFT_DATA_DIR, "hello"};
        std::string log = copysetDir + RAFT_LOG_DIR;
        std::string snap = copysetDir + RAFT_SNAP_DIR;
        std::string meta = copysetDir + RAFT_META_DIR;
        std::string data = copysetDir + RAFT_DATA_DIR;
        std::vector<std::string> chunks{"chunk_123", "chunk_345"};
        std::string chunks1 = data + chunks[0];
        std::string chunks2 = data + chunks[1];
        EXPECT_CALL(*lfs, DirExists(trashPath)).WillRepeatedly(Return(true));
        EXPECT_CALL(*lfs, List(trashPath, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(files), Return(0)));
        EXPECT_CALL(*lfs, List(copysetDir, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(raftfiles), Return(0)));
        EXPECT_CALL(*lfs, Open(copysetDir, _)).WillRepeatedly(Return(10));
        struct stat info;
        time(&info.st_ctime);
        info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
        EXPECT_CALL(*lfs, Fstat(10, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(info), Return(0)));
        EXPECT_CALL(*lfs, Close(10)).WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs, Delete(log)).WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs, Delete(snap)).WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs, Delete(meta)).WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs, List(data, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(chunks), Return(0)));
        EXPECT_CALL(*pool, RecycleFile(chunks1)).WillRepeatedly(Return(-1));
        EXPECT_CALL(*pool, RecycleFile(chunks2)).WillRepeatedly(Return(0));
        for (int i = 0; i < 50; i++) {
            trash->DeleteEligibleFileInTrash();
        }
    }

    void SetCopysetNeedDelete(const std::string& copysetdir, bool needDelete) {
        int fd = needDelete ? 1 : 2;
        EXPECT_CALL(*lfs, Open(copysetdir, _)).WillOnce(Return(fd));
        struct stat info;
        time(&info.st_ctime);
        if (needDelete) {
            info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
        }
        EXPECT_CALL(*lfs, Fstat(fd, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(info), Return(0)));
        EXPECT_CALL(*lfs, Close(fd)).WillRepeatedly(Return(0));
    }

 protected:
    std::shared_ptr<Trash> trash;
    std::shared_ptr<MockLocalFileSystem> lfs;
    std::shared_ptr<MockFilePool> pool;
    std::shared_ptr<MockFilePool> walPool;
    TrashOptions ops;
};

TEST_F(TrashTest, test_trashDir_not_exist) {
    EXPECT_CALL(*lfs, DirExists(_)).WillOnce(Return(false));
    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_list_trashPath_err) {
    EXPECT_CALL(*lfs, DirExists(_)).WillOnce(Return(true));
    EXPECT_CALL(*lfs, List(_, _)).WillOnce(Return(-1));
    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_copysetDir_open_fail) {
    std::vector<std::string> files{"hello", "4294967493.55555", "2234"};
    EXPECT_CALL(*lfs, DirExists(_)).WillOnce(Return(true));
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(-1));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_copysetDir_stat_err) {
    std::vector<std::string> files{"4294967493.55555"};
    EXPECT_CALL(*lfs, DirExists(_)).WillOnce(Return(true));
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    EXPECT_CALL(*lfs, Fstat(10, _)).WillOnce(Return(-1));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_copysetDir_timeNotEnough) {
    std::vector<std::string> files{"4294967493.55555"};
    EXPECT_CALL(*lfs, DirExists(_)).WillOnce(Return(true));
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_cleanCopySet_list_err) {
    std::vector<std::string> files{"4294967493.55555"};
    EXPECT_CALL(*lfs, DirExists(_)).Times(2).WillRepeatedly(Return(true));
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)))
        .WillOnce(Return(-1));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_cleanCopySet_list_empty_delete_err) {
    std::vector<std::string> files{"4294967493.55555"};
    EXPECT_CALL(*lfs, DirExists(_)).Times(2).WillRepeatedly(Return(true));
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)))
        .WillOnce(
            DoAll(SetArgPointee<1>(std::vector<std::string>{}), Return(0)));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Delete("./runlog/trash_test0/trash/4294967493.55555"))
        .WillOnce(Return(-1));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_cleanCopySet_list_empty_delete_success) {
    std::vector<std::string> files{"4294967493.55555"};
    EXPECT_CALL(*lfs, DirExists(_)).Times(2).WillRepeatedly(Return(true));
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)))
        .WillOnce(
            DoAll(SetArgPointee<1>(std::vector<std::string>{}), Return(0)));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Delete("./runlog/trash_test0/trash/4294967493.55555"))
        .WillOnce(Return(0));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_cleanCopySet_list_noEmpty_recycleChunks_list_err) {
    std::vector<std::string> files{"4294967493.55555"};
    std::vector<std::string> raftfiles{RAFT_LOG_DIR, RAFT_SNAP_DIR,
                                       RAFT_META_DIR, RAFT_DATA_DIR, "hello"};
    std::vector<std::string> empty;
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Delete("./runlog/trash_test0/trash/4294967493.55555"))
        .WillOnce(Return(-1));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash", _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(DoAll(SetArgPointee<1>(raftfiles), Return(0)));
    EXPECT_CALL(*lfs,
                List("./runlog/trash_test0/trash/4294967493.55555/log", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs,
                List("./runlog/trash_test0/trash/4294967493.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(
        *lfs,
        List("./runlog/trash_test0/trash/4294967493.55555/raft_snapshot", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(
        *lfs, List("./runlog/trash_test0/trash/4294967493.55555/raft_meta", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest,
       test_cleanCopySet_list_noEmpty_recycleChunks_list_empty_deleteOk) {
    std::vector<std::string> files{"4294967493.55555"};
    std::vector<std::string> raftfiles{RAFT_LOG_DIR, RAFT_SNAP_DIR,
                                       RAFT_META_DIR, RAFT_DATA_DIR, "hello"};
    std::vector<std::string> empty;
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Delete("./runlog/trash_test0/trash/4294967493.55555"))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash", _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(DoAll(SetArgPointee<1>(raftfiles), Return(0)));
    EXPECT_CALL(*lfs,
                List("./runlog/trash_test0/trash/4294967493.55555/log", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs,
                List("./runlog/trash_test0/trash/4294967493.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(
        *lfs,
        List("./runlog/trash_test0/trash/4294967493.55555/raft_snapshot", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(
        *lfs, List("./runlog/trash_test0/trash/4294967493.55555/raft_meta", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest,
       test_cleanCopySet_list_noEmpty_recycleChunks_validFile_recycle) {
    std::vector<std::string> files{"4294967493.55555"};
    std::vector<std::string> raftfiles{RAFT_LOG_DIR, RAFT_SNAP_DIR,
                                       RAFT_META_DIR, RAFT_DATA_DIR, "hello"};
    std::vector<std::string> chunks{"chunk_123", "chunk_345"};
    std::vector<std::string> empty;
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Delete("./runlog/trash_test0/trash/4294967493.55555"))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash", _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(DoAll(SetArgPointee<1>(raftfiles), Return(0)));
    EXPECT_CALL(*lfs,
                List("./runlog/trash_test0/trash/4294967493.55555/log", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs,
                List("./runlog/trash_test0/trash/4294967493.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(chunks), Return(0)));
    EXPECT_CALL(
        *lfs,
        List("./runlog/trash_test0/trash/4294967493.55555/raft_snapshot", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(
        *lfs, List("./runlog/trash_test0/trash/4294967493.55555/raft_meta", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));

    EXPECT_CALL(
        *pool,
        RecycleFile(
            "./runlog/trash_test0/trash/4294967493.55555/data/chunk_123"))
        .WillOnce(Return(0));
    EXPECT_CALL(
        *pool,
        RecycleFile(
            "./runlog/trash_test0/trash/4294967493.55555/data/chunk_345"))
        .WillOnce(Return(0));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, recycle_wal_success) {
    std::vector<std::string> files{"4294967493.55555"};
    std::vector<std::string> raftfiles{RAFT_LOG_DIR, RAFT_SNAP_DIR,
                                       RAFT_META_DIR, RAFT_DATA_DIR, "hello"};
    std::vector<std::string> logfiles{
        "curve_log_10086_10087", "curve_log_inprogress_10088",
        "log_10083_10084", "log_inprogress_10085"};
    std::vector<std::string> empty;
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Delete("./runlog/trash_test0/trash/4294967493.55555"))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash", _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(DoAll(SetArgPointee<1>(raftfiles), Return(0)));
    EXPECT_CALL(*lfs,
                List("./runlog/trash_test0/trash/4294967493.55555/log", _))
        .WillOnce(DoAll(SetArgPointee<1>(logfiles), Return(0)));
    EXPECT_CALL(*lfs,
                List("./runlog/trash_test0/trash/4294967493.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(
        *lfs,
        List("./runlog/trash_test0/trash/4294967493.55555/raft_snapshot", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(
        *lfs, List("./runlog/trash_test0/trash/4294967493.55555/raft_meta", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));

    EXPECT_CALL(*walPool,
                RecycleFile("./runlog/trash_test0/trash/4294967493.55555/log/"
                            "curve_log_10086_10087"))
        .WillOnce(Return(0));
    EXPECT_CALL(*walPool,
                RecycleFile("./runlog/trash_test0/trash/4294967493.55555/log/"
                            "curve_log_inprogress_10088"))
        .WillOnce(Return(0));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, recycle_wal_failed) {
    std::vector<std::string> files{"4294967493.55555"};
    std::vector<std::string> raftfiles{RAFT_LOG_DIR, RAFT_SNAP_DIR,
                                       RAFT_META_DIR, RAFT_DATA_DIR, "hello"};
    std::vector<std::string> logfiles{
        "curve_log_10086_10087", "curve_log_inprogress_10088",
        "log_10083_10084", "log_inprogress_10085"};
    std::vector<std::string> empty;
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Open("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash", _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, List("./runlog/trash_test0/trash/4294967493.55555", _))
        .WillOnce(DoAll(SetArgPointee<1>(raftfiles), Return(0)));
    EXPECT_CALL(*lfs,
                List("./runlog/trash_test0/trash/4294967493.55555/log", _))
        .WillOnce(DoAll(SetArgPointee<1>(logfiles), Return(0)));
    EXPECT_CALL(*lfs,
                List("./runlog/trash_test0/trash/4294967493.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(
        *lfs,
        List("./runlog/trash_test0/trash/4294967493.55555/raft_snapshot", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(
        *lfs, List("./runlog/trash_test0/trash/4294967493.55555/raft_meta", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));

    EXPECT_CALL(*walPool,
                RecycleFile("./runlog/trash_test0/trash/4294967493.55555/log/"
                            "curve_log_10086_10087"))
        .WillOnce(Return(0));
    EXPECT_CALL(*walPool,
                RecycleFile("./runlog/trash_test0/trash/4294967493.55555/log/"
                            "curve_log_inprogress_10088"))
        .WillOnce(Return(-1));

    //失败的情况下不应删除
    EXPECT_CALL(*lfs, Delete("./runlog/trash_test0/trash/4294967493.55555"))
        .Times(0);

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, recycle_copyset_dir_noExist_createErr) {
    std::string dirPath = "./runlog/trash_test0/copysets/12345678";
    std::string trashPath = "./runlog/trash_test0/trash";
    EXPECT_CALL(*lfs, DirExists(trashPath)).WillOnce(Return(false));
    EXPECT_CALL(*lfs, Mkdir(trashPath)).WillOnce(Return(-1));
    ASSERT_EQ(-1, trash->RecycleCopySet(dirPath));
}

TEST_F(TrashTest, recycle_copyset_dir_trash_exist) {
    std::string dirPath = "./runlog/trash_test0/copysets/12345678";
    std::string trashPath = "./runlog/trash_test0/trash";
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(false))
        .WillOnce(Return(true));
    EXPECT_CALL(*lfs, Mkdir(trashPath)).WillOnce(Return(0));
    ASSERT_EQ(-1, trash->RecycleCopySet(dirPath));
}

TEST_F(TrashTest, recycle_copyset_dir_rename_err) {
    std::string dirPath = "./runlog/trash_test0/copysets/12345678";
    std::string trashPath = "./runlog/trash_test0/trash";
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(false))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Mkdir(trashPath)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Rename(dirPath, _, 0)).WillOnce(Return(-1));
    ASSERT_EQ(-1, trash->RecycleCopySet(dirPath));
}

TEST_F(TrashTest, recycle_copyset_dir_list_err) {
    std::string dirPath = "./runlog/trash_test0/copysets/12345678";
    std::string trashPath = "./runlog/trash_test0/trash";
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(false))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Mkdir(trashPath)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Rename(dirPath, _, 0)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(0, trash->RecycleCopySet(dirPath));
}

TEST_F(TrashTest, recycle_copyset_dir_ok) {
    std::string dirPath = "./runlog/trash_test0/copysets/12345678";
    std::string trashPath = "./runlog/trash_test0/trash";
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(false))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Mkdir(trashPath)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Rename(dirPath, _, 0)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(Return(0));
    ASSERT_EQ(0, trash->RecycleCopySet(dirPath));
}

TEST_F(TrashTest, DISABLED_test_concurrenct) {
    std::thread thread1(&TrashTest::RecycleCopyset50Times, this);
    std::thread thread2(&TrashTest::CleanFiles, this);
    thread1.join();
    thread2.join();
}

TEST_F(TrashTest, test_chunk_num_statistic) {
    std::string trashPath = "./runlog/trash_test0/trash";
    std::vector<std::string> copysets{"4294967493.55555", "4294967494.55555"};
    std::vector<std::string> dirs{"data", "log"};
    std::vector<std::string> chunks1{"chunk_100", "chunk_101"};
    std::vector<std::string> chunks2{"chunk_200_snap_1", "abc"};
    std::vector<std::string> logfiles1{
        "curve_log_10086_10087", "curve_log_inprogress_10088",
        "log_10083_10084", "log_inprogress_10085"};
    std::vector<std::string> logfiles2{};

    // (1) chunk exists when init
    //  trash/
    //       4294967493.55555/
    //                       data/
    //                           chunk_100, chunk_101           +2
    //                       log/
    //                          curve_log_10086_10087,          +1
    //                          curve_log_inprogress_10088,     +1
    //                          log_10083_10084,
    //                          log_inprogress_10085
    //       4294967494.55555/
    //                       data/
    //                           chunk_200_snap_1, abc          +1
    //                       log/

    using item4list = struct{
        std::string subdir;
        std::vector<std::string>& names;
    };
    std::vector<item4list> action4List{
        { "", copysets },
        { "/4294967493.55555", dirs},
        { "/4294967493.55555/data", chunks1 },
        { "/4294967493.55555/log", logfiles1 },
        { "/4294967494.55555", dirs},
        { "/4294967494.55555/data", chunks2 },
        { "/4294967494.55555/log", logfiles2 },
    };

    for (auto& it : action4List) {
        EXPECT_CALL(*lfs, List(trashPath + it.subdir, _))
            .WillOnce(DoAll(SetArgPointee<1>(it.names), Return(0)));
    }

    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(true))      // data
        .WillOnce(Return(false))     // chunk_100
        .WillOnce(Return(false))     // chunk_101
        .WillOnce(Return(true))      // log
        .WillOnce(Return(false))     // curve_log_10086_10087
        .WillOnce(Return(false))     // curve_log_inprogress_10088_10088
        .WillOnce(Return(false))     // log_10083_10084
        .WillOnce(Return(false))     // log_inprogress_10085
        .WillOnce(Return(true))      // data
        .WillOnce(Return(false))     // chunk_200_snap_1
        .WillOnce(Return(false))     // abc
        .WillOnce(Return(true));     // log

    trash->Init(ops);
    ASSERT_EQ(5, trash->GetChunkNum());

    // (2) recycle copyset
    // trash_test0/copysets/4294967495/
    //                                data/
    //                                    chunk_333, chunk_444     +2
    //                                log/
    //                                   curve_log_10086_10087    +1
    //                                raft_snapshot/temp/data/
    //                                                       chunk_10000 +1
    //
    std::string copysetDir = "/copysets/4294967495";
    std::vector<std::string> dirs2{"data", "log", "raft_snapshot"};
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(true))
        .WillOnce(Return(false))
        .WillOnce(Return(true))   // data
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(true))   // log
        .WillOnce(Return(false))
        .WillOnce(Return(true))   // raft_snapshot
        .WillOnce(Return(true))   // temp
        .WillOnce(Return(true))   // data
        .WillOnce(Return(false));

    std::string trashedCopysetDir = "/trash_test0/copysets/4294967495";
    EXPECT_CALL(*lfs, Rename(copysetDir, _, 0))
        .WillOnce(DoAll(SaveArg<1>(&trashedCopysetDir), Return(0)));
    std::vector<std::string> chunks3{"chunk_333", "chunk_444"};
    std::vector<std::string> logfiles3{"curve_log_10086_10087"};
    std::vector<std::string> temp{"temp"};
    std::vector<std::string> data{"data"};
    std::vector<std::string> chunks4{"chunk_10000"};
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(dirs2), Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(chunks3), Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(logfiles3), Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(temp), Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(data), Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(chunks4), Return(0)));
    ASSERT_EQ(0, trash->RecycleCopySet(copysetDir));
    ASSERT_EQ(9, trash->GetChunkNum());

    // (3) delete eligible copyset
    std::string trashedCopysetName =
        trashedCopysetDir.substr(trashedCopysetDir.find_last_of("/") + 1);
    copysets.push_back(trashedCopysetName);

    bool needDelete = true;
    bool notNeedDelete = false;
    std::vector<std::string> raftfiles{RAFT_DATA_DIR, RAFT_LOG_DIR};

    // DirExists
    using item4dirExists = struct{
        std::string subdir;
        bool exist;
    };
    std::vector<item4dirExists> action4DirExists{
        { "", true },
        { "/4294967493.55555", true },
        { "/4294967493.55555/data", true },
        { "/4294967493.55555/log", true },
        { "/4294967493.55555/data/chunk_100", false },
        { "/4294967493.55555/data/chunk_101", false },
        { "/4294967493.55555/log/curve_log_10086_10087", false },
        { "/4294967493.55555/log/curve_log_inprogress_10088", false },
        { "/4294967493.55555/log/log_10083_10084", false },
        { "/4294967493.55555/log/log_inprogress_10085", false },
    };

    for (auto& it : action4DirExists) {
        EXPECT_CALL(*lfs, DirExists(trashPath + it.subdir))
            .WillOnce(Return(it.exist));
    }

    // List
    std::vector<item4list> action4List2{
        { "", copysets },
        { "/4294967493.55555", raftfiles },
        { "/4294967493.55555/data", chunks1 },
        { "/4294967493.55555/log", logfiles1 },
    };

    for (auto& it : action4List2) {
        EXPECT_CALL(*lfs, List(trashPath + it.subdir, _))
            .WillOnce(DoAll(SetArgPointee<1>(it.names), Return(0)));
    }

    SetCopysetNeedDelete(trashPath + "/" + copysets[0], needDelete);
    SetCopysetNeedDelete(trashPath + "/" + copysets[1], notNeedDelete);
    SetCopysetNeedDelete(trashPath + "/" + copysets[2], notNeedDelete);

    // RecycleFile
    using item4CycleFile = struct{
        std::shared_ptr<MockFilePool> pool;
        std::string subdir;
        int ret;
    };
    std::vector<item4CycleFile> action4CycleFile{
        { pool, "/4294967493.55555/data/chunk_100", 0 },
        { pool, "/4294967493.55555/data/chunk_101", -1 },
        { walPool, "/4294967493.55555/log/curve_log_10086_10087", 0 },
        { walPool, "/4294967493.55555/log/curve_log_inprogress_10088", -1 },
    };

    for (auto& it : action4CycleFile) {
        EXPECT_CALL(*(it.pool), RecycleFile(trashPath + it.subdir))
            .WillOnce(Return(it.ret));
    }

    // Delete
    EXPECT_CALL(*lfs, Delete("./runlog/trash_test0/trash/4294967493.55555"))
        .Times(0);

    trash->DeleteEligibleFileInTrash();
    ASSERT_EQ(7, trash->GetChunkNum());
}

}  // namespace chunkserver
}  // namespace curve
