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

#include <gtest/gtest.h>
#include <memory>
#include "src/chunkserver/trash.h"
#include "test/fs/mock_local_filesystem.h"
#include "test/chunkserver/datastore/mock_file_pool.h"
#include "src/chunkserver/copyset_node.h"

using ::testing::_;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::Mock;
using ::testing::Truly;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

using curve::fs::MockLocalFileSystem;

namespace curve {
namespace chunkserver {
class TrashTest : public ::testing::Test {
 protected:
    void SetUp() {
        lfs = std::make_shared<MockLocalFileSystem>();
        pool = std::make_shared<MockFilePool>(lfs);
        ops.localFileSystem = lfs;
        ops.chunkFilePool = pool;
        ops.trashPath = "local://./0/trash";
        ops.expiredAfterSec = 1;
        ops.scanPeriodSec = 1;

        trash = std::make_shared<Trash>();

        EXPECT_CALL(*lfs, List("./0/trash", _)).WillOnce(Return(0));
        trash->Init(ops);
        ASSERT_EQ(0, trash->GetChunkNum());
    }

 public:
    void RecycleCopyset50Times() {
        EXPECT_CALL(*lfs, DirExists(_)).WillRepeatedly(Return(false));
        EXPECT_CALL(*lfs, Mkdir(_)).WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs, Rename(_, _, 0)).WillRepeatedly(Return(0));
        for (int i = 0; i < 50; i++) {
            ASSERT_EQ(0, trash->RecycleCopySet("./0/copysets/12345678"));
        }
    }

    void CleanFiles() {
        std::vector<std::string> files{"12345678"};
        std::string trashPath = "./0/trash";
        std::string copysetDir = "./0/trash/12345678";
        std::vector<std::string> raftfiles{RAFT_LOG_DIR,
            RAFT_SNAP_DIR, RAFT_META_DIR, RAFT_DATA_DIR, "hello"};
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
        EXPECT_CALL(*lfs, Open(copysetdir, _))
            .WillOnce(Return(fd));
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
    EXPECT_CALL(*lfs, Open("./0/trash/4294967493.55555", _))
        .WillOnce(Return(-1));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_copysetDir_stat_err) {
    std::vector<std::string> files{"4294967493.55555"};
    EXPECT_CALL(*lfs, DirExists(_)).WillOnce(Return(true));
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, Open("./0/trash/4294967493.55555", _))
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
    EXPECT_CALL(*lfs, Open("./0/trash/4294967493.55555", _))
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
    EXPECT_CALL(*lfs, Open("./0/trash/4294967493.55555", _))
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
        .WillOnce(DoAll(SetArgPointee<1>(std::vector<std::string>{}),
                        Return(0)));
    EXPECT_CALL(*lfs, Open("./0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Delete("./0/trash/4294967493.55555"))
        .WillOnce(Return(-1));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_cleanCopySet_list_empty_delete_success) {
    std::vector<std::string> files{"4294967493.55555"};
    EXPECT_CALL(*lfs, DirExists(_)).Times(2).WillRepeatedly(Return(true));
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(std::vector<std::string>{}),
                        Return(0)));
    EXPECT_CALL(*lfs, Open("./0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Delete("./0/trash/4294967493.55555"))
        .WillOnce(Return(0));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, test_cleanCopySet_list_noEmpty_recycleChunks_list_err) {
    std::vector<std::string> files{"4294967493.55555"};
    std::vector<std::string> raftfiles{RAFT_LOG_DIR,
        RAFT_SNAP_DIR, RAFT_META_DIR, RAFT_DATA_DIR, "hello"};
    std::vector<std::string> empty;
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Open("./0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Delete("./0/trash/4294967493.55555"))
        .WillOnce(Return(-1));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, List("./0/trash", _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555", _))
        .WillOnce(DoAll(SetArgPointee<1>(raftfiles), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/log", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/raft_snapshot", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/raft_meta", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));


    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest,
    test_cleanCopySet_list_noEmpty_recycleChunks_list_empty_deleteOk) {
    std::vector<std::string> files{"4294967493.55555"};
    std::vector<std::string> raftfiles{RAFT_LOG_DIR,
        RAFT_SNAP_DIR, RAFT_META_DIR, RAFT_DATA_DIR, "hello"};
    std::vector<std::string> empty;
        EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Open("./0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Delete("./0/trash/4294967493.55555"))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs, List("./0/trash", _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555", _))
        .WillOnce(DoAll(SetArgPointee<1>(raftfiles), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/log", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/raft_snapshot", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/raft_meta", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest,
    test_cleanCopySet_list_noEmpty_recycleChunks_validFile_recycle) {
    std::vector<std::string> files{"4294967493.55555"};
    std::vector<std::string> raftfiles{RAFT_LOG_DIR,
        RAFT_SNAP_DIR, RAFT_META_DIR, RAFT_DATA_DIR, "hello"};
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
    EXPECT_CALL(*lfs, Open("./0/trash/4294967493.55555", _))
        .WillOnce(Return(10));
    struct stat info;
    time(&info.st_ctime);
    info.st_ctime -= ops.expiredAfterSec * 2 * 3600;
    EXPECT_CALL(*lfs, Fstat(10, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)));
    EXPECT_CALL(*lfs, Close(10)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Delete("./0/trash/4294967493.55555"))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs, List("./0/trash", _))
        .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555", _))
        .WillOnce(DoAll(SetArgPointee<1>(raftfiles), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/log", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(chunks), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/raft_snapshot", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/raft_meta", _))
        .WillOnce(DoAll(SetArgPointee<1>(empty), Return(0)));

    EXPECT_CALL(*pool,
        RecycleFile("./0/trash/4294967493.55555/data/chunk_123"))
        .WillOnce(Return(0));
    EXPECT_CALL(*pool,
        RecycleFile("./0/trash/4294967493.55555/data/chunk_345"))
        .WillOnce(Return(0));

    trash->DeleteEligibleFileInTrash();
}

TEST_F(TrashTest, recycle_copyset_dir_noExist_createErr) {
    std::string dirPath = "./0/copysets/12345678";
    std::string trashPath = "./0/trash";
    EXPECT_CALL(*lfs, DirExists(trashPath)).WillOnce(Return(false));
    EXPECT_CALL(*lfs, Mkdir(trashPath)).WillOnce(Return(-1));
    ASSERT_EQ(-1, trash->RecycleCopySet(dirPath));
}

TEST_F(TrashTest, recycle_copyset_dir_trash_exist) {
    std::string dirPath = "./0/copysets/12345678";
    std::string trashPath = "./0/trash";
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(false)).WillOnce(Return(true));
    EXPECT_CALL(*lfs, Mkdir(trashPath)).WillOnce(Return(0));
    ASSERT_EQ(-1, trash->RecycleCopySet(dirPath));
}

TEST_F(TrashTest, recycle_copyset_dir_rename_err) {
    std::string dirPath = "./0/copysets/12345678";
    std::string trashPath = "./0/trash";
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(false)).WillOnce(Return(false));
    EXPECT_CALL(*lfs, Mkdir(trashPath)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Rename(dirPath, _, 0)).WillOnce(Return(-1));
    ASSERT_EQ(-1, trash->RecycleCopySet(dirPath));
}

TEST_F(TrashTest, recycle_copyset_dir_list_err) {
    std::string dirPath = "./0/copysets/12345678";
    std::string trashPath = "./0/trash";
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(false)).WillOnce(Return(false));
    EXPECT_CALL(*lfs, Mkdir(trashPath)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Rename(dirPath, _, 0)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, List(_, _)).WillOnce(Return(-1));
    ASSERT_EQ(0, trash->RecycleCopySet(dirPath));
}

TEST_F(TrashTest, recycle_copyset_dir_ok) {
    std::string dirPath = "./0/copysets/12345678";
    std::string trashPath = "./0/trash";
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(false)).WillOnce(Return(false));
    EXPECT_CALL(*lfs, Mkdir(trashPath)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, Rename(dirPath, _, 0)).WillOnce(Return(0));
    EXPECT_CALL(*lfs, List(_, _)).WillOnce(Return(0));
    ASSERT_EQ(0, trash->RecycleCopySet(dirPath));
}

TEST_F(TrashTest, DISABLED_test_concurrenct) {
    std::thread thread1(&TrashTest::RecycleCopyset50Times, this);
    std::thread thread2(&TrashTest::CleanFiles, this);
    thread1.join();
    thread2.join();
}

TEST_F(TrashTest, test_chunk_num_statistic) {
    std::string trashPath = "./0/trash";
    std::vector<std::string> copysets{"4294967493.55555", "4294967494.55555"};
    std::vector<std::string> chunks1{"chunk_123",
                                     "chunk_345"};
    std::vector<std::string> chunks2{"chunk_111",
                                     "chunk_222",
                                     "chunk_222_snap_1"};

    // chunk exists when init
    EXPECT_CALL(*lfs, List(trashPath, _))
        .WillOnce(DoAll(SetArgPointee<1>(copysets), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(chunks1), Return(0)));
    EXPECT_CALL(*lfs, List("./0/trash/4294967494.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(chunks2), Return(0)));

    trash->Init(ops);
    ASSERT_EQ(5, trash->GetChunkNum());

    // recycle copyset
    std::string copysetDir = "./0/copysets/4294967495";
    EXPECT_CALL(*lfs, DirExists(_))
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    std::string trashedCopysetDir;
    EXPECT_CALL(*lfs, Rename(copysetDir, _, 0))
        .WillOnce(DoAll(SaveArg<1>(&trashedCopysetDir), Return(0)));
    std::vector<std::string> chunks3{"chunk_333", "chunk_444"};
    EXPECT_CALL(*lfs, List(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunks3), Return(0)));
    ASSERT_EQ(0, trash->RecycleCopySet(copysetDir));
    ASSERT_EQ(7, trash->GetChunkNum());

    std::string trashedCopysetName =
        trashedCopysetDir.substr(trashedCopysetDir.find_last_of("/") + 1);
    copysets.push_back(trashedCopysetName);

    // delete eligible copyset
    bool needDelete = true;
    bool notNeedDelete = false;
    std::vector<std::string> raftfiles{RAFT_DATA_DIR};

    EXPECT_CALL(*lfs, DirExists(trashPath)).WillOnce(Return(true));
    EXPECT_CALL(*lfs, DirExists("./0/trash/4294967493.55555"))
        .WillOnce(Return(true));
    EXPECT_CALL(*lfs, DirExists("./0/trash/4294967493.55555/data"))
        .WillOnce(Return(true));
    EXPECT_CALL(*lfs, DirExists("./0/trash/4294967493.55555/data/chunk_123"))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, DirExists("./0/trash/4294967493.55555/data/chunk_345"))
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs, List(trashPath, _))
        .WillOnce(DoAll(SetArgPointee<1>(copysets), Return(0)));

    SetCopysetNeedDelete(trashPath + "/" + copysets[0], needDelete);
    SetCopysetNeedDelete(trashPath + "/" + copysets[1], notNeedDelete);
    SetCopysetNeedDelete(trashPath + "/" + copysets[2], notNeedDelete);

    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555", _))
        .WillOnce(DoAll(SetArgPointee<1>(raftfiles), Return(0)));

    EXPECT_CALL(*lfs, List("./0/trash/4294967493.55555/data", _))
        .WillOnce(DoAll(SetArgPointee<1>(chunks1), Return(0)));
    EXPECT_CALL(*pool,
        RecycleFile("./0/trash/4294967493.55555/data/chunk_123"))
        .WillOnce(Return(0));
    EXPECT_CALL(*pool,
        RecycleFile("./0/trash/4294967493.55555/data/chunk_345"))
        .WillOnce(Return(-1));
    EXPECT_CALL(*lfs, Delete("./0/trash/4294967493.55555"))
        .Times(0);

    trash->DeleteEligibleFileInTrash();
    ASSERT_EQ(6, trash->GetChunkNum());
}

}  // namespace chunkserver
}  // namespace curve
