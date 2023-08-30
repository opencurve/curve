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
 * File Created: Tuesday, 11th June 2019 5:17:55 pm
 * Author: tongguangxun
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <braft/snapshot.h>
#include <butil/memory/ref_counted.h>

#include <memory>

#include "src/fs/local_filesystem.h"
#include "test/fs/mock_local_filesystem.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/chunkserver/raftsnapshot/curve_filesystem_adaptor.h"
#include "src/chunkserver/raftsnapshot/define.h"

using ::testing::_;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Mock;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnPointee;
using ::testing::NotNull;
using ::testing::StrEq;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::ReturnArg;
using ::testing::SetArgReferee;
using ::testing::AtLeast;

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::chunkserver::FilePool;
using curve::fs::MockLocalFileSystem;
namespace curve {
namespace chunkserver {

static const char* kFilePool = "./raftsnap/chunkfilepool/";
static const char* kFilePoolMeta = "./raftsnap/chunkfilepool.meta";

class RaftSnapshotFilesystemAdaptorMockTest : public testing::Test {
 public:
    void SetUp() {
        fsptr = curve::fs::LocalFsFactory::CreateFs(
                        curve::fs::FileSystemType::EXT4, "/dev/sda");
        FilePoolPtr_ = std::make_shared<FilePool>(fsptr);
        ASSERT_TRUE(FilePoolPtr_);

        int count = 1;
        fsptr->Mkdir("./raftsnap");
        fsptr->Mkdir("./raftsnap/chunkfilepool");
        while (count < 4) {
            std::string filename = kFilePool + std::to_string(count);
            int fd = fsptr->Open(filename, O_RDWR | O_CREAT);
            ASSERT_GE(fd, 0);
            char data[8192];
            memset(data, 'a', 8192);
            fsptr->Write(fd, data, 0, 8192);
            fsptr->Close(fd);
            count++;
        }

        uint32_t chunksize = 4096;
        uint32_t metapagesize = 4096;

        FilePoolOptions cpopt;
        cpopt.getFileFromPool = true;
        cpopt.fileSize = chunksize;
        cpopt.metaPageSize = metapagesize;
        cpopt.metaFileSize = 4096;
        cpopt.blockSize = 4096;
        strcpy(cpopt.filePoolDir, kFilePool);   // NOLINT(runtime/printf)
        strcpy(cpopt.metaPath, kFilePoolMeta);  // NOLINT(runtime/printf)

        FilePoolMeta meta;
        meta.chunkSize = chunksize;
        meta.metaPageSize = metapagesize;
        meta.hasBlockSize = true;
        meta.blockSize = 4096;
        meta.filePoolPath = kFilePool;

        ASSERT_EQ(0, FilePoolHelper::PersistEnCodeMetaInfo(fsptr, meta,
                                                           kFilePoolMeta))
            << "Persist chunkfile pool meta info failed";

        lfs = std::make_shared<curve::fs::MockLocalFileSystem>();

        rfa = new CurveFilesystemAdaptor(FilePoolPtr_, lfs);
        std::vector<std::string> filterList;
        std::string snapshotMeta(BRAFT_SNAPSHOT_META_FILE);
        filterList.push_back(snapshotMeta);
        rfa->SetFilterList(filterList);

        ASSERT_TRUE(FilePoolPtr_->Initialize(cpopt));
        scoped_refptr<braft::FileSystemAdaptor> scptr(rfa);

        FilePoolPtr_->SetLocalFileSystem(lfs);

        fsadaptor.swap(scptr);
        fsadaptor->AddRef();
    }

    void TearDown() {
        std::vector<std::string> filename;
        fsptr->List("./raftsnap/FilePool", &filename);
        for (auto iter : filename) {
            auto path = "./raftsnap/chunkfilepool/" + iter;
            int err = fsptr->Delete(path.c_str());
            if (err) {
                LOG(INFO) << "unlink file failed!, errno = " << errno;
            }
        }
        fsptr->Delete("./raftsnap/chunkfilepool");
        fsptr->Delete("./raftsnap/chunkfilepool.meta");
        FilePoolPtr_->UnInitialize();
        fsadaptor->Release();
    }

    void CreateChunkFile(const std::string& filepath) {
        int fd = fsptr->Open(filepath.c_str(), O_RDWR | O_CREAT);
        char data[8192];
        memset(data, 'a', 8192);
        fsptr->Write(fd, data, 0, 8192);
        fsptr->Close(fd);
    }

    scoped_refptr<braft::FileSystemAdaptor> fsadaptor;
    std::shared_ptr<FilePool>  FilePoolPtr_;
    std::shared_ptr<LocalFileSystem>  fsptr;
    std::shared_ptr<MockLocalFileSystem>  lfs;
    CurveFilesystemAdaptor*  rfa;
};

TEST_F(RaftSnapshotFilesystemAdaptorMockTest, open_file_mock_test) {
    //1 Open flag without CREAT, open failed
    CreateChunkFile("./10");
    std::string path = "./10";
    butil::File::Error e;
    ASSERT_EQ(FilePoolPtr_->Size(), 3);
    EXPECT_CALL(*lfs, Open(_, _)).Times(AtLeast(1)).WillRepeatedly(Return(-1));
    braft::FileAdaptor* fa = fsadaptor->open(path,
                                             O_RDONLY | O_CLOEXEC,
                                             nullptr,
                                             &e);

    ASSERT_EQ(FilePoolPtr_->Size(), 3);
    ASSERT_EQ(nullptr, fa);

    //2 Open flag with CREAT to retrieve files from FilePool, but FilePool failed to open the file
    //So we still follow the original logic and successfully create the file locally
    EXPECT_CALL(*lfs, Open(_, _)).Times(3).WillOnce(Return(-1))
                                          .WillOnce(Return(-1))
                                          .WillOnce(Return(-1));
    EXPECT_CALL(*lfs, FileExists(_)).Times(1).WillRepeatedly(Return(0));
    ASSERT_EQ(FilePoolPtr_->Size(), 3);
    path = "./11";
    fa = fsadaptor->open(path, O_RDONLY | O_CLOEXEC | O_CREAT, nullptr, &e);
    ASSERT_EQ(FilePoolPtr_->Size(), 0);
    ASSERT_TRUE(fsptr->FileExists("./10"));
    ASSERT_EQ(0, fsptr->Delete("./10"));
    ASSERT_FALSE(fsptr->FileExists("./10"));
    ASSERT_EQ(nullptr, fa);

    //3 The file to be created is in Filter, but it was created locally and successfully
    EXPECT_CALL(*lfs, Open(_, _)).Times(1).WillOnce(Return(0));
    EXPECT_CALL(*lfs, FileExists(_)).Times(0);
    path = BRAFT_SNAPSHOT_META_FILE;
    fa = fsadaptor->open(path, O_RDONLY | O_CLOEXEC | O_CREAT, nullptr, &e);
    ASSERT_NE(nullptr, fa);
}

TEST_F(RaftSnapshotFilesystemAdaptorMockTest, delete_file_mock_test) {
    //1 Delete file. The file exists and is on the filter list, but delete failed with false return
    EXPECT_CALL(*lfs, DirExists(_)).Times(1).WillRepeatedly(Return(false));
    EXPECT_CALL(*lfs, FileExists(_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(*lfs, Delete(_)).Times(1).WillRepeatedly(Return(-1));
    bool ret = fsadaptor->delete_file(BRAFT_SNAPSHOT_META_FILE, true);
    ASSERT_FALSE(ret);

    //2 Delete file. The file exists and is not on the filter list, but the recycle chunk failed with false return
    EXPECT_CALL(*lfs, Delete(_)).Times(1).WillRepeatedly(Return(-1));
    EXPECT_CALL(*lfs, DirExists(_)).Times(1).WillRepeatedly(Return(false));
    EXPECT_CALL(*lfs, FileExists(_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(*lfs, Open(_, _)).Times(1).WillRepeatedly(Return(-1));
    ret = fsadaptor->delete_file("temp", true);
    ASSERT_FALSE(ret);

    //3 Delete directory. The file exists and is not on the filter list, but the recycle chunk failed with false return
    std::vector<std::string> dircontent;
    dircontent.push_back("/2");
    dircontent.push_back("/1");
    dircontent.push_back(BRAFT_SNAPSHOT_META_FILE);
    EXPECT_CALL(*lfs, DirExists(_)).Times(2).WillOnce(Return(true))
                                          .WillOnce(Return(false));
    EXPECT_CALL(*lfs, Delete(_)).Times(1).WillRepeatedly(Return(-1));
    EXPECT_CALL(*lfs, Open(_, _)).Times(1).WillRepeatedly(Return(-1));
    EXPECT_CALL(*lfs, List(_, _)).Times(2).WillRepeatedly(DoAll(
                                SetArgPointee<1>(dircontent), Return(-1)));
    ret = fsadaptor->delete_file("1", true);
    ASSERT_FALSE(ret);
}

TEST_F(RaftSnapshotFilesystemAdaptorMockTest, rename_mock_test) {
    //1 Renaming file, file exists and is on the filter list, but Rename failed with false return
    EXPECT_CALL(*lfs, Rename(_, _, _)).Times(1).WillRepeatedly(Return(-1));
    EXPECT_CALL(*lfs, FileExists(_)).Times(0);
    bool ret = fsadaptor->rename("1", BRAFT_SNAPSHOT_META_FILE);
    ASSERT_FALSE(ret);

    //2 Renaming file. The file exists and is not on the filter list, but Rename failed with false return
    EXPECT_CALL(*lfs, Rename(_, _, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(*lfs, FileExists(_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(*lfs, Open(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(*lfs, Fstat(_, _)).Times(1).WillRepeatedly(Return(-1));
    ret = fsadaptor->rename("1", "2");
    ASSERT_TRUE(ret);
}

}   // namespace chunkserver
}   // namespace curve
