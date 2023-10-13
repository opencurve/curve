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

#include "src/chunkserver/raftsnapshot/curve_filesystem_adaptor.h"

#include <braft/snapshot.h>
#include <butil/memory/ref_counted.h>
#include <gtest/gtest.h>

#include <memory>

#include "src/chunkserver/datastore/file_pool.h"
#include "src/chunkserver/raftsnapshot/define.h"
#include "src/fs/local_filesystem.h"

using curve::chunkserver::FilePool;
using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
namespace curve {
namespace chunkserver {

static const char* kFilePool = "./raftsnap/chunkfilepool/";
static const char* kFilePoolMeta = "./raftsnap/chunkfilepool.meta";

class CurveFilesystemAdaptorTest : public testing::Test {
 public:
    void SetUp() {
        fsptr = curve::fs::LocalFsFactory::CreateFs(
            curve::fs::FileSystemType::EXT4, "/dev/sda");
        chunkFilePoolPtr_ = std::make_shared<FilePool>(fsptr);
        ASSERT_TRUE(chunkFilePoolPtr_);

        int count = 1;
        fsptr->Mkdir("./raftsnap");
        fsptr->Mkdir("./raftsnap/chunkfilepool");
        while (count < 4) {
            std::string filename = kFilePool + std::to_string(count);
            int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
            ASSERT_GE(fd, 0);
            char data[8192];
            memset(data, 'a', 8192);
            fsptr->Write(fd, data, 0, 8192);
            fsptr->Close(fd);
            count++;
        }

        uint32_t chunksize = 4096;
        uint32_t metapagesize = 4096;
        uint32_t blockSize = 4096;

        FilePoolMeta meta;
        meta.chunkSize = chunksize;
        meta.metaPageSize = metapagesize;
        meta.hasBlockSize = true;
        meta.blockSize = blockSize;
        meta.filePoolPath = kFilePool;

        FilePoolOptions cpopt;
        cpopt.getFileFromPool = true;
        cpopt.fileSize = chunksize;
        cpopt.metaPageSize = metapagesize;
        cpopt.blockSize = blockSize;
        strcpy(cpopt.filePoolDir, kFilePool);   // NOLINT(runtime/printf)
        strcpy(cpopt.metaPath, kFilePoolMeta);  // NOLINT(runtime/printf)

        ASSERT_EQ(0, FilePoolHelper::PersistEnCodeMetaInfo(fsptr, meta,
                                                           kFilePoolMeta));

        rfa = new CurveFilesystemAdaptor(chunkFilePoolPtr_, fsptr);
        std::vector<std::string> filterList;
        std::string snapshotMeta(BRAFT_SNAPSHOT_META_FILE);
        filterList.push_back(snapshotMeta);
        rfa->SetFilterList(filterList);

        ASSERT_TRUE(chunkFilePoolPtr_->Initialize(cpopt));
        scoped_refptr<braft::FileSystemAdaptor> scptr(rfa);

        fsadaptor.swap(scptr);
        fsadaptor->AddRef();
    }

    void TearDown() {
        fsptr->Delete("./raftsnap");
        chunkFilePoolPtr_->UnInitialize();
        fsadaptor->Release();
    }

    void ClearFilePool() {
        std::vector<std::string> filename;
        fsptr->List("./raftsnap/chunkfilepool", &filename);
        for (auto& iter : filename) {
            auto path = "./raftsnap/chunkfilepool/" + iter;
            int err = fsptr->Delete(path.c_str());
            if (err) {
                LOG(INFO) << "unlink file failed!, errno = " << errno;
            }
        }
    }

    void CreateChunkFile(const std::string& filepath) {
        int fd = fsptr->Open(filepath.c_str(), O_RDWR | O_CREAT);
        char data[8192];
        memset(data, 'a', 8192);
        fsptr->Write(fd, data, 0, 8192);
        fsptr->Close(fd);
    }

    scoped_refptr<braft::FileSystemAdaptor> fsadaptor;
    std::shared_ptr<FilePool> chunkFilePoolPtr_;
    std::shared_ptr<LocalFileSystem> fsptr;
    CurveFilesystemAdaptor* rfa;
};

TEST_F(CurveFilesystemAdaptorTest, open_file_test) {
    // 1. Open flag without CREAT
    std::string path = "./raftsnap/10";
    butil::File::Error e;
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 3);
    braft::FileAdaptor* fa =
        fsadaptor->open(path, O_RDONLY | O_CLOEXEC, nullptr, &e);
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 3);
    ASSERT_FALSE(fsptr->FileExists("./raftsnap/10"));
    ASSERT_EQ(nullptr, fa);

    // 2. Open flag for CREAT, retrieve files from FilePool
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 3);
    fa = fsadaptor->open(path, O_RDONLY | O_CLOEXEC | O_CREAT, nullptr, &e);
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 2);
    ASSERT_TRUE(fsptr->FileExists("./raftsnap/10"));
    ASSERT_NE(nullptr, fa);

    // 3. Open flag, wait for CREAT, and when FilePool is empty, retrieve the
    // file from FilePool
    ClearFilePool();
    fa = fsadaptor->open("./raftsnap/11", O_RDONLY | O_CLOEXEC | O_CREAT,
                         nullptr, &e);
    ASSERT_EQ(nullptr, fa);
}

TEST_F(CurveFilesystemAdaptorTest, delete_file_test) {
    // 1. Create a multi-level directory with chunk files in it
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp"));
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp/test_temp1"));
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp/test_temp1/test_temp2"));
    CreateChunkFile("./test_temp/1");
    CreateChunkFile("./test_temp/2");
    CreateChunkFile("./test_temp/test_temp1/1");
    CreateChunkFile("./test_temp/test_temp1/2");
    CreateChunkFile("./test_temp/test_temp1/test_temp2/1");
    CreateChunkFile("./test_temp/test_temp1/test_temp2/2");
    // Non recursive deletion of non empty folders, returning false
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 3);
    ASSERT_FALSE(fsadaptor->delete_file("./test_temp", false));
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 3);
    // Recursively delete folder, chunk is recycled to FilePool
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp", true));
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 9);
    ASSERT_FALSE(fsptr->DirExists("./test_temp"));
    ASSERT_FALSE(fsptr->DirExists("./test_temp/test_temp1"));
    ASSERT_FALSE(fsptr->DirExists("./test_temp/test_temp1/test_temp2"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/1"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/2"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/test_temp1/1"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/test_temp1/2"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/test_temp1/test_temp2/1"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/test_temp1/test_temp2/2"));

    // 2. Create a single level empty directory
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp3"));
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp3", false));
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp4"));
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp4", true));
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 9);
    ASSERT_FALSE(fsptr->DirExists("./test_temp3"));
    ASSERT_FALSE(fsptr->DirExists("./test_temp4"));

    // 3. Deleting a regular chunk file will be recycled to FilePool
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp5"));
    CreateChunkFile("./test_temp5/3");
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp5/3", false));
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 10);
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp6"));
    CreateChunkFile("./test_temp6/4");
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp6/4", true));
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 11);
    ASSERT_FALSE(fsptr->FileExists("./test_temp6/4"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp5/3"));
    ASSERT_TRUE(fsptr->DirExists("./test_temp5"));
    ASSERT_TRUE(fsptr->DirExists("./test_temp6"));
    ASSERT_EQ(0, fsptr->Delete("./test_temp5"));
    ASSERT_EQ(0, fsptr->Delete("./test_temp6"));

    // 4. Deleting a file of a non chunk size will directly delete the file
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp7"));
    int fd = fsptr->Open("./test_temp7/5", O_RDWR | O_CREAT);
    char data[4096];
    memset(data, 'a', 4096);
    fsptr->Write(fd, data, 0, 4096);
    fsptr->Close(fd);
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp7/5", true));
    ASSERT_EQ(chunkFilePoolPtr_->Size(), 11);
    ASSERT_FALSE(fsptr->FileExists("./test_temp7/5"));
    ASSERT_EQ(0, fsptr->Delete("./test_temp7"));
}

TEST_F(CurveFilesystemAdaptorTest, rename_test) {
    // 1. Create a multi-level directory with chunk files in it
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp"));
    std::string filename = "./test_temp/";
    filename.append(BRAFT_SNAPSHOT_META_FILE);

    // The target file size is chunksize, but it is on the filtering list, so it
    // is directly filtered
    CreateChunkFile(filename);
    int poolSize = chunkFilePoolPtr_->Size();
    std::string temppath = "./temp";
    char metaPage[4096];
    ASSERT_EQ(0, chunkFilePoolPtr_->GetFile(temppath, metaPage));
    ASSERT_TRUE(rfa->rename(temppath, filename));
    ASSERT_TRUE(fsptr->FileExists(filename));
    ASSERT_FALSE(fsptr->FileExists(temppath));
    ASSERT_EQ(poolSize - 1, chunkFilePoolPtr_->Size());
    ASSERT_EQ(0, fsptr->Delete(filename));

    // The target file size is chunksize, but it is not on the filter list, so
    // recycle it first and rename it again
    filename = "./test_temp/";
    filename.append("test");
    CreateChunkFile(filename);
    ASSERT_EQ(0, chunkFilePoolPtr_->GetFile(temppath, metaPage));
    ASSERT_TRUE(rfa->rename(temppath, filename));
    ASSERT_EQ(poolSize - 1, chunkFilePoolPtr_->Size());
    ASSERT_FALSE(fsptr->FileExists(temppath));
    ASSERT_TRUE(fsptr->FileExists(filename));
    ASSERT_EQ(0, fsptr->Delete(filename));

    ASSERT_EQ(0, fsptr->Delete("./test_temp"));
}

}  // namespace chunkserver
}  // namespace curve
