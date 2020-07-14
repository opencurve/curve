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
#include <braft/snapshot.h>
#include <butil/memory/ref_counted.h>

#include <memory>

#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "src/chunkserver/raftsnapshot_filesystem_adaptor.h"

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::chunkserver::ChunkfilePool;
namespace curve {
namespace chunkserver {
class RaftSnapshotFilesystemAdaptorTest : public testing::Test {
 public:
    void SetUp() {
        fsptr = curve::fs::LocalFsFactory::CreateFs(
                        curve::fs::FileSystemType::EXT4, "/dev/sda");
        ChunkfilepoolPtr_ = std::make_shared<ChunkfilePool>(fsptr);
        if (ChunkfilepoolPtr_ == nullptr) {
            LOG(FATAL) << "allocate chunkfile pool failed!";
        }
        int count = 1;
        fsptr->Mkdir("./raftsnap");
        fsptr->Mkdir("./raftsnap/chunkfilepool");
        std::string dirname = "./raftsnap/chunkfilepool";
        while (count < 4) {
            std::string  filename = "./raftsnap/chunkfilepool/"
                                  + std::to_string(count);
            int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
            char data[8192];
            memset(data, 'a', 8192);
            fsptr->Write(fd, data, 0, 8192);
            fsptr->Close(fd);
            count++;
        }

        uint32_t chunksize = 4096;
        uint32_t metapagesize = 4096;

        ChunkfilePoolOptions cpopt;
        cpopt.getChunkFromPool = true;
        cpopt.chunkSize = chunksize;
        cpopt.metaPageSize = metapagesize;
        cpopt.cpMetaFileSize = 4096;
        memcpy(cpopt.chunkFilePoolDir, "./raftsnap/chunkfilepool", 17);
        memcpy(cpopt.metaPath, "./raftsnap/chunkfilepool.meta", 30);

        int ret = ChunkfilePoolHelper::PersistEnCodeMetaInfo(
                                                    fsptr,
                                                    chunksize,
                                                    metapagesize,
                                                    dirname,
                                            "./raftsnap/chunkfilepool.meta");

        if (ret == -1) {
            LOG(ERROR) << "persist chunkfile pool meta info failed!";
            return;
        }

        rfa = new RaftSnapshotFilesystemAdaptor(ChunkfilepoolPtr_, fsptr);
        std::vector<std::string> filterList;
        std::string snapshotMeta(BRAFT_SNAPSHOT_META_FILE);
        filterList.push_back(snapshotMeta);
        rfa->SetFilterList(filterList);

        ASSERT_TRUE(ChunkfilepoolPtr_->Initialize(cpopt));
        scoped_refptr<braft::FileSystemAdaptor> scptr(rfa);

        fsadaptor.swap(scptr);
        fsadaptor->AddRef();
    }

    void TearDown() {
        fsptr->Delete("./raftsnap");
        ChunkfilepoolPtr_->UnInitialize();
        fsadaptor->Release();
    }

    void ClearChunkFilepool() {
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
    std::shared_ptr<ChunkfilePool>  ChunkfilepoolPtr_;
    std::shared_ptr<LocalFileSystem>  fsptr;
    RaftSnapshotFilesystemAdaptor*  rfa;
};

TEST_F(RaftSnapshotFilesystemAdaptorTest, open_file_test) {
    // 1. open flag不带CREAT
    std::string path = "./raftsnap/10";
    butil::File::Error e;
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 3);
    braft::FileAdaptor* fa = fsadaptor->open(path,
                                             O_RDONLY | O_CLOEXEC,
                                             nullptr,
                                             &e);
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 3);
    ASSERT_FALSE(fsptr->FileExists("./raftsnap/10"));
    ASSERT_EQ(nullptr, fa);

    // 2. open flag待CREAT, 从chunkfilepool取文件
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 3);
    fa = fsadaptor->open(path, O_RDONLY | O_CLOEXEC | O_CREAT, nullptr, &e);
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 2);
    ASSERT_TRUE(fsptr->FileExists("./raftsnap/10"));
    ASSERT_NE(nullptr, fa);

    // 3. open flag待CREAT,chunkfilepool为空时，从chunkfilepool取文件
    ClearChunkFilepool();
    fa = fsadaptor->open("./raftsnap/11",
                         O_RDONLY | O_CLOEXEC | O_CREAT,
                         nullptr,
                         &e);
    ASSERT_EQ(nullptr, fa);
}

TEST_F(RaftSnapshotFilesystemAdaptorTest, delete_file_test) {
    // 1. 创建一个多层目录，且目录中含有chunk文件
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp"));
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp/test_temp1"));
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp/test_temp1/test_temp2"));
    CreateChunkFile("./test_temp/1");
    CreateChunkFile("./test_temp/2");
    CreateChunkFile("./test_temp/test_temp1/1");
    CreateChunkFile("./test_temp/test_temp1/2");
    CreateChunkFile("./test_temp/test_temp1/test_temp2/1");
    CreateChunkFile("./test_temp/test_temp1/test_temp2/2");
    // 非递归删除非空文件夹，返回false
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 3);
    ASSERT_FALSE(fsadaptor->delete_file("./test_temp", false));
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 3);
    // 递归删除文件夹，chunk被回收到chunkfilepool
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp", true));
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 9);
    ASSERT_FALSE(fsptr->DirExists("./test_temp"));
    ASSERT_FALSE(fsptr->DirExists("./test_temp/test_temp1"));
    ASSERT_FALSE(fsptr->DirExists("./test_temp/test_temp1/test_temp2"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/1"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/2"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/test_temp1/1"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/test_temp1/2"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/test_temp1/test_temp2/1"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp/test_temp1/test_temp2/2"));

    // 2. 创建一个单层空目录
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp3"));
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp3", false));
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp4"));
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp4", true));
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 9);
    ASSERT_FALSE(fsptr->DirExists("./test_temp3"));
    ASSERT_FALSE(fsptr->DirExists("./test_temp4"));

    // 3. 删除一个常规chunk文件， 会被回收到chunkfilepool
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp5"));
    CreateChunkFile("./test_temp5/3");
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp5/3", false));
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 10);
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp6"));
    CreateChunkFile("./test_temp6/4");
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp6/4", true));
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 11);
    ASSERT_FALSE(fsptr->FileExists("./test_temp6/4"));
    ASSERT_FALSE(fsptr->FileExists("./test_temp5/3"));
    ASSERT_TRUE(fsptr->DirExists("./test_temp5"));
    ASSERT_TRUE(fsptr->DirExists("./test_temp6"));
    ASSERT_EQ(0, fsptr->Delete("./test_temp5"));
    ASSERT_EQ(0, fsptr->Delete("./test_temp6"));


    // 4. 删除一个非chunk大小的文件，会直接删除该文件
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp7"));
    int fd = fsptr->Open("./test_temp7/5", O_RDWR | O_CREAT);
    char data[4096];
    memset(data, 'a', 4096);
    fsptr->Write(fd, data, 0, 4096);
    fsptr->Close(fd);
    ASSERT_TRUE(fsadaptor->delete_file("./test_temp7/5", true));
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 11);
    ASSERT_FALSE(fsptr->FileExists("./test_temp7/5"));
    ASSERT_EQ(0, fsptr->Delete("./test_temp7"));
}

TEST_F(RaftSnapshotFilesystemAdaptorTest, rename_test) {
    // 1. 创建一个多层目录，且目录中含有chunk文件
    ASSERT_EQ(0, fsptr->Mkdir("./test_temp"));
    std::string filename = "./test_temp/";
    filename.append(BRAFT_SNAPSHOT_META_FILE);

    // 目标文件size是chunksize，但是目标文件在过滤名单里，所以直接过滤
    CreateChunkFile(filename);
    int poolSize = ChunkfilepoolPtr_->Size();
    std::string temppath = "./temp";
    char metaPage[4096];
    ASSERT_EQ(0, ChunkfilepoolPtr_->GetChunk(temppath, metaPage));
    ASSERT_TRUE(rfa->rename(temppath, filename));
    ASSERT_TRUE(fsptr->FileExists(filename));
    ASSERT_FALSE(fsptr->FileExists(temppath));
    ASSERT_EQ(poolSize - 1, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(0, fsptr->Delete(filename));

     // 目标文件size是chunksize，但是目标文件不在过滤名单里，所以先回收再rename
    filename = "./test_temp/";
    filename.append("test");
    CreateChunkFile(filename);
    ASSERT_EQ(0, ChunkfilepoolPtr_->GetChunk(temppath, metaPage));
    ASSERT_TRUE(rfa->rename(temppath, filename));
    ASSERT_EQ(poolSize - 1, ChunkfilepoolPtr_->Size());
    ASSERT_FALSE(fsptr->FileExists(temppath));
    ASSERT_TRUE(fsptr->FileExists(filename));
    ASSERT_EQ(0, fsptr->Delete(filename));


    ASSERT_EQ(0, fsptr->Delete("./test_temp"));
}

}   // namespace chunkserver
}   // namespace curve
