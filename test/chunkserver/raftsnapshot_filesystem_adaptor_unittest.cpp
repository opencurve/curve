/*
 * Project: curve
 * File Created: Tuesday, 11th June 2019 5:17:55 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
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
        std::string dirname = "./chunkfilepool";
        while (count < 4) {
            std::string  filename = "./chunkfilepool/" + std::to_string(count);
            fsptr->Mkdir("./chunkfilepool");
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
        memcpy(cpopt.chunkFilePoolDir, "./chunkfilepool", 17);
        memcpy(cpopt.metaPath, "./chunkfilepool.meta", 22);

        int ret = ChunkfilePoolHelper::PersistEnCodeMetaInfo(
                                                    fsptr,
                                                    chunksize,
                                                    metapagesize,
                                                    dirname,
                                                    "./chunkfilepool.meta");

        if (ret == -1) {
            LOG(ERROR) << "persist chunkfile pool meta info failed!";
            return;
        }

        ASSERT_TRUE(ChunkfilepoolPtr_->Initialize(cpopt));
        scoped_refptr<braft::FileSystemAdaptor> scptr(
        new RaftSnapshotFilesystemAdaptor(ChunkfilepoolPtr_, fsptr));

        fsadaptor.swap(scptr);
        fsadaptor->AddRef();
    }

    void TearDown() {
        std::vector<std::string> filename;
        fsptr->List("./chunkfilepool", &filename);
        for (auto iter : filename) {
            auto path = "./chunkfilepool/" + iter;
            int err = fsptr->Delete(path.c_str());
            if (err) {
                LOG(INFO) << "unlink file failed!, errno = " << errno;
            }
        }
        fsptr->Delete("./chunkfilepool");
        fsptr->Delete("./chunkfilepool.meta");
        ChunkfilepoolPtr_->UnInitialize();
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
    std::shared_ptr<ChunkfilePool>  ChunkfilepoolPtr_;
    std::shared_ptr<LocalFileSystem>  fsptr;
};

TEST_F(RaftSnapshotFilesystemAdaptorTest, open_file_test) {
    // 1. open flag不带CREAT
    std::string path = "./10";
    butil::File::Error e;
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 3);
    braft::FileAdaptor* fa = fsadaptor->open(path,
                                             O_RDONLY | O_CLOEXEC,
                                             nullptr,
                                             &e);
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 3);
    ASSERT_FALSE(fsptr->FileExists("./10"));
    ASSERT_EQ(nullptr, fa);

    // 2. open flag待CREAT, 从chunkfilepool取文件
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 3);
    fa = fsadaptor->open(path, O_RDONLY | O_CLOEXEC | O_CREAT, nullptr, &e);
    ASSERT_EQ(ChunkfilepoolPtr_->Size(), 2);
    ASSERT_TRUE(fsptr->FileExists("./10"));
    ASSERT_EQ(0, fsptr->Delete("./10"));
    ASSERT_FALSE(fsptr->FileExists("./10"));
    ASSERT_NE(nullptr, fa);
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

}   // namespace chunkserver
}   // namespace curve
