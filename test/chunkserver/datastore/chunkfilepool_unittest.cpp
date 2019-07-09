/*
 * Project: curve
 * File Created: Monday, 10th December 2018 3:22:12 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <json/json.h>
#include <fcntl.h>
#include <climits>
#include <memory>

#include "src/common/crc32.h"
#include "src/common/curve_define.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "test/fs/mock_local_filesystem.h"

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

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::chunkserver::ChunkfilePool;
using curve::chunkserver::ChunkfilePoolOptions;
using curve::chunkserver::ChunkFilePoolState_t;
using curve::common::kChunkFilePoolMaigic;
using curve::chunkserver::ChunkfilePoolHelper;

class CSChunkfilePool_test : public testing::Test {
 public:
    void SetUp() {
        fsptr = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

        ChunkfilepoolPtr_ = std::make_shared<ChunkfilePool>(fsptr);
        if (ChunkfilepoolPtr_ == nullptr) {
            LOG(FATAL) << "allocate chunkfile pool failed!";
        }
        int count = 1;
        fsptr->Mkdir("./cspooltest/");
        std::string dirname = "./cspooltest/chunkfilepool";
        while (count < 51) {
            std::string  filename = "./cspooltest/chunkfilepool/"
                                  + std::to_string(count);
            fsptr->Mkdir("./cspooltest/chunkfilepool");
            int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
            char data[8192];
            memset(data, 'a', 8192);
            fsptr->Write(fd, data, 0, 8192);
            fsptr->Close(fd);
            count++;
        }

        uint32_t chunksize = 4096;
        uint32_t metapagesize = 4096;

        int ret = ChunkfilePoolHelper::PersistEnCodeMetaInfo(
                                                    fsptr,
                                                    chunksize,
                                                    metapagesize,
                                                    dirname,
                                            "./cspooltest/chunkfilepool.meta");

        if (ret == -1) {
            LOG(ERROR) << "persist chunkfile pool meta info failed!";
            return;
        }

        int fd = fsptr->Open("./cspooltest/chunkfilepool.meta2",
                             O_RDWR | O_CREAT);
        if (fd < 0) {
            return;
        }

        char* buffer = new char[2048];
        memset(buffer, 1, 2048);
        ret = fsptr->Write(fd, buffer, 0, 2048);
        if (ret != 2048) {
            delete[] buffer;
            return;
        }
        delete[] buffer;
        fsptr->Close(fd);
    }

    void TearDown() {
        std::vector<std::string> filename;
        fsptr->List("./cspooltest/chunkfilepool", &filename);
        for (auto iter : filename) {
            auto path = "./cspooltest/chunkfilepool/" + iter;
            int err = fsptr->Delete(path.c_str());
            if (err) {
                LOG(INFO) << "unlink file failed!, errno = " << errno;
            }
        }
        fsptr->Delete("./cspooltest/chunkfilepool");
        fsptr->Delete("./cspooltest/chunkfilepool.meta");
        fsptr->Delete("./cspooltest/chunkfilepool.meta2");
        fsptr->Delete("./cspooltest");
        ChunkfilepoolPtr_->UnInitialize();
    }

    std::shared_ptr<ChunkfilePool>  ChunkfilepoolPtr_;
    std::shared_ptr<LocalFileSystem>  fsptr;
};

bool CheckFileOpenOrNot(const std::string& filename) {
    std::string syscmd;
    syscmd.append("lsof ").append(filename);
    FILE * fp;
    char buffer[4096];
    memset(buffer, 0, 4096);
    fp = popen(syscmd.c_str(), "r");
    fgets(buffer, sizeof(buffer), fp);
    pclose(fp);

    std::string out(buffer, 4096);
    return out.find("No such file or directory") != out.npos;
}

TEST_F(CSChunkfilePool_test, InitializeTest) {
    std::string chunkfilepool = "./cspooltest/chunkfilepool.meta";

    ChunkfilePoolOptions cfop;
    cfop.chunkSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, chunkfilepool.c_str(), chunkfilepool.size());

    // initialize
    ASSERT_TRUE(ChunkfilepoolPtr_->Initialize(cfop));
    ASSERT_EQ(50, ChunkfilepoolPtr_->Size());
    // 初始化阶段会扫描chunkfilepool内的所有文件，在扫描结束之后需要关闭这些文件
    // 防止过多的文件描述符被占用
    ASSERT_FALSE(CheckFileOpenOrNot("./cspooltest/chunkfilepool/1"));
    ASSERT_FALSE(CheckFileOpenOrNot("./cspooltest/chunkfilepool/2"));
    cfop.chunkSize = 8192;
    cfop.metaPageSize = 4096;
    // test meta content wrong
    ASSERT_FALSE(ChunkfilepoolPtr_->Initialize(cfop));
    cfop.chunkSize = 8192;
    cfop.metaPageSize = 4096;
    ASSERT_FALSE(ChunkfilepoolPtr_->Initialize(cfop));
    // invalid file name
    std::string  filename = "./cspooltest/chunkfilepool/a";
    cfop.chunkSize = 4096;
    cfop.metaPageSize = 4096;
    int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
    char data[8192];
    memset(data, 'a', 8192);
    fsptr->Write(fd, data, 0, 8192);
    fsptr->Close(fd);
    ASSERT_FALSE(ChunkfilepoolPtr_->Initialize(cfop));
    // test meta file wrong
    chunkfilepool = "./cspooltest/chunkfilepool.meta2";
    cfop.chunkSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, chunkfilepool.c_str(), chunkfilepool.size());
    ASSERT_FALSE(ChunkfilepoolPtr_->Initialize(cfop));
    // test meta file not exist
    chunkfilepool = "./cspooltest/chunkfilepool.meta3";
    cfop.chunkSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, chunkfilepool.c_str(), chunkfilepool.size());
    ASSERT_FALSE(ChunkfilepoolPtr_->Initialize(cfop));

    fsptr->Delete("./cspooltest/chunkfilepool/a");
    fsptr->Delete("./cspooltest/chunkfilepool.meta3");
}

TEST_F(CSChunkfilePool_test, GetChunkTest) {
    std::string chunkfilepool = "./cspooltest/chunkfilepool.meta";
    ChunkfilePoolOptions cfop;
    cfop.chunkSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, chunkfilepool.c_str(), chunkfilepool.size());
    // test get chunk success
    char metapage[4096];
    memset(metapage, '1', 4096);
    ASSERT_EQ(-1, ChunkfilepoolPtr_->GetChunk("./new_exit", metapage));
    ASSERT_EQ(-2, fsptr->Delete("./new_exit"));
    ChunkfilepoolPtr_->Initialize(cfop);
    ASSERT_EQ(50, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(0, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_EQ(49, ChunkfilepoolPtr_->Size());
    ASSERT_TRUE(fsptr->FileExists("./new1"));
    int fd = fsptr->Open("./new1", O_RDWR);
    char data[4096];
    ASSERT_GE(fd, 0);
    int len = fsptr->Read(fd, data, 0, 4096);
    ASSERT_EQ(4096, len);
    for (int i = 0; i < 4096; i++) {
        ASSERT_EQ(data[i], '1');
    }
    ASSERT_EQ(0, fsptr->Close(fd));
    ASSERT_EQ(0, fsptr->Delete("./new1"));

    // test get chunk success
    ASSERT_EQ(0, ChunkfilepoolPtr_->GetChunk("./new2", metapage));
    ASSERT_TRUE(fsptr->FileExists("./new2"));
    ASSERT_NE(49, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(0, fsptr->Delete("./new2"));
}


TEST_F(CSChunkfilePool_test, GetChunkMockTest) {
    auto lfs = std::make_shared<curve::fs::MockLocalFileSystem>();

    std::string chunkfilepool = "./cspooltest/chunkfilepool.meta";
    ChunkfilePoolOptions cfop;
    cfop.chunkSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, chunkfilepool.c_str(), chunkfilepool.size());
    // test get chunk success
    char metapage[4096];
    memset(metapage, '1', 4096);
    ChunkfilepoolPtr_->Initialize(cfop);

    ChunkfilepoolPtr_->SetLocalFileSystem(lfs);

    // 打开文件失败，chunkfile pool没变化
    EXPECT_CALL(*lfs, Open(_, _)).Times(5).WillRepeatedly(Return(-1));
    ASSERT_EQ(50, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(-1, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_EQ(45, ChunkfilepoolPtr_->Size());

    // 写入文件失败，chunkfile pool没变化
    EXPECT_CALL(*lfs, Open(_, _)).Times(5).WillRepeatedly(Return(1));
    EXPECT_CALL(*lfs, Write(_, _, _, _)).Times(5).WillRepeatedly(Return(-1));
    EXPECT_CALL(*lfs, Close(_)).Times(5).WillRepeatedly(Return(-1));
    ASSERT_EQ(45, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(-1, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_EQ(40, ChunkfilepoolPtr_->Size());

    // Fsync文件失败，chunkfile pool没变化
    EXPECT_CALL(*lfs, Open(_, _)).Times(5).WillRepeatedly(Return(1));
    EXPECT_CALL(*lfs, Write(_, _, _, _)).Times(5).WillRepeatedly(Return(4096));
    EXPECT_CALL(*lfs, Fsync(_)).Times(5).WillRepeatedly(Return(-1));
    EXPECT_CALL(*lfs, Close(_)).Times(5).WillRepeatedly(Return(-1));
    ASSERT_EQ(40, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(-1, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_EQ(35, ChunkfilepoolPtr_->Size());

    // Close文件失败，chunkfile pool没变化
    EXPECT_CALL(*lfs, Open(_, _)).Times(5).WillRepeatedly(Return(1));
    EXPECT_CALL(*lfs, Write(_, _, _, _)).Times(5).WillRepeatedly(Return(4096));
    EXPECT_CALL(*lfs, Fsync(_)).Times(5).WillRepeatedly(Return(0));
    EXPECT_CALL(*lfs, Close(_)).Times(5).WillRepeatedly(Return(-1));
    ASSERT_EQ(35, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(-1, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_EQ(30, ChunkfilepoolPtr_->Size());


    // Close文件成功Rename失败，chunkfile pool没变化
    EXPECT_CALL(*lfs, Open(_, _)).Times(5).WillRepeatedly(Return(1));
    EXPECT_CALL(*lfs, Write(_, _, _, _)).Times(5).WillRepeatedly(Return(4096));
    EXPECT_CALL(*lfs, Fsync(_)).Times(5).WillRepeatedly(Return(0));
    EXPECT_CALL(*lfs, Close(_)).Times(5).WillRepeatedly(Return(0));
    EXPECT_CALL(*lfs, Rename(_, _, _)).Times(5).WillRepeatedly(Return(-1));
    ASSERT_EQ(30, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(-1, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_EQ(25, ChunkfilepoolPtr_->Size());

    // Close文件成功Rename返回EEXIST，chunkfile pool没变化
    EXPECT_CALL(*lfs, Open(_, _)).Times(1).WillRepeatedly(Return(1));
    EXPECT_CALL(*lfs, Write(_, _, _, _)).Times(1).WillRepeatedly(Return(4096));
    EXPECT_CALL(*lfs, Fsync(_)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(*lfs, Close(_)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(*lfs, Rename(_, _, _)).Times(1).WillRepeatedly(Return(-EEXIST));
    ASSERT_EQ(25, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(-EEXIST, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_EQ(24, ChunkfilepoolPtr_->Size());

    // Close文件成功Rename成功，chunkfile pool少一个chunk
    EXPECT_CALL(*lfs, Open(_, _)).Times(1).WillRepeatedly(Return(1));
    EXPECT_CALL(*lfs, Write(_, _, _, _)).Times(1).WillRepeatedly(Return(4096));
    EXPECT_CALL(*lfs, Fsync(_)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(*lfs, Close(_)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(*lfs, Rename(_, _, _)).Times(1).WillRepeatedly(Return(0));
    ASSERT_EQ(24, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(0, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_EQ(23, ChunkfilepoolPtr_->Size());
}


TEST_F(CSChunkfilePool_test, RecycleChunkTest) {
    std::string chunkfilepool = "./cspooltest/chunkfilepool.meta";
    ChunkfilePoolOptions cfop;
    cfop.chunkSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, chunkfilepool.c_str(), chunkfilepool.size());

    ChunkfilepoolPtr_->Initialize(cfop);
    ChunkFilePoolState_t currentStat = ChunkfilepoolPtr_->GetState();
    ASSERT_EQ(50, currentStat.preallocatedChunksLeft);
    ASSERT_EQ(50, ChunkfilepoolPtr_->Size());
    char metapage[4096];
    memset(metapage, '1', 4096);
    ASSERT_EQ(0, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_TRUE(fsptr->FileExists("./new1"));
    ASSERT_EQ(49, ChunkfilepoolPtr_->Size());

    currentStat = ChunkfilepoolPtr_->GetState();
    ASSERT_EQ(49, currentStat.preallocatedChunksLeft);

    ChunkfilepoolPtr_->RecycleChunk("./new1");
    ASSERT_EQ(50, ChunkfilepoolPtr_->Size());

    currentStat = ChunkfilepoolPtr_->GetState();
    ASSERT_EQ(50, currentStat.preallocatedChunksLeft);

    ASSERT_FALSE(fsptr->FileExists("./new1"));
    ASSERT_TRUE(fsptr->FileExists("./cspooltest/chunkfilepool/4"));
    ASSERT_EQ(0, fsptr->Delete("./cspooltest/chunkfilepool/4"));
}

TEST(CSChunkfilePool, GetChunkDirectlyTest) {
    std::shared_ptr<ChunkfilePool>  ChunkfilepoolPtr_;
    std::shared_ptr<LocalFileSystem>  fsptr;
    fsptr = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

    // create chunkfile in chunkfile pool dir
    // if chunkfile pool 的getchunkfrompool开关关掉了，那么
    // chunkfilepool的size是一直为0，不会从pool目录中找
    std::string  filename = "./cspooltest/chunkfilepool/1000";
    fsptr->Mkdir("./cspooltest/chunkfilepool");
    int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);

    char data[8192];
    memset(data, 'a', 8192);
    ASSERT_EQ(8192, fsptr->Write(fd, data, 0, 8192));
    fsptr->Close(fd);
    ASSERT_TRUE(fsptr->FileExists("./cspooltest/chunkfilepool/1000"));

    ChunkfilePoolOptions cspopt;
    cspopt.getChunkFromPool = false;
    cspopt.chunkSize = 16 * 1024;
    cspopt.metaPageSize = 4 * 1024;
    cspopt.cpMetaFileSize = 4 * 1024;
    cspopt.retryTimes = 5;
    strcpy(cspopt.chunkFilePoolDir, "./cspooltest/chunkfilepool");                             // NOLINT

    ChunkfilepoolPtr_ = std::make_shared<ChunkfilePool>(fsptr);
    if (ChunkfilepoolPtr_ == nullptr) {
        LOG(FATAL) << "allocate chunkfile pool failed!";
    }
    ASSERT_TRUE(ChunkfilepoolPtr_->Initialize(cspopt));
    ASSERT_EQ(0, ChunkfilepoolPtr_->Size());

    // 测试获取chunk，chunkfile pool size不变一直为0
    char metapage[4096];
    memset(metapage, '1', 4096);

    ASSERT_EQ(0, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_EQ(0, ChunkfilepoolPtr_->Size());

    ASSERT_TRUE(fsptr->FileExists("./new1"));
    fd = fsptr->Open("./new1", O_RDWR);
    ASSERT_GE(fd, 0);

    char buf[4096];
    ASSERT_EQ(4096, fsptr->Read(fd, buf, 0, 4096));
    for (int i = 0; i < 4096; i++) {
        ASSERT_EQ(buf[i], '1');
    }

    // 测试回收chunk,文件被删除，chunkfilepool Size不受影响
    ChunkfilepoolPtr_->RecycleChunk("./new1");
    ASSERT_EQ(0, ChunkfilepoolPtr_->Size());
    ASSERT_FALSE(fsptr->FileExists("./new1"));

    // 删除测试文件及目录
    ASSERT_EQ(0, fsptr->Close(fd));
    ASSERT_EQ(0, fsptr->Delete("./cspooltest/chunkfilepool/1000"));
    ASSERT_EQ(0, fsptr->Delete("./cspooltest/chunkfilepool"));
    ChunkfilepoolPtr_->UnInitialize();
}
