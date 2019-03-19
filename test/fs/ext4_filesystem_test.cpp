/*
 * Project: curve
 * Created Date: Tuesday December 18th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <dirent.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <memory>

#include "test/fs/mock_posix_wrapper.h"
#include "src/fs/ext4_filesystem_impl.h"

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

namespace curve {
namespace fs {

ACTION_TEMPLATE(SetVoidArgPointee,
                HAS_1_TEMPLATE_PARAMS(int, k),
                AND_1_VALUE_PARAMS(first)) {
    auto output = reinterpret_cast<char*>(::testing::get<k>(args));
    *output = first;
}

class Ext4LocalFileSystemTest : public testing::Test {
 public:
        void SetUp() {
            wrapper = std::make_shared<MockPosixWrapper>();
            lfs = Ext4FileSystemImpl::getInstance();
            lfs->SetPosixWrapper(wrapper);
            errno = 1234;
        }

        void TearDown() {
            errno = 0;
            // allows the destructor of lfs_ to be invoked correctly
            Mock::VerifyAndClear(wrapper.get());
        }

 protected:
    std::shared_ptr<MockPosixWrapper> wrapper;
    std::shared_ptr<Ext4FileSystemImpl> lfs;
};

// test Statfs
TEST_F(Ext4LocalFileSystemTest, StatfsTest) {
    FileSystemInfo fsinfo;
    EXPECT_CALL(*wrapper, statfs(NotNull(), NotNull()))
        .WillOnce(Return(0));
    ASSERT_EQ(lfs->Statfs("./", &fsinfo), 0);
    EXPECT_CALL(*wrapper, statfs(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    ASSERT_EQ(lfs->Statfs("./", &fsinfo), -errno);
}

// test Open
TEST_F(Ext4LocalFileSystemTest, OpenTest) {
    EXPECT_CALL(*wrapper, open(NotNull(), _, _))
        .WillOnce(Return(666));
    ASSERT_EQ(lfs->Open("/a", 0), 666);
    EXPECT_CALL(*wrapper, open(NotNull(), _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(lfs->Open("/a", 0), -errno);
}

// test Close
TEST_F(Ext4LocalFileSystemTest, CloseTest) {
    EXPECT_CALL(*wrapper, close(_))
        .WillOnce(Return(0));
    ASSERT_EQ(lfs->Close(666), 0);
    EXPECT_CALL(*wrapper, close(_))
        .WillOnce(Return(-1));
    ASSERT_EQ(lfs->Close(666), -errno);
}

// test Delete
TEST_F(Ext4LocalFileSystemTest, DeleteTest) {
    // fake env
    {
        struct stat dirInfo;
        dirInfo.st_mode = S_IFDIR;
        struct stat fileInfo;
        fileInfo.st_mode = S_IFREG;
        // /a is a file
        EXPECT_CALL(*wrapper, stat(StrEq("/a"), NotNull()))
            .WillRepeatedly(DoAll(SetArgPointee<1>(fileInfo),
                                  Return(0)));
        // /b is a dir
        EXPECT_CALL(*wrapper, stat(StrEq("/b"), NotNull()))
            .WillRepeatedly(DoAll(SetArgPointee<1>(dirInfo),
                                  Return(0)));
        // /b/1 is a file
        EXPECT_CALL(*wrapper, stat(StrEq("/b/1"), NotNull()))
            .WillRepeatedly(DoAll(SetArgPointee<1>(fileInfo),
                                  Return(0)));

        DIR* dirp = reinterpret_cast<DIR*>(0x01);
        struct dirent entryArray[1];
        memset(entryArray, 0, sizeof(entryArray));
        memcpy(entryArray[0].d_name, "1", 1);
        EXPECT_CALL(*wrapper, opendir(StrEq("/b")))
            .WillOnce(Return(dirp));
        EXPECT_CALL(*wrapper, readdir(dirp))
            .Times(2)
            .WillOnce(Return(entryArray))
            .WillOnce(Return(nullptr));
        EXPECT_CALL(*wrapper, closedir(_))
            .WillOnce(Return(0));

        EXPECT_CALL(*wrapper, remove(NotNull()))
            .WillRepeatedly(Return(0));
    }

    // test delete dir
    {
        // success
        ASSERT_EQ(lfs->Delete("/b"), 0);

        // opendir failed
        EXPECT_CALL(*wrapper, opendir(StrEq("/b")))
            .WillOnce(Return(nullptr));
        // List will failed
        ASSERT_EQ(lfs->Delete("/b"), -errno);
    }

    // test delete file
    {
        ASSERT_EQ(lfs->Delete("/a"), 0);
        // error occured when remove file
        EXPECT_CALL(*wrapper, remove(NotNull()))
            .WillOnce(Return(-1));
        ASSERT_EQ(lfs->Delete("/a"), -errno);
    }
}

// test Mkdir
TEST_F(Ext4LocalFileSystemTest, MkdirTest) {
    ASSERT_EQ(lfs->Mkdir("/"), 0);
    struct stat info;
    info.st_mode = S_IFDIR;
    // success
    EXPECT_CALL(*wrapper, stat(StrEq("/a"), NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                        Return(0)));
    EXPECT_CALL(*wrapper, mkdir(NotNull(), _))
        .Times(0);
    ASSERT_EQ(lfs->Mkdir("/a"), 0);
    // stat failed ,mkdir success
    EXPECT_CALL(*wrapper, stat(StrEq("/a"), NotNull()))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper, mkdir(StrEq("/a"), _))
        .WillOnce(Return(0));
    ASSERT_EQ(lfs->Mkdir("/a"), 0);
    // test relative path
    EXPECT_CALL(*wrapper, stat(_, NotNull()))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(0)))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper, mkdir(StrEq("aaa/bbb"), _))
        .WillOnce(Return(0));
    ASSERT_EQ(lfs->Mkdir("aaa/bbb"), 0);
    // is not a dir, mkdir failed
    info.st_mode = S_IFREG;
    EXPECT_CALL(*wrapper, stat(StrEq("/a"), NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                        Return(0)));
    EXPECT_CALL(*wrapper, mkdir(NotNull(), _))
        .WillOnce(Return(-1));
    ASSERT_EQ(lfs->Mkdir("/a"), -errno);
}

// test DirExists
TEST_F(Ext4LocalFileSystemTest, DirExistsTest) {
    struct stat info;
    info.st_mode = S_IFDIR;
    // is dir
    EXPECT_CALL(*wrapper, stat(StrEq("/a"), NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                        Return(0)));
    ASSERT_EQ(lfs->DirExists("/a"), true);
    // stat failed
    EXPECT_CALL(*wrapper, stat(StrEq("/a"), NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                        Return(-1)));
    ASSERT_EQ(lfs->DirExists("/a"), false);
    // not dir
    info.st_mode = S_IFREG;
    EXPECT_CALL(*wrapper, stat(StrEq("/a"), NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                        Return(0)));
    ASSERT_EQ(lfs->DirExists("/a"), false);
}

// test FileExists
TEST_F(Ext4LocalFileSystemTest, FileExistsTest) {
    struct stat info;
    info.st_mode = S_IFREG;
    // is file
    EXPECT_CALL(*wrapper, stat(StrEq("/a"), NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                        Return(0)));
    ASSERT_EQ(lfs->FileExists("/a"), true);
    // stat failed
    EXPECT_CALL(*wrapper, stat(StrEq("/a"), NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                        Return(-1)));
    ASSERT_EQ(lfs->FileExists("/a"), false);
    // not file
    info.st_mode = S_IFDIR;
    EXPECT_CALL(*wrapper, stat(StrEq("/a"), NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                        Return(0)));
    ASSERT_EQ(lfs->FileExists("/a"), false);
}

// test Rename
TEST_F(Ext4LocalFileSystemTest, RenameTest) {
    EXPECT_CALL(*wrapper, rename(NotNull(), NotNull()))
        .WillOnce(Return(0));
    ASSERT_EQ(lfs->Rename("/a", "/b"), 0);
    EXPECT_CALL(*wrapper, rename(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    ASSERT_EQ(lfs->Rename("/a", "/b"), -errno);
}

// test List
TEST_F(Ext4LocalFileSystemTest, ListTest) {
    DIR* dirp = reinterpret_cast<DIR*>(0x01);
    struct dirent entryArray[3];
    memset(entryArray, 0, sizeof(entryArray));
    memcpy(entryArray[0].d_name, ".", 1);
    memcpy(entryArray[1].d_name, "..", 2);
    memcpy(entryArray[2].d_name, "1", 1);
    vector<string> names;
    // opendir failed
    EXPECT_CALL(*wrapper, opendir(StrEq("/a")))
        .WillOnce(Return(nullptr));
    ASSERT_EQ(lfs->List("/a", &names), -errno);
    // success
    EXPECT_CALL(*wrapper, opendir(StrEq("/a")))
        .WillOnce(Return(dirp));
    EXPECT_CALL(*wrapper, readdir(dirp))
        .Times(4)
        .WillOnce(Return(entryArray))
        .WillOnce(Return(entryArray + 1))
        .WillOnce(Return(entryArray + 2))
        .WillOnce(Return(nullptr));
    EXPECT_CALL(*wrapper, closedir(_))
        .WillOnce(Return(0));
    ASSERT_EQ(lfs->List("/a", &names), 0);
    ASSERT_THAT(names, ElementsAre("1"));
}

// test read
TEST_F(Ext4LocalFileSystemTest, ReadTest) {
    char buf[4] = {0};
    // success
    EXPECT_CALL(*wrapper, pread(_, NotNull(), _, _))
        .Times(3)
        .WillOnce(DoAll(SetVoidArgPointee<1>('1'), Return(1)))
        .WillOnce(DoAll(SetVoidArgPointee<1>('2'), Return(1)))
        .WillOnce(DoAll(SetVoidArgPointee<1>('3'), Return(1)));
    ASSERT_EQ(lfs->Read(666, buf, 0, 3), 3);
    ASSERT_STREQ(buf, "123");
    // out of range test
    memset(buf, 0, 4);
    EXPECT_CALL(*wrapper, pread(_, NotNull(), _, _))
        .WillOnce(DoAll(SetVoidArgPointee<1>('1'), Return(1)))
        .WillOnce(DoAll(SetVoidArgPointee<1>('2'), Return(1)))
        .WillOnce(Return(0));
    ASSERT_EQ(lfs->Read(666, buf, 0, 3), 2);
    ASSERT_STREQ(buf, "12");
    // pread failed
    EXPECT_CALL(*wrapper, pread(_, NotNull(), _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(lfs->Read(666, buf, 0, 3), -errno);
    // set errno = EINTR,and will repeatedly return -1
    errno = EINTR;
    EXPECT_CALL(*wrapper, pread(_, NotNull(), _, _))
        .WillRepeatedly(Return(-1));
    ASSERT_EQ(lfs->Read(666, buf, 0, 3), -errno);
    // set errno = EINTR,but only return -1 once
    errno = EINTR;
    EXPECT_CALL(*wrapper, pread(_, NotNull(), _, _))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(3));
    ASSERT_EQ(lfs->Read(666, buf, 0, 3), 3);
}

// test write
TEST_F(Ext4LocalFileSystemTest, WriteTest) {
    char buf[4] = {0};
    // success
    EXPECT_CALL(*wrapper, pwrite(_, buf, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper, pwrite(_, buf + 1, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper, pwrite(_, buf + 2, _, _))
        .WillOnce(Return(1));
    ASSERT_EQ(lfs->Write(666, buf, 0, 3), 3);
    // pwrite failed
    EXPECT_CALL(*wrapper, pwrite(_, NotNull(), _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(lfs->Write(666, buf, 0, 3), -errno);
    // set errno = EINTR,and will repeatedly return -1
    errno = EINTR;
    EXPECT_CALL(*wrapper, pwrite(_, NotNull(), _, _))
        .WillRepeatedly(Return(-1));
    ASSERT_EQ(lfs->Write(666, buf, 0, 3), -errno);
    // set errno = EINTR,but only return -1 once
    errno = EINTR;
    EXPECT_CALL(*wrapper, pwrite(_, NotNull(), _, _))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(3));
    ASSERT_EQ(lfs->Write(666, buf, 0, 3), 3);
}

// test Fallocate
TEST_F(Ext4LocalFileSystemTest, FallocateTest) {
    // success
    EXPECT_CALL(*wrapper, fallocate(_, _, _, _))
        .WillOnce(Return(0));
    ASSERT_EQ(lfs->Fallocate(666, 0, 0, 4096), 0);
    // fallocate failed
    EXPECT_CALL(*wrapper, fallocate(_, _, _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(lfs->Fallocate(666, 0, 0, 4096), -errno);
}

// test Fstat
TEST_F(Ext4LocalFileSystemTest, FstatTest) {
    struct stat info;
    // success
    EXPECT_CALL(*wrapper, fstat(_, _))
        .WillOnce(Return(0));
    ASSERT_EQ(lfs->Fstat(666, &info), 0);
    // fallocate failed
    EXPECT_CALL(*wrapper, fstat(_, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(lfs->Fstat(666, &info), -errno);
}

// test Fsync
TEST_F(Ext4LocalFileSystemTest, FsyncTest) {
    // success
    EXPECT_CALL(*wrapper, fsync(_))
        .WillOnce(Return(0));
    ASSERT_EQ(lfs->Fsync(666), 0);
    // fallocate failed
    EXPECT_CALL(*wrapper, fsync(_))
        .WillOnce(Return(-1));
    ASSERT_EQ(lfs->Fsync(666), -errno);
}

TEST_F(Ext4LocalFileSystemTest, ReadRealTest) {
    std::shared_ptr<PosixWrapper> pw = std::make_shared<PosixWrapper>();
    lfs->SetPosixWrapper(pw);
    int fd = lfs->Open("a", O_CREAT|O_RDWR);
    ASSERT_LT(0, fd);  // 0 < fd
    char buf[8192] = {0};
    ASSERT_EQ(4096, lfs->Write(fd, buf, 0, 4096));
    ASSERT_EQ(4096, lfs->Read(fd, buf, 0, 8192));
    ASSERT_EQ(0, lfs->Close(0));
    ASSERT_EQ(0, lfs->Delete("a"));
    FileSystemInfo fsinfo;
    ASSERT_EQ(0, lfs->Statfs("./", &fsinfo));
    ASSERT_TRUE(fsinfo.allocated == fsinfo.stored);
    ASSERT_TRUE(fsinfo.total >= fsinfo.available + fsinfo.stored);
}

}  // namespace fs
}  // namespace curve
