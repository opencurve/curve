/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-09-20
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include <vector>
#include <string>

#include "curvefs/src/client/vfs/handlers.h"
#include "curvefs/test/client/vfs/helper/helper.h"

namespace curvefs {
namespace client {
namespace vfs {

class VFSTest : public ::testing::Test {};

TEST_F(VFSTest, Mount) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->Mount("myfs", "/mnt/myfs"));
}

TEST_F(VFSTest, Umount) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->Umount());
}

TEST_F(VFSTest, MkDir) {
    auto vfs = VFSBuilder().Build();

    EXISTS(vfs->MkDir("/", 0755));

    NOT_EXIST(vfs->MkDir("/d1/d2", 0755));
    OK(vfs->MkDir("/d1", 0755));
    OK(vfs->MkDir("/d1/d2", 0755));
}

TEST_F(VFSTest, MkDirs) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->MkDirs("/", 0755));

    OK(vfs->MkDirs("/d1/d2", 0755));
    OK(vfs->MkDirs("/d1", 0755));
}

TEST_F(VFSTest, RmDir) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->MkDir("/d1", 0755));
    EXISTS(vfs->MkDir("/d1", 0755));

    OK(vfs->RmDir("/d1"));
    OK(vfs->MkDir("/d1", 0755));
}

TEST_F(VFSTest, OpenDir) {
    auto vfs = VFSBuilder().Build();

    DirStream stream;
    NOT_EXIST(vfs->OpenDir("/d1", &stream));

    OK(vfs->MkDir("/d1", 0755));
    OK(vfs->OpenDir("/d1", &stream));
}

TEST_F(VFSTest, ReadDir) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->MkDir("/d1", 0755));
    OK(vfs->MkDir("/d2", 0755));
    OK(vfs->MkDir("/d3", 0755));

    // OpenDir
    DirStream stream;
    OK(vfs->OpenDir("/", &stream));

    // ReadDir
    DirEntry dirEntry;
    std::vector<std::string> out;
    for ( ; ; ) {
        auto rc = vfs->ReadDir(&stream, &dirEntry);
        if (rc == CURVEFS_ERROR::END_OF_FILE) {
            break;
        }
        out.push_back(dirEntry.name);
    }
    auto entries = std::vector<std::string>{ "d1", "d2", "d3" };
    ASSERT_EQ(out, entries);

    // CloseDir
    OK(vfs->CloseDir(&stream));
}

TEST_F(VFSTest, CloseDir) {
    auto vfs = VFSBuilder().Build();

    DirStream stream;
    stream.ino = 100;
    NOT_EXIST(vfs->CloseDir(&stream));

    OK(vfs->OpenDir("/", &stream));
    OK(vfs->CloseDir(&stream));
}

TEST_F(VFSTest, Create) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->Create("/f1", 0644));

    NOT_EXIST(vfs->Create("/d1/f1", 0644));
    OK(vfs->MkDir("/d1", 0755));
    OK(vfs->Create("/d1/f1", 0644));

    OK(vfs->MkDir("/d1/d2", 0755));
    OK(vfs->Create("/d1/d2/f1", 0644));
}

TEST_F(VFSTest, Open) {
    auto vfs = VFSBuilder().Build();

    uint64_t fd;
    NOT_EXIST(vfs->Open("/f1", O_RDWR, 0644, &fd));

    OK(vfs->Create("/f1", 0644));
    OK(vfs->Open("/f1", O_RDWR, 0644, &fd));
    ASSERT_EQ(fd, 0);

    // Open again
    OK(vfs->Open("/f1", O_RDWR, 0644, &fd));
    ASSERT_EQ(fd, 1);
}

#define TEST_WRITE(path, data)                                    \
do {                                                              \
    uint64_t fd;                                                  \
    uint64_t nwritten;                                            \
    std::string buffer(data);                                     \
    OK(vfs->Open(path, 0644, O_RDWR, &fd));                       \
    OK(vfs->Write(fd, buffer.c_str(), buffer.size(), &nwritten)); \
    ASSERT_EQ(nwritten, buffer.size());                           \
} while (0)

#define TEST_READ(path, count, data)                   \
do {                                                   \
    uint64_t fd;                                       \
    uint64_t nread;                                    \
    std::unique_ptr<char[]> buffer(new char[1 * KiB]); \
    OK(vfs->Open(path, 0644, O_RDWR, &fd));            \
    OK(vfs->Read(fd, buffer.get(), count, &nread));    \
    ASSERT_EQ(nread, std::string(data).size());        \
    ASSERT_EQ(std::string(buffer.get(), nread), data); \
} while (0)

TEST_F(VFSTest, LSeek_SeekSet) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->Create("/f1", 0644));

    // content => abcde
    TEST_WRITE("/f1", "abcde");

    // SEEK_SET, write; content => abxxe
    uint64_t fd;
    uint64_t nwritten;
    OK(vfs->Open("/f1", 0644, O_RDWR, &fd));
    OK(vfs->LSeek(fd, 2, SEEK_SET));
    OK(vfs->Write(fd, "xx", 2, &nwritten));
    ASSERT_EQ(nwritten, 2);

    TEST_READ("/f1", 1 * KiB, "abxxe");
}

TEST_F(VFSTest, LSeek_SeekCur) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->Create("/f1", 0644));

    // content => abcde
    TEST_WRITE("/f1", "abcde");

    // SEEK_CUR, write; content => abcxe
    uint64_t fd;
    uint64_t nwritten;
    OK(vfs->Open("/f1", 0644, O_RDWR, &fd));
    OK(vfs->LSeek(fd, 1, SEEK_CUR));
    OK(vfs->LSeek(fd, 2, SEEK_CUR));
    OK(vfs->Write(fd, "x", 1, &nwritten));
    ASSERT_EQ(nwritten, 1);

    TEST_READ("/f1", 1 * KiB, "abcxe");
}

TEST_F(VFSTest, LSeek_SeekEnd) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->Create("/f1", 0644));

    // content => abc
    TEST_WRITE("/f1", "abc");

    // SEEK_END, write; content => abcxxx
    uint64_t fd;
    uint64_t nwritten;
    OK(vfs->Open("/f1", 0644, O_RDWR, &fd));
    OK(vfs->LSeek(fd, 0, SEEK_END));
    OK(vfs->Write(fd, "xxx", 3, &nwritten));
    ASSERT_EQ(nwritten, 3);

    TEST_READ("/f1", 1 * KiB, "abcxxx");
}

TEST_F(VFSTest, Read) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->Create("/f1", 0644));

    TEST_READ("/f1", 1 * KiB, "");

    TEST_WRITE("f1", "abcde");
    TEST_READ("/f1", 3, "abc");
    TEST_READ("/f1", 1 * KiB, "abcde");
    TEST_READ("/f1", 0, "");

    TEST_WRITE("f1", "xxx");
    TEST_READ("/f1", 1 * KiB, "xxxde");
}

TEST_F(VFSTest, Write) {
    auto vfs = VFSBuilder().Build();

    OK(vfs->Create("/f1", 0644));

    TEST_WRITE("/f1", "abcde");
    TEST_READ("/f1", 1 * KiB, "abcde");

    TEST_WRITE("/f1", "0123456789");
    TEST_READ("/f1", 1 * KiB, "0123456789");
}

TEST_F(VFSTest, FSync) {
    auto vfs = VFSBuilder().Build();

    BAD_FD(vfs->FSync(0));
    BAD_FD(vfs->FSync(100));

    uint64_t fd;
    OK(vfs->Create("/f1", 0644));
    OK(vfs->Open("/f1", 0644, O_RDWR, &fd));
    OK(vfs->FSync(fd));
}

TEST_F(VFSTest, Close) {
    auto vfs = VFSBuilder().Build();

    BAD_FD(vfs->Close(100));

    uint64_t fd;
    OK(vfs->Create("/f1", 0644));
    OK(vfs->Open("/f1", 0644, O_RDWR, &fd));
    OK(vfs->Close(fd));
}

TEST_F(VFSTest, Unlink) {
    auto vfs = VFSBuilder().Build();

    struct stat stat;
    OK(vfs->Create("/f1", 0644));
    OK(vfs->LStat("/f1", &stat));

    OK(vfs->Unlink("/f1"));
    NOT_EXIST(vfs->LStat("/f1", &stat));
}

TEST_F(VFSTest, StatFs) {
    auto vfs = VFSBuilder().Build();

    struct statvfs statvfs;
    OK(vfs->StatFs(&statvfs));
    ASSERT_EQ(statvfs.f_blocks, 1UL << 30);
    ASSERT_EQ(statvfs.f_files, 1UL << 30);
}

TEST_F(VFSTest, LStat) {
    auto vfs = VFSBuilder().Build();

    struct stat stat;
    NOT_EXIST(vfs->LStat("/f1", &stat));

    OK(vfs->Create("/f1", 0644));
    OK(vfs->LStat("/f1", &stat));
    ASSERT_EQ(stat.st_size, 0);

    std::string data = "0123456789";
    TEST_WRITE("/f1", data);

    OK(vfs->LStat("/f1", &stat));
    ASSERT_EQ(stat.st_size, data.size());

    OK(vfs->Unlink("/f1"));
    NOT_EXIST(vfs->LStat("/f1", &stat));
}

TEST_F(VFSTest, FStat) {
    auto vfs = VFSBuilder().Build();

    struct stat stat;
    BAD_FD(vfs->FStat(0, &stat));

    uint64_t fd;
    OK(vfs->Create("/f1", 0644));
    OK(vfs->Open("/f1", O_RDWR, 0644, &fd));
    OK(vfs->FStat(fd, &stat));
    ASSERT_EQ(stat.st_size, 0);

    std::string data = "0123456789";
    TEST_WRITE("/f1", data);

    OK(vfs->FStat(fd, &stat));
    ASSERT_EQ(stat.st_size, data.size());

    OK(vfs->Close(fd));
    BAD_FD(vfs->FStat(fd, &stat));
}

TEST_F(VFSTest, SetAttr) {
    auto vfs = VFSBuilder().Build();

    struct stat stat;
    NOT_EXIST(vfs->SetAttr("/f1", &stat, 0));

    OK(vfs->Create("/f1", 0644));

    stat.st_size = 4 * KiB;
    OK(vfs->SetAttr("/f1", &stat, AttrMask::SET_ATTR_SIZE));
    ASSERT_EQ(stat.st_size, 4 * KiB);
}

TEST_F(VFSTest, SetAttr_Permission) { /* skip it */ }

TEST_F(VFSTest, Chmod) {
    auto vfs = VFSBuilder().Build();

    NOT_EXIST(vfs->Chmod("/f1", 0600));

    OK(vfs->Create("/f1", 0777));

    struct stat stat;
    OK(vfs->LStat("/f1", &stat));
    ASSERT_EQ(stat.st_mode, S_IFREG | 0755);

    OK(vfs->Chmod("/f1", 0600));
    OK(vfs->LStat("/f1", &stat));
    ASSERT_EQ(stat.st_mode, S_IFREG | 0600);
}

TEST_F(VFSTest, Chmod_Permission) { /* skip it */ }

TEST_F(VFSTest, Chown) {
    auto vfs = VFSBuilder().Build();

    NOT_EXIST(vfs->Chown("/f1", 1000, 1000));

    OK(vfs->Create("/f1", 0644));

    struct stat stat;
    OK(vfs->LStat("/f1", &stat));
    ASSERT_EQ(stat.st_uid, 0);
    ASSERT_EQ(stat.st_gid, 0);

    OK(vfs->Chown("/f1", 1000, 1000));
    OK(vfs->LStat("/f1", &stat));
    ASSERT_EQ(stat.st_uid, 1000);
    ASSERT_EQ(stat.st_gid, 1000);
}

TEST_F(VFSTest, Chown_Permission) { /* skip it */ }

TEST_F(VFSTest, Rename) {
    auto vfs = VFSBuilder().Build();

    // CASE 1: rename file
    NOT_EXIST(vfs->Rename("/f1", "/f2"));

    OK(vfs->Create("/f1", 0644));
    OK(vfs->Rename("/f1", "/f2"));

    struct stat stat;
    NOT_EXIST(vfs->LStat("/f1", &stat));
    OK(vfs->LStat("/f2", &stat));

    // CASE 2: rename directory
    NOT_EXIST(vfs->Rename("/d1", "/d2"));

    OK(vfs->MkDir("/d1", 0755));
    OK(vfs->Rename("/d1", "/d2"));

    NOT_EXIST(vfs->LStat("/d1", &stat));
    OK(vfs->LStat("/d2", &stat));
}

TEST_F(VFSTest, Attr2Stat) { /* skip it */ }

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
