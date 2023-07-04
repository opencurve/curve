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
}

TEST_F(VFSTest, Umount) {
}

TEST_F(VFSTest, MkDir) {
    auto vfs = VFSBuilder().Build();
    ASSERT_EQ(vfs.MkDir("/"), CURVEFS_ERROR::EXISTS);
    ASSERT_EQ(vfs.MkDir("/d1/d2"), CURVEFS_ERROR::NOT_EXISTS);
    ASSERT_EQ(vfs.MkDir("/d1"), CURVEFS_ERROR::OK);
}

TEST_F(VFSTest, MkDirs) {
    auto vfs = VFSBuilder().Build();
    ASSERT_EQ(vfs.MkDirs("/"), CURVEFS_ERROR::OK);
    ASSERT_EQ(vfs.MkDirs("/d1/d2"), CURVEFS_ERROR::OK);
    ASSERT_EQ(vfs.MkDirs("/d1"), CURVEFS_ERROR::OK);
}

TEST_F(VFSTest, OpenDir) {
    auto vfs = VFSBuilder().Build();
    ASSERT_EQ(vfs->OpenDir("/d1"), CURVEFS_ERROR::NOT_EXISTS);

    ASSERT_EQ(vfs->MkDir("/d1"), CURVEFS_ERROR::OK);
    ASSERT_EQ(vfs->OpenDir("/d1"), CURVEFS_ERROR::OK);
}

TEST_F(VFSTest, ReadDir) {
    auto vfs = VFSBuilder().Build();
    ASSERT_EQ(vfs.MkDir("/d1", 0755), CURVEFS_ERROR::OK);
    ASSERT_EQ(vfs.MkDir("/d2", 0755), CURVEFS_ERROR::OK);
    ASSERT_EQ(vfs.MkDir("/d3", 0755), CURVEFS_ERROR::OK);

    DirStream stream;
    DirEntry dirEntry;
    auto rc = vfs->OpenDir("/", &stream);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);

    std::vector<std::string> out;
    for ( ; ; ) {
        rc = vfs->ReadDir(&stream, &dirEntry);
        ASSERT_EQ(rc, CURVEFS_ERROR::OK);
        out.push_back(dirEntry.name);
    }
    ASSERT_EQ(out, std::vector<std::string>{ "d1", "d2", "d3" });

    ASSERT_EQ(vfs->CloseDir(&stream), CURVEFS_ERROR::OK);
}

TEST_F(VFSTest, CloseDir) {
}

TEST_F(VFSTest, Create) {
    auto vfs = VFSBuilder().Build();

    ASSERT_EQ(vfs->Create("/f1", 0644), CURVEFS_ERROR::OK);

    ASSERT_EQ(vfs->Create("/d1/f1", 0644), CURVEFS_ERROR::NOT_EXIST);
    ASSERT_EQ(vfs.MkDir("/d1", 0755), CURVEFS_ERROR::OK);
    ASSERT_EQ(vfs->Create("/d1/f1", 0644), CURVEFS_ERROR::OK);
}

TEST_F(VFSTest, Open) {
    auto vfs = VFSBuilder().Build();

    uint64_t fd;
    auto rc = vfs->Open("/f1", O_RDWR, 0644, &fd);
    ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EXIST);

    rc = vfs->Open("/f1", O_RDWR, 0644, &fd);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
}

TEST_F(VFSTest, LSeek) {
    auto vfs = VFSBuilder().Build();
}

TEST_F(VFSTest, Read) {

    // CASE 2:
}

constexpr uint64_t kKiB = 1024ULL;
constexpr uint64_t kMiB = 1024ULL * kKiB;
constexpr uint64_t kGiB = 1024ULL * kMiB;
constexpr uint64_t kTiB = 1024ULL * kGiB;

#define OK(rc) ASSERT_EQ(rc, CURVEFS_ERROR::OK);
#define BAD_FD(rc) ASSERT_EQ(rc, CURVEFS_ERROR::BAD_FD);
#define NOT_EXIST(rc) ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EXIST);
#define NOT_PERMISSION(rc) ASSERT_EQ(rc, CURVEFS_ERROR::NO_PERMISSION);

TEST_F(VFSTest, Write) {
    auto vfs = VFSBuilder().Build();

    std::string data = "abcde";

    uint64_t fd;
    uint64_t nwritten;
    OK(vfs->Create("/f1", 0644));
    OK(vfs->Open("/f1", 0644, O_RDWR, &fd));
    OK(vfs->Write(fd, buffer, data.size(), &nwritten));
    ASSERT_EQ(nwritten, data.size());

    uint64_t nread;
    std::unique_ptr<char[]> buffer(new char[1 * KiB]);
    OK(vfs->Read(fd, buffer, data.size(), &nread));
    ASSERT_EQ(nread, data.size());
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

    NOT_EXIST(vfs->Unlink("/d1/f1"));
    OK(vfs->MkDir("/d1", 0755));

    struct stat stat;
    NOT_EXIST(vfs->LStat("/d1/f1", &stat));
}

TEST_F(VFSTest, StatFS) {
    auto vfs = VFSBuilder().Build();

    struct statvfs statvfs;
    OK(vfs->StatFS(&statvfs));
}

TEST_F(VFSTest, LStat) {
    auto vfs = VFSBuilder().Build();

    struct stat stat;
    NOT_EXIST(vfs->LStat("/f1", &stat));

    OK(vfs->Create("/f1", 0644));
    OK(vfs->LStat("/f1", &stat));
    ASSERT_EQ(stat.st_size, 0);

    uint64_t nwritten;
    std::string data = "0123456789";
    OK(vfs->Write(fd, data.c_str(), data.size(), &nwritten));
    ASSERT_EQ(nwritten, data.size());
    OK(vfs->LStat("/f1", &stat));
    ASSERT_EQ(stat.st_size, data.size());

    OK(vfs->Unlink("/f1"));
    NOT_EXIST(vfs->LStat("/f1", &stat));
}

TEST_F(VFSTest, FStat) {
    auto vfs = VFSBuilder().Build();

    struct stat stat;
    BAD_FD(vfs->LStat(0, &stat));

    uint64_t fd;
    OK(vfs->Create("/f1", 0644));
    OK(vfs->Open("/f1", O_RDWR, 0644, &fd));
    BAD_FD(vfs->LStat(fd, &stat));
    ASSERT_EQ(stat.st_size, 0);

    uint64_t nwritten;
    std::string data = "0123456789";
    OK(vfs->Write(fd, data.c_str(), data.size(), &nwritten));
    ASSERT_EQ(nwritten, data.size());
    OK(vfs->FStat(fd, &stat));
    ASSERT_EQ(stat.st_size, data.size());

    OK(vfs->Close(fd));
    BAD_FD(vfs->LStat(fd, &stat));
}

TEST_F(VFSTest, SetAttr) {
    auto builder = VFSBuilder();
    auto fs = builder.Build();
}

TEST_F(VFSTest, Chmod) {
    auto builder = VFSBuilder();
    auto fs = builder.Build();

    NOT_EXIST(vfs->Chmod("/f1", 0600));

    OK(vfs->Create("/f1", 0644));

    struct stat stat;
    OK(vfs->LStat("/f2", &stat));
    ASSERT_EQ(stat.st_mode, 0644);

    OK(vfs->Chmod("/f1", 0600));
    OK(vfs->LStat("/f2", &stat));
    ASSERT_EQ(stat.st_mode, 0600);
}

TEST_F(VFSTest, Chown) {
    auto builder = VFSBuilder();

    NOT_EXIST(vfs->Chown("/f1", 1000, 1000));

    OK(vfs->Create("/f1", 0644));

    struct stat stat;
    OK(vfs->LStat("/f2", &stat));
    ASSERT_EQ(stat.st_uid, 0);
    ASSERT_EQ(stat.st_gid, 0);

    NOT_EXIST(vfs->Chown("/f1", 1000, 1000));
    OK(vfs->LStat("/f2", &stat));
    ASSERT_EQ(stat.st_uid, 1000);
    ASSERT_EQ(stat.st_gid, 1000);
}

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

    struct stat stat;
    NOT_EXIST(vfs->LStat("/d1", &stat));
    OK(vfs->LStat("/d2", &stat));
}

TEST_F(VFSTest, Attr2Stat) {
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
