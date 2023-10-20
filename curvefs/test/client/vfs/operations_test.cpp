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

/*
 * Actually the operations is a wrapper of FuseClient, so for each operation
 * we should only check wether the param which pass to FuseClient is right.
 */

#include <gtest/gtest.h>

#include <vector>
#include <string>

#include "curvefs/test/client/filesystem/helper/helper.h"
#include "curvefs/src/client/vfs/operations.h"
#include "curvefs/test/client/vfs/helper/helper.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curvefs::client::common::FileSystemOption;
using ::curvefs::client::filesystem::MkAttr;
using ::curvefs::client::filesystem::FileSystemBuilder;
using ::curvefs::client::filesystem::AttrOption;
using ::curvefs::client::filesystem::EntryOut;

class OperationsTest : public ::testing::Test {};

TEST_F(OperationsTest, Mount) { /* skip it */ }

TEST_F(OperationsTest, Umount) { /* skip it */ }

TEST_F(OperationsTest, MkDir) {
    auto permission = PermissionBuilder().SetOption(
        [&](UserPermissionOption* option){
            option->uid = 1000;
            option->gids = std::vector<uint32_t>{ 2000 };
        }).Build();

    auto builder = OperationsBuilder(permission);
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE5(*client, FuseOpMkDir, [&](fuse_req_t req,
                                                  Ino parent,
                                                  const char* name,
                                                  uint16_t mode,
                                                  EntryOut* entryOut) {
        auto ctx = fuse_req_ctx(req);
        EXPECT_EQ(parent, 1);
        EXPECT_STREQ(name, "f1");
        EXPECT_EQ(mode, 0777);
        EXPECT_EQ(ctx->uid, 1000);
        EXPECT_EQ(ctx->gid, 2000);

        entryOut->attrTimeout = 1;
        entryOut->entryTimeout = 2;
        return CURVEFS_ERROR::OK;
    });

    EntryOut entryOut;
    OK(op->MkDir(1, "f1", 0777, &entryOut));
    ASSERT_EQ(entryOut.attrTimeout, 1);
    ASSERT_EQ(entryOut.entryTimeout, 2);

    // CASE 2: EXISTS
    EXPECT_CALL_RETURN5(*client, FuseOpMkDir, CURVEFS_ERROR::EXISTS);
    EXISTS(op->MkDir(1, "f1", 0777, &entryOut));
}

TEST_F(OperationsTest, RmDir) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE3(*client, FuseOpRmDir, [&](fuse_req_t req,
                                                  Ino parent,
                                                  const char* name) {
        EXPECT_EQ(parent, 1);
        EXPECT_STREQ(name, "f1");
        return CURVEFS_ERROR::OK;
    });

    OK(op->RmDir(1, "f1"));

    // CASE 2: NOT EXIST
    EXPECT_CALL_RETURN3(*client, FuseOpRmDir, CURVEFS_ERROR::NOT_EXIST);
    NOT_EXIST(op->RmDir(1, "f1"));
}

TEST_F(OperationsTest, OpenDir) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE3(*client, FuseOpOpenDir, [&](fuse_req_t req,
                                                    Ino ino,
                                                    fuse_file_info* fi) {
        EXPECT_EQ(ino, 100);
        fi->fh = 12345;
        return CURVEFS_ERROR::OK;
    });

    uint64_t fh;
    OK(op->OpenDir(100, &fh));
    ASSERT_EQ(fh, 12345);

    // CASE 2: NOT EXISTS
    EXPECT_CALL_RETURN3(*client, FuseOpOpenDir, CURVEFS_ERROR::NOT_EXIST);
    NOT_EXIST(op->OpenDir(100, &fh));
}

TEST_F(OperationsTest, ReadDir) { /* skip it */ }

TEST_F(OperationsTest, CloseDir) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    EXPECT_CALL_INVOKE3(*client, FuseOpReleaseDir, [&](fuse_req_t req,
                                                       Ino ino,
                                                       fuse_file_info* fi) {
        EXPECT_EQ(ino, 100);
        return CURVEFS_ERROR::OK;
    });

    OK(op->CloseDir(100));
}

TEST_F(OperationsTest, Create) {
    auto permission = PermissionBuilder().SetOption(
        [&](UserPermissionOption* option){
            option->uid = 1000;
            option->gids = std::vector<uint32_t>{ 2000, 1000 };
        }).Build();
    auto builder = OperationsBuilder(permission);
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE6(*client, FuseOpCreate, [&](fuse_req_t req,
                                                   Ino parent,
                                                   const char* name,
                                                   uint16_t mode,
                                                   fuse_file_info* fi,
                                                   EntryOut* entryOut) {
        auto ctx = fuse_req_ctx(req);
        EXPECT_EQ(parent, 1);
        EXPECT_STREQ(name, "f1");
        EXPECT_EQ(mode, 0777);
        EXPECT_EQ(ctx->uid, 1000);
        EXPECT_EQ(ctx->gid, 2000);
        return CURVEFS_ERROR::OK;
    });

    EntryOut entryOut;
    OK(op->Create(1, "f1", 0777, &entryOut));

    // CASE 2: EXISTS
    EXPECT_CALL_RETURN6(*client, FuseOpCreate, CURVEFS_ERROR::EXISTS);
    EXISTS(op->Create(1, "f1", 0777, &entryOut));
}

TEST_F(OperationsTest, Open) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE4(*client, FuseOpOpen, [&](fuse_req_t req,
                                                 Ino ino,
                                                 fuse_file_info* fi,
                                                 FileOut* fileOut) {
        EXPECT_EQ(ino, 100);
        EXPECT_EQ(fi->flags, O_RDWR);
        return CURVEFS_ERROR::OK;
    });

    OK(op->Open(100, O_RDWR));

    // CASE 2: ESTALE
    EXPECT_CALL_RETURN4(*client, FuseOpOpen, CURVEFS_ERROR::STALE);
    STALE(op->Open(100, O_RDWR));
}

TEST_F(OperationsTest, Read) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE7(*client, FuseOpRead, [&](fuse_req_t req,
                                                 Ino ino,
                                                 size_t size,
                                                 uint64_t offset,
                                                 fuse_file_info* fi,
                                                 char* buffer,
                                                 size_t* nread) {
        EXPECT_EQ(ino, 100);
        EXPECT_EQ(size, 4096);
        EXPECT_EQ(offset, 1024);
        snprintf(buffer, sizeof("xyz"), "xyz");
        *nread = 3;
        return CURVEFS_ERROR::OK;
    });

    size_t nread;
    std::unique_ptr<char[]> buffer(new char[1 * KiB]);
    OK(op->Read(100, 1024, buffer.get(), 4096, &nread));
    ASSERT_EQ(nread, 3);
    ASSERT_STREQ(buffer.get(), "xyz");

    // CASE 2: INTERNAL ERROR
    EXPECT_CALL_RETURN7(*client, FuseOpRead, CURVEFS_ERROR::INTERNAL);
    INTERNAL_ERROR(op->Read(100, 1024, buffer.get(), 4096, &nread));
}

TEST_F(OperationsTest, Write) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE7(*client, FuseOpWrite, [&](fuse_req_t req,
                                                  Ino ino,
                                                  const char* buffer,
                                                  size_t size,
                                                  off_t offset,
                                                  fuse_file_info* fi,
                                                  FileOut* fileOut) {
        EXPECT_EQ(ino, 100);
        EXPECT_EQ(strncmp(buffer, "xyz", 3), 0);
        EXPECT_EQ(size, 3);
        EXPECT_EQ(offset, 1024);
        fileOut->nwritten = 3;
        return CURVEFS_ERROR::OK;
    });

    size_t nwritten;
    char buffer[10] = { 'x', 'y', 'z' };
    OK(op->Write(100, 1024, buffer, 3, &nwritten));
    ASSERT_EQ(nwritten, 3);

    // CASE 2: INTERNAL ERROR
    EXPECT_CALL_RETURN7(*client, FuseOpWrite, CURVEFS_ERROR::INTERNAL);
    INTERNAL_ERROR(op->Write(100, 1024, buffer, 3, &nwritten));
}

TEST_F(OperationsTest, Flush) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE3(*client, FuseOpFlush, [&](fuse_req_t req,
                                                  Ino ino,
                                                  fuse_file_info* fi) {
        EXPECT_EQ(ino, 100);
        return CURVEFS_ERROR::OK;
    });

    OK(op->Flush(100));

    // CASE 2: INTERNAL ERROR
    EXPECT_CALL_RETURN3(*client, FuseOpFlush, CURVEFS_ERROR::INTERNAL);
    INTERNAL_ERROR(op->Flush(100));
}

TEST_F(OperationsTest, Close) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE3(*client, FuseOpRelease, [&](fuse_req_t req,
                                                    Ino ino,
                                                    fuse_file_info* fi) {
        EXPECT_EQ(ino, 100);
        return CURVEFS_ERROR::OK;
    });

    OK(op->Close(100));

    // CASE 2: INTERNAL ERROR
    EXPECT_CALL_RETURN3(*client, FuseOpRelease, CURVEFS_ERROR::INTERNAL);
    INTERNAL_ERROR(op->Close(100));
}

TEST_F(OperationsTest, Unlink) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE3(*client, FuseOpUnlink, [&](fuse_req_t req,
                                                   Ino parent,
                                                   const char* name) {
        EXPECT_EQ(parent, 1);
        EXPECT_STREQ(name, "f1");
        return CURVEFS_ERROR::OK;
    });

    OK(op->Unlink(1, "f1"));

    // CASE 2: NOT EXIST
    EXPECT_CALL_RETURN3(*client, FuseOpUnlink, CURVEFS_ERROR::NOT_EXIST);
    NOT_EXIST(op->Unlink(1, "f1"));
}

TEST_F(OperationsTest, StatFs) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE3(*client, FuseOpStatFs, [&](fuse_req_t req,
                                                   Ino ino,
                                                   struct statvfs* statvfs) {
        EXPECT_EQ(ino, 1);
        return CURVEFS_ERROR::OK;
    });

    struct statvfs statvfs;
    OK(op->StatFs(1, &statvfs));

    // CASE 2: INTERNAL ERROR
    EXPECT_CALL_RETURN3(*client, FuseOpStatFs, CURVEFS_ERROR::INTERNAL);
    INTERNAL_ERROR(op->StatFs(1, &statvfs));
}

TEST_F(OperationsTest, Lookup) {
    auto fs = FileSystemBuilder().SetOption([&](FileSystemOption* option) {
        option->kernelCacheOption.entryTimeoutSec = 1;
        option->kernelCacheOption.attrTimeoutSec = 2;
        option->kernelCacheOption.dirEntryTimeoutSec = 3;
        option->kernelCacheOption.dirAttrTimeoutSec = 4;
    }).Build();

    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: Lookup file
    EXPECT_CALL_INVOKE0(*client, GetFileSystem, [&]() { return fs; });
    EXPECT_CALL_INVOKE4(*client, FuseOpLookup, [&](fuse_req_t req,
                                                   Ino parent,
                                                   const char* name,
                                                   EntryOut* entryOut) {
        EXPECT_EQ(parent, 1);
        EXPECT_STREQ(name, "f1");
        entryOut->attr = MkAttr(100, AttrOption().type(FsFileType::TYPE_FILE));
        return CURVEFS_ERROR::OK;
    });

    EntryOut entryOut;
    OK(op->Lookup(1, "f1", &entryOut));
    ASSERT_EQ(entryOut.attr.inodeid(), 100);
    ASSERT_EQ(entryOut.entryTimeout, 1);
    ASSERT_EQ(entryOut.attrTimeout, 2);

    // CASE 2: Lookup directory
    EXPECT_CALL_INVOKE0(*client, GetFileSystem, [&]() { return fs; });
    EXPECT_CALL_INVOKE4(*client, FuseOpLookup, [&](fuse_req_t req,
                                                   Ino parent,
                                                   const char* name,
                                                   EntryOut* entryOut) {
        EXPECT_EQ(parent, 1);
        EXPECT_STREQ(name, "d1");
        entryOut->attr = MkAttr(200,
            AttrOption().type(FsFileType::TYPE_DIRECTORY));
        return CURVEFS_ERROR::OK;
    });

    OK(op->Lookup(1, "d1", &entryOut));
    ASSERT_EQ(entryOut.attr.inodeid(), 200);
    ASSERT_EQ(entryOut.entryTimeout, 3);
    ASSERT_EQ(entryOut.attrTimeout, 4);

    // CASE 3: NOT EXIST
    EXPECT_CALL_RETURN4(*client, FuseOpLookup, CURVEFS_ERROR::NOT_EXIST);
    NOT_EXIST(op->Lookup(1, "f2", &entryOut));
}

TEST_F(OperationsTest, GetAttr) {
    auto fs = FileSystemBuilder().SetOption([&](FileSystemOption* option) {
        option->kernelCacheOption.attrTimeoutSec = 1;
        option->kernelCacheOption.dirAttrTimeoutSec = 2;
    }).Build();

    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: get file attribute
    EXPECT_CALL_INVOKE0(*client, GetFileSystem, [&]() { return fs; });
    EXPECT_CALL_INVOKE4(*client, FuseOpGetAttr, [&](fuse_req_t req,
                                                    Ino ino,
                                                    fuse_file_info* fi,
                                                    AttrOut* attrOut) {
        EXPECT_EQ(ino, 100);
        attrOut->attr = MkAttr(100, AttrOption().type(FsFileType::TYPE_FILE));
        return CURVEFS_ERROR::OK;
    });

    AttrOut attrOut;
    OK(op->GetAttr(100, &attrOut));
    ASSERT_EQ(attrOut.attr.inodeid(), 100);
    ASSERT_EQ(attrOut.attrTimeout, 1);

    // CASE 2: get directory attribute
    EXPECT_CALL_INVOKE0(*client, GetFileSystem, [&]() { return fs; });
    EXPECT_CALL_INVOKE4(*client, FuseOpGetAttr, [&](fuse_req_t req,
                                                    Ino ino,
                                                    fuse_file_info* fi,
                                                    AttrOut* attrOut) {
        EXPECT_EQ(ino, 200);
        attrOut->attr = MkAttr(200,
            AttrOption().type(FsFileType::TYPE_DIRECTORY));
        return CURVEFS_ERROR::OK;
    });

    OK(op->GetAttr(200, &attrOut));
    ASSERT_EQ(attrOut.attr.inodeid(), 200);
    ASSERT_EQ(attrOut.attrTimeout, 2);

    // CASE 3: NOT EXIST
    EXPECT_CALL_RETURN4(*client, FuseOpGetAttr, CURVEFS_ERROR::NOT_EXIST);
    NOT_EXIST(op->GetAttr(300, &attrOut));
}

TEST_F(OperationsTest, SetAttr) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE6(*client, FuseOpSetAttr, [&](fuse_req_t req,
                                                    Ino ino,
                                                    struct stat* stat,
                                                    int toSet,
                                                    struct fuse_file_info* fi,
                                                    AttrOut* attrOut) {
        EXPECT_EQ(ino, 100);
        EXPECT_EQ(stat->st_uid, 1000);
        EXPECT_EQ(stat->st_gid, 2000);
        EXPECT_EQ(toSet, AttrMask::SET_ATTR_UID | AttrMask::SET_ATTR_GID);
        return CURVEFS_ERROR::OK;
    });

    struct stat stat;
    stat.st_uid = 1000;
    stat.st_gid = 2000;
    int toSet = AttrMask::SET_ATTR_UID | AttrMask::SET_ATTR_GID;
    OK(op->SetAttr(100, &stat, toSet));

    // CASE 2: NOT EXIST
    EXPECT_CALL_RETURN6(*client, FuseOpSetAttr, CURVEFS_ERROR::NOT_EXIST);
    NOT_EXIST(op->SetAttr(100, &stat, toSet));
}

TEST_F(OperationsTest, ReadLink) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE3(*client, FuseOpReadLink, [&](fuse_req_t req,
                                                     Ino ino,
                                                     std::string* link) {
        EXPECT_EQ(ino, 100);
        *link = "d1/d2/f1";
        return CURVEFS_ERROR::OK;
    });

    std::string link;
    OK(op->ReadLink(100, &link));
    ASSERT_EQ(link, "d1/d2/f1");

    // CASE 2: NOT EXIST
    EXPECT_CALL_RETURN3(*client, FuseOpReadLink, CURVEFS_ERROR::NOT_EXIST);
    NOT_EXIST(op->ReadLink(100, &link));
}

TEST_F(OperationsTest, Rename) {
    auto builder = OperationsBuilder();
    auto op = builder.Build();
    auto client = builder.GetClient();

    // CASE 1: OK
    EXPECT_CALL_INVOKE6(*client, FuseOpRename, [&](fuse_req_t req,
                                                   Ino parent,
                                                   const char* name,
                                                   Ino newparent,
                                                   const char* newname,
                                                   unsigned int flags) {
        EXPECT_EQ(parent, 100);
        EXPECT_STREQ(name, "f1");
        EXPECT_EQ(newparent, 200);
        EXPECT_STREQ(newname, "f2");
        return CURVEFS_ERROR::OK;
    });

    OK(op->Rename(100, "f1", 200, "f2"));

    // CASE 2: EXIST
    EXPECT_CALL_RETURN6(*client, FuseOpRename, CURVEFS_ERROR::EXISTS);
    EXISTS(op->Rename(100, "f1", 200, "f2"));
}

TEST_F(OperationsTest, Attr2Stat) { /* skip it */ }

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
