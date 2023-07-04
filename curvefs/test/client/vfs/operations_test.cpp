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

TEST_F(OperationsTest, MkDir) {
    auto client = std::make_shared<MockFuseClient>();
    auto userPerm = UserPermissionOption{
        uid: 1000,
        gids: std::vector<uint32_t>{ 2000, 1000 },
    };
    auto op = std::make_shared<OperationsImpl>(client, userPerm);

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpMkDir(*client, [&](fuse_req_t req,
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
    auto rc = op->MkDir(1, "f1", 0777, &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    ASSERT_EQ(entryOut.attrTimeout, 1);
    ASSERT_EQ(entryOut.entryTimeout, 2);

    // CASE 2: EXISTS
    EXPECT_CALL_RETURN_FuseOpMkDir(*client, CURVEFS_ERROR::EXISTS);
    rc = op->MkDir(1, "f1", 0777, &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::EXISTS);
}

TEST_F(OperationsTest, RmDir) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpRmDir(*client, [&](fuse_req_t req,
                                                Ino parent,
                                                const char* name) {
        EXPECT_EQ(parent, 1);
        EXPECT_STREQ(name, "f1");
        return CURVEFS_ERROR::OK;
    });

    auto rc = op->RmDir(1, "f1");
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);

    // CASE 2: NOT EXISTS
    EXPECT_CALL_RETURN_FuseOpRmDir(*client, CURVEFS_ERROR::NOT_EXIST);
    rc = op->RmDir(1, "f1");
    ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EXIST);
}

TEST_F(OperationsTest, OpenDir) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpOpenDir(*client, [&](fuse_req_t req,
                                                  Ino ino,
                                                  fuse_file_info* fi) {
        EXPECT_EQ(ino, 100);
        fi->fh = 12345;
        return CURVEFS_ERROR::OK;
    });

    uint64_t fh;
    auto rc = op->OpenDir(100, &fh);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    ASSERT_EQ(fh, 12345);

    // CASE 2: NOT EXISTS
    EXPECT_CALL_RETURN_FuseOpOpenDir(*client, CURVEFS_ERROR::NOT_EXIST);
    rc = op->OpenDir(100, &fh);
    ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EXIST);
}

TEST_F(OperationsTest, ReadDir) {
    // TODO(Wine93): let it works.
}

TEST_F(OperationsTest, CloseDir) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    EXPECT_CALL_INVOKE_FuseOpReleaseDir(*client, [&](fuse_req_t req,
                                                     Ino ino,
                                                     fuse_file_info* fi) {
        EXPECT_EQ(ino, 100);
        return CURVEFS_ERROR::OK;
    });

    auto rc = op->CloseDir(100);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
}

TEST_F(OperationsTest, Create) {
    auto client = std::make_shared<MockFuseClient>();
    auto userPerm = UserPermissionOption{
        uid: 1000,
        gids: std::vector<uint32_t>{ 2000, 1000 },
    };
    auto op = std::make_shared<OperationsImpl>(client, userPerm);

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpCreate(*client, [&](fuse_req_t req,
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
    auto rc = op->Create(1, "f1", 0777, &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);

    // CASE 2: EXISTS
    EXPECT_CALL_RETURN_FuseOpCreate(*client, CURVEFS_ERROR::EXISTS);
    rc = op->Create(1, "f1", 0777, &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::EXISTS);
}

TEST_F(OperationsTest, Open) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpOpen(*client, [&](fuse_req_t req,
                                               Ino ino,
                                               fuse_file_info* fi,
                                               FileOut* fileOut) {

        EXPECT_EQ(ino, 100);
        EXPECT_EQ(fi->flags, O_RDWR);
        return CURVEFS_ERROR::OK;
    });

    auto rc = op->Open(100, O_RDWR);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);

    // CASE 2: ESTALE
    EXPECT_CALL_RETURN_FuseOpOpen(*client, CURVEFS_ERROR::STALE);
    rc = op->Open(100, O_RDWR);
    ASSERT_EQ(rc, CURVEFS_ERROR::STALE);
}

TEST_F(OperationsTest, Read) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpRead(*client, [&](fuse_req_t req,
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
    char buffer[8192];
    auto rc = op->Read(100, 1024, buffer, 4096, &nread);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    ASSERT_EQ(nread, 3);
    ASSERT_STREQ(buffer, "xyz");

    // CASE 2: INTERNAL ERROR
     EXPECT_CALL_RETURN_FuseOpRead(*client, CURVEFS_ERROR::INTERNAL);
    rc = op->Read(100, 1024, buffer, 4096, &nread);
    ASSERT_EQ(rc, CURVEFS_ERROR::INTERNAL);
}

TEST_F(OperationsTest, Write) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpWrite(*client, [&](fuse_req_t req,
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
    auto rc = op->Write(100, 1024, buffer, 3, &nwritten);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    ASSERT_EQ(nwritten, 3);

    // CASE 2: INTERNAL ERROR
    EXPECT_CALL_RETURN_FuseOpWrite(*client, CURVEFS_ERROR::INTERNAL);
    rc = op->Write(100, 1024, buffer, 3, &nwritten);
    ASSERT_EQ(rc, CURVEFS_ERROR::INTERNAL);
}

TEST_F(OperationsTest, Flush) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpFlush(*client, [&](fuse_req_t req,
                                                Ino ino,
                                                fuse_file_info* fi) {
        EXPECT_EQ(ino, 100);
        return CURVEFS_ERROR::OK;
    });

    auto rc = op->Flush(100);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);

    // CASE 2: INTERNAL ERROR
    EXPECT_CALL_RETURN_FuseOpFlush(*client, CURVEFS_ERROR::INTERNAL);
    rc = op->Flush(100);
    ASSERT_EQ(rc, CURVEFS_ERROR::INTERNAL);
}

TEST_F(OperationsTest, Close) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpRelease(*client, [&](fuse_req_t req,
                                                  Ino ino,
                                                  fuse_file_info* fi) {
        EXPECT_EQ(ino, 100);
        return CURVEFS_ERROR::OK;
    });

    auto rc = op->Close(100);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);

    // CASE 2: INTERNAL ERROR
    EXPECT_CALL_RETURN_FuseOpRelease(*client, CURVEFS_ERROR::INTERNAL);
    rc = op->Close(100);
    ASSERT_EQ(rc, CURVEFS_ERROR::INTERNAL);
}

TEST_F(OperationsTest, Unlink) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpUnlink(*client, [&](fuse_req_t req,
                                                 Ino parent,
                                                 const char* name) {
        EXPECT_EQ(parent, 1);
        EXPECT_STREQ(name, "f1");
        return CURVEFS_ERROR::OK;
    });

    auto rc = op->Unlink(1, "f1");
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);

    // CASE 2: NOT EXIST
    EXPECT_CALL_RETURN_FuseOpUnlink(*client, CURVEFS_ERROR::NOT_EXIST);
    rc = op->Unlink(1, "f1");
    ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EXIST);
}

TEST_F(OperationsTest, StatFS) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE_FuseOpStatFs(*client, [&](fuse_req_t req,
                                                 Ino ino,
                                                 struct statvfs* statvfs) {
        EXPECT_EQ(ino, 1);
        return CURVEFS_ERROR::OK;
    });

    struct statvfs statvfs;
    auto rc = op->StatFS(1, &statvfs);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);

    // CASE 2: INTERNAL ERROR
    EXPECT_CALL_RETURN_FuseOpStatFs(*client, CURVEFS_ERROR::INTERNAL);
    rc = op->StatFS(1, &statvfs);
    ASSERT_EQ(rc, CURVEFS_ERROR::INTERNAL);
}

TEST_F(OperationsTest, Lookup) {
    auto builder = FileSystemBuilder().SetOption([&](FileSystemOption* option) {
        option->kernelCacheOption.entryTimeoutSec = 1;
        option->kernelCacheOption.attrTimeoutSec = 2;
        option->kernelCacheOption.dirEntryTimeoutSec = 3;
        option->kernelCacheOption.dirAttrTimeoutSec = 4;
    });
    auto fs = builder.Build();
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(
        client, fs, UserPermissionOption());

    // CASE 1: Lookup file
    EXPECT_CALL_INVOKE_FuseOpLookup(*client, [&](fuse_req_t req,
                                                 Ino parent,
                                                 const char* name,
                                                 EntryOut* entryOut) {
        EXPECT_EQ(parent, 1);
        EXPECT_STREQ(name, "f1");
        entryOut->attr = MkAttr(100, AttrOption().type(FsFileType::TYPE_FILE));
        return CURVEFS_ERROR::OK;
    });

    EntryOut entryOut;
    auto rc = op->Lookup(1, "f1", &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    ASSERT_EQ(entryOut.attr.inodeid(), 100);
    ASSERT_EQ(entryOut.entryTimeout, 1);
    ASSERT_EQ(entryOut.attrTimeout, 2);

    // CASE 2: Lookup directory
    EXPECT_CALL_INVOKE_FuseOpLookup(*client, [&](fuse_req_t req,
                                                 Ino parent,
                                                 const char* name,
                                                 EntryOut* entryOut) {
        EXPECT_EQ(parent, 1);
        EXPECT_STREQ(name, "d1");
        entryOut->attr = MkAttr(200,
            AttrOption().type(FsFileType::TYPE_DIRECTORY));
        return CURVEFS_ERROR::OK;
    });

    rc = op->Lookup(1, "d1", &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    ASSERT_EQ(entryOut.attr.inodeid(), 200);
    ASSERT_EQ(entryOut.entryTimeout, 3);
    ASSERT_EQ(entryOut.attrTimeout, 4);

    // CASE 3: NOT EXIST
    EXPECT_CALL_RETURN_FuseOpLookup(*client, CURVEFS_ERROR::NOT_EXIST);
    rc = op->Lookup(1, "f2", &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EXIST);
}

TEST_F(OperationsTest, GetAttr) {
}

TEST_F(OperationsTest, SetAttr) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

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
    auto rc = op->SetAttr(100, &stat, toSet);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);

    // CASE 2: NOT EXIST
    EXPECT_CALL_RETURN6(*client, FuseOpSetAttr, CURVEFS_ERROR::NOT_EXIST);
    rc = op->SetAttr(100, &stat, toSet);
    ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EXIST);
}

TEST_F(OperationsTest, ReadLink) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE3(*client, FuseOpReadLink, [&](fuse_req_t req,
                                                     Ino ino,
                                                     std::string* link) {
        EXPECT_EQ(ino, 100);
        *link = "d1/d2/f1";
        return CURVEFS_ERROR::OK;
    });

    std::string link;
    auto rc = op->ReadLink(100, &link);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    ASSERT_EQ(link, "d1/d2/f1");

    // CASE 2: NOT EXIST
    EXPECT_CALL_RETURN3(*client, FuseOpReadLink, CURVEFS_ERROR::NOT_EXIST);
    rc = op->ReadLink(100, &link);
    ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EXIST);
}

TEST_F(OperationsTest, Rename) {
    auto client = std::make_shared<MockFuseClient>();
    auto op = std::make_shared<OperationsImpl>(client, UserPermissionOption());

    // CASE 1: OK
    EXPECT_CALL_INVOKE6(*client, FuseOpRename, [&](fuse_req_t req,
                                                   Ino parent,
                                                   const char *name,
                                                   Ino newparent,
                                                   const char *newname,
                                                   unsigned int flags) {
        EXPECT_EQ(parent, 100);
        EXPECT_STREQ(name, "f1");
        EXPECT_EQ(newparent, 200);
        EXPECT_STREQ(newname, "f2");
        return CURVEFS_ERROR::OK;
    });

    auto rc = op->Rename(100, "f1", 200, "f2");
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);

    // CASE 2: EXIST
    EXPECT_CALL_RETURN6(*client, FuseOpRename, CURVEFS_ERROR::EXISTS);
    rc = op->Rename(100, "f1", 200, "f2");
    ASSERT_EQ(rc, CURVEFS_ERROR::EXISTS);
}

TEST_F(OperationsTest, Attr2Stat) {
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
