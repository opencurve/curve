
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
 * Created Date: 2023-04-03
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include "curvefs/test/client/filesystem/helper/helper.h"
#include "curvefs/src/client/filesystem/filesystem.h"

namespace curvefs {
namespace client {
namespace filesystem {

class FileSystemTest : public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(FileSystemTest, Attr2Stat) {
    InodeAttr attr = MkAttr(100, AttrOption()
        .type(FsFileType::TYPE_S3)
        .mode(33216)
        .nlink(2)
        .uid(1000)
        .gid(1001)
        .length(4096)
        .rdev(2048)
        .atime(100, 101)
        .mtime(200, 201)
        .ctime(300, 301));

    // CASE 1: check stat field one by one
    struct stat stat;
    auto fs = FileSystemBuilder().Build();
    fs->Attr2Stat(&attr, &stat);
    ASSERT_EQ(stat.st_ino, 100);
    ASSERT_EQ(stat.st_mode, 33216);
    ASSERT_EQ(stat.st_nlink, 2);
    ASSERT_EQ(stat.st_uid, 1000);
    ASSERT_EQ(stat.st_gid, 1001);
    ASSERT_EQ(stat.st_size, 4096);
    ASSERT_EQ(stat.st_rdev, 2048);
    ASSERT_EQ(stat.st_atim.tv_sec, 100);
    ASSERT_EQ(stat.st_atim.tv_nsec, 101);
    ASSERT_EQ(stat.st_mtim.tv_sec, 200);
    ASSERT_EQ(stat.st_mtim.tv_nsec, 201);
    ASSERT_EQ(stat.st_ctim.tv_sec, 300);
    ASSERT_EQ(stat.st_ctim.tv_nsec, 301);
    ASSERT_EQ(stat.st_blksize, 0x10000u);
    ASSERT_EQ(stat.st_blocks, 8);

    // CASE 2: convert all kind of file types
    std::vector<FsFileType> types = {
        FsFileType::TYPE_DIRECTORY,
        FsFileType::TYPE_FILE,
        FsFileType::TYPE_SYM_LINK,
    };
    for (const auto& type : types) {
        attr.set_type(type);
        fs->Attr2Stat(&attr, &stat);
        ASSERT_EQ(stat.st_blocks, 0);
    }
}

TEST_F(FileSystemTest, Entry2Param) {
    EntryOut entryOut;
    entryOut.attr = MkAttr(100, AttrOption().length(4096));
    entryOut.entryTimeout = 1;
    entryOut.attrTimeout = 2;

    fuse_entry_param e;
    auto fs = FileSystemBuilder().Build();
    fs->Entry2Param(&entryOut, &e);
    ASSERT_EQ(e.ino, 100);
    ASSERT_EQ(e.attr.st_size, 4096);
    ASSERT_EQ(e.generation, 0);
    ASSERT_EQ(e.entry_timeout, 1);
    ASSERT_EQ(e.attr_timeout, 2);
}

TEST_F(FileSystemTest, SetEntryTimeout) {
    auto builder = FileSystemBuilder();
    auto fs = builder.SetOption([](FileSystemOption* option){
        option->kernelCacheOption.entryTimeoutSec = 1;
        option->kernelCacheOption.attrTimeoutSec = 2;
        option->kernelCacheOption.dirEntryTimeoutSec = 3;
        option->kernelCacheOption.dirAttrTimeoutSec = 4;
    }).Build();

    // CASE 1: set kernel cache timeout for regular file or symbol link
    std::vector<FsFileType> types = {
        FsFileType::TYPE_S3,
        FsFileType::TYPE_FILE,
        FsFileType::TYPE_SYM_LINK,
    };
    for (const auto& type : types) {
        auto attr = MkAttr(100, AttrOption().type(type));
        auto entryOut = EntryOut(attr);
        fs->SetEntryTimeout(&entryOut);
        ASSERT_EQ(entryOut.entryTimeout, 1);
        ASSERT_EQ(entryOut.attrTimeout, 2);
    }

    // CASE 2: set kernel cache timeout for directory
    auto attr = MkAttr(100, AttrOption().type(FsFileType::TYPE_DIRECTORY));
    auto entryOut = EntryOut(attr);
    fs->SetEntryTimeout(&entryOut);
    ASSERT_EQ(entryOut.entryTimeout, 3);
    ASSERT_EQ(entryOut.attrTimeout, 4);
}

TEST_F(FileSystemTest, SetAttrTimeout) {
    auto builder = FileSystemBuilder();
    auto fs = builder.SetOption([](FileSystemOption* option){
        option->kernelCacheOption.attrTimeoutSec = 1;
        option->kernelCacheOption.dirAttrTimeoutSec = 2;
    }).Build();

    // CASE 1: set kernel attribute cache timeout
    //         for regular file or symbol link
    std::vector<FsFileType> types = {
        FsFileType::TYPE_S3,
        FsFileType::TYPE_FILE,
        FsFileType::TYPE_SYM_LINK,
    };
    for (const auto& typ : types) {
        auto attr = MkAttr(100, AttrOption().type(typ));
        auto attrOut = AttrOut(attr);
        fs->SetAttrTimeout(&attrOut);
        ASSERT_EQ(attrOut.attrTimeout, 1);
    }

    // CASE 2: set kernel attribute cache timeout for directory
    auto attr = MkAttr(100, AttrOption().type(FsFileType::TYPE_DIRECTORY));
    auto attrOut = AttrOut(attr);
    fs->SetAttrTimeout(&attrOut);
    ASSERT_EQ(attrOut.attrTimeout, 2);
}

TEST_F(FileSystemTest, Reply) {
    // TODO(Wine93): make it works
}

TEST_F(FileSystemTest, Lookup_Basic) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();

    EXPECT_CALL_RETURN_GetDentry(*builder.GetDentryManager(),
                                 CURVEFS_ERROR::OK);
    EXPECT_CALL_RETURN_GetInodeAttr(*builder.GetInodeManager(),
                                    CURVEFS_ERROR::OK);

    EntryOut entryOut;
    auto rc = fs->Lookup(Request(), 1, "f1", &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
}

TEST_F(FileSystemTest, Lookup_NameTooLong) {
    auto builder = FileSystemBuilder();
    auto fs = builder.SetOption([](FileSystemOption* option) {
        option->maxNameLength = 255;
    }).Build();

    EntryOut entryOut;
    auto rc = fs->Lookup(Request(), 1, std::string(256, 'x'), &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::NAMETOOLONG);
}

TEST_F(FileSystemTest, Lookup_NegativeCache) {
    auto builder = FileSystemBuilder();
    auto fs = builder.SetOption([](FileSystemOption* option) {
        option->lookupCacheOption.negativeTimeoutSec = 1;
        option->lookupCacheOption.lruSize = 100000;
    }).Build();

    EXPECT_CALL_RETURN_GetDentry(*builder.GetDentryManager(),
                                 CURVEFS_ERROR::NOTEXIST);

    EntryOut entryOut;
    auto rc = fs->Lookup(Request(), 1, "f1", &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::NOTEXIST);

    rc = fs->Lookup(Request(), 1, "f1", &entryOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::NOTEXIST);
}

TEST_F(FileSystemTest, GetAttr_Basic) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();

    EXPECT_CALL_INVOKE_GetInodeAttr(*builder.GetInodeManager(),
        [&](uint64_t ino, InodeAttr* attr) -> CURVEFS_ERROR {
            attr->set_inodeid(ino);
            attr->set_length(4096);
            attr->set_mtime(123);
            attr->set_mtime_ns(456);
            return CURVEFS_ERROR::OK;
        });

    AttrOut attrOut;
    auto rc = fs->GetAttr(Request(), 100, &attrOut);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    ASSERT_EQ(attrOut.attr.inodeid(), 100);
    ASSERT_EQ(attrOut.attr.length(), 4096);
    ASSERT_EQ(attrOut.attr.mtime(), 123);
    ASSERT_EQ(attrOut.attr.mtime_ns(), 456);
}

TEST_F(FileSystemTest, OpenDir_Basic) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();

    EXPECT_CALL_RETURN_GetInodeAttr(*builder.GetInodeManager(),
                                    CURVEFS_ERROR::OK);

    auto fi = FileInfo();
    auto rc = fs->OpenDir(Request(), 1, &fi);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
}

TEST_F(FileSystemTest, ReadDir_Basic) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();

    // mock what opendir() does:
    auto handler = fs->NewHandler();
    auto fi = FileInfo();
    fi.fh = handler->fh;

    // CASE 1: readdir success
    EXPECT_CALL_INVOKE_ListDentry(*builder.GetDentryManager(),
        [&](uint64_t parent,
            std::list<Dentry>* dentries,
            uint32_t limit,
            bool only,
            uint32_t nlink) -> CURVEFS_ERROR {
            dentries->push_back(MkDentry(1, "test"));
            return CURVEFS_ERROR::OK;
        });
    EXPECT_CALL_INVOKE_BatchGetInodeAttrAsync(*builder.GetInodeManager(),
        [&](uint64_t parentId,
            std::set<uint64_t>* inos,
            std::map<uint64_t, InodeAttr>* attrs) -> CURVEFS_ERROR {
            for (const auto& ino : *inos) {
                auto attr = MkAttr(ino, AttrOption().mtime(123, ino));
                attrs->emplace(ino, attr);
            }
            return CURVEFS_ERROR::OK;
        });

    DirEntry dirEntry;
    auto entries = std::make_shared<DirEntryList>();
    auto rc = fs->ReadDir(Request(), 1, &fi, &entries);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    ASSERT_EQ(entries->Size(), 1);
    ASSERT_TRUE(entries->Get(1, &dirEntry));
    ASSERT_EQ(dirEntry.ino, 1);
    ASSERT_EQ(dirEntry.name, "test");
}

TEST_F(FileSystemTest, ReadDir_CheckEntries) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();
    Ino ino(1);

    // mock what opendir() does:
    auto handler = fs->NewHandler();
    auto fi = FileInfo();
    fi.fh = handler->fh;

    auto CHECK_ENTRIES = [&](const std::shared_ptr<DirEntryList>& entries) {
        std::vector<DirEntry> out;
        entries->Iterate([&](DirEntry* dirEntry){
            out.push_back(*dirEntry);
        });
        ASSERT_EQ(out.size(), 3);

        int idx = 0;
        for (auto ino = 100; ino <= 102; ino++) {
            ASSERT_EQ(out[idx].ino, ino);
            ASSERT_EQ(out[idx].name, StrFormat("f%d", ino));
            ASSERT_EQ(out[idx].attr.mtime(), 123);
            ASSERT_EQ(out[idx].attr.mtime_ns(), ino);
            idx++;
        }
    };

    // CASE 1: check entries
    {
        EXPECT_CALL_INVOKE_ListDentry(*builder.GetDentryManager(),
            [&](uint64_t parent,
                std::list<Dentry>* dentries,
                uint32_t limit,
                bool only,
                uint32_t nlink) -> CURVEFS_ERROR {
                for (auto ino = 100; ino <= 102; ino++) {
                    dentries->push_back(MkDentry(ino, StrFormat("f%d", ino)));
                }
                return CURVEFS_ERROR::OK;
            });

        EXPECT_CALL_INVOKE_BatchGetInodeAttrAsync(*builder.GetInodeManager(),
            [&](uint64_t parentId,
                std::set<uint64_t>* inos,
                std::map<uint64_t, InodeAttr>* attrs) -> CURVEFS_ERROR {
                for (const auto& ino : *inos) {
                    auto attr = MkAttr(ino, AttrOption().mtime(123, ino));
                    attrs->emplace(ino, attr);
                }
                return CURVEFS_ERROR::OK;
            });

        auto entries = std::make_shared<DirEntryList>();
        auto rc = fs->ReadDir(Request(), ino, &fi, &entries);
        ASSERT_EQ(rc, CURVEFS_ERROR::OK);
        CHECK_ENTRIES(entries);
    }

    // CASE 2: check dir cache
    {
        // readdir from cache
        auto entries = std::make_shared<DirEntryList>();
        auto rc = fs->ReadDir(Request(), ino, &fi, &entries);
        ASSERT_EQ(rc, CURVEFS_ERROR::OK);
        CHECK_ENTRIES(entries);
    }
}

TEST_F(FileSystemTest, ReleaseDir_Basic) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();

    auto fi = FileInfo();
    auto rc = fs->ReleaseDir(Request(), 1, &fi);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
}

TEST_F(FileSystemTest, ReleaseDir_CheckHandler) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();

    // mock what opendir() does:
    auto handler = fs->NewHandler();
    auto fh = handler->fh;

    // CASE 1: find handler success
    ASSERT_TRUE(fs->FindHandler(fh) != nullptr);

    // CASE 2: releasedir will release handler
    auto fi = FileInfo();
    fi.fh = fh;
    auto rc = fs->ReleaseDir(Request(), 1, &fi);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    ASSERT_TRUE(fs->FindHandler(fh) == nullptr);
}

TEST_F(FileSystemTest, Open_Basic) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();

    // mock what lookup() does:
    Ino ino(100);
    auto attrWatcher = fs->BorrowMember().attrWatcher;
    attrWatcher->RemeberMtime(MkAttr(ino, AttrOption().mtime(123, 456)));

    // CASE 1: open success
    {
        EXPECT_CALL_INVOKE_GetInode(*builder.GetInodeManager(),
            [&](uint64_t ino,
                std::shared_ptr<InodeWrapper>& inode) -> CURVEFS_ERROR {
                inode = MkInode(ino, InodeOption().mtime(123, 456));
                return CURVEFS_ERROR::OK;
            });

        auto fi = FileInfo();
        auto rc = fs->Open(Request(), ino, &fi);
        ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    }

    // CASE 2: file already opened
    {
        auto fi = FileInfo();
        auto rc = fs->Open(Request(), ino, &fi);
        ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    }
}

TEST_F(FileSystemTest, Open_Stale) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();

    // mock lookup() does:
    Ino ino(100);
    auto attrWatcher = fs->BorrowMember().attrWatcher;
    attrWatcher->RemeberMtime(MkAttr(ino, AttrOption().mtime(123, 456)));

    // CASE 1: mtime(123, 456) != mtime(123, 789)
    EXPECT_CALL_INVOKE_GetInode(*builder.GetInodeManager(),
        [&](uint64_t ino, std::shared_ptr<InodeWrapper>& inode)
            -> CURVEFS_ERROR {
            inode = MkInode(ino, InodeOption().mtime(123, 789));
            return CURVEFS_ERROR::OK;
        });

    auto fi = FileInfo();
    auto rc = fs->Open(Request(), ino, &fi);
    ASSERT_EQ(rc, CURVEFS_ERROR::STALE);
}

TEST_F(FileSystemTest, Open_StaleForAttrNotFound) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();

    EXPECT_CALL_INVOKE_GetInode(*builder.GetInodeManager(),
       [&](uint64_t ino, std::shared_ptr<InodeWrapper>& inode)
           -> CURVEFS_ERROR {
           inode = MkInode(ino, InodeOption().mtime(123, 456));
           return CURVEFS_ERROR::OK;
       });

    Ino ino(100);
    auto fi = FileInfo();
    auto rc = fs->Open(Request(), ino, &fi);
    ASSERT_EQ(rc, CURVEFS_ERROR::STALE);
}

TEST_F(FileSystemTest, Release_Basic) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();
    auto rc = fs->Release(Request(), 100);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
}

TEST_F(FileSystemTest, Release_CheckOpenStatus) {
    auto builder = FileSystemBuilder();
    auto fs = builder.Build();

    // mock what open() does:
    Ino ino(100);
    auto inode = MkInode(100);
    auto openfiles = fs->BorrowMember().openFiles;
    openfiles->Open(ino, inode);

    // CASE 1: ino(100) is opened
    auto out = MkInode(0);
    bool yes = openfiles->IsOpened(ino, &out);
    ASSERT_TRUE(yes);
    ASSERT_EQ(inode->GetInodeId(), ino);

    // CASE 2: release will close open file
    auto rc = fs->Release(Request(), 100);
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    yes = openfiles->IsOpened(ino, &out);
    ASSERT_FALSE(yes);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
