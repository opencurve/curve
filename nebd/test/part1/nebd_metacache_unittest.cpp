/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#include "nebd/src/part1/nebd_metacache.h"

#include <gtest/gtest.h>
#include <gflags/gflags.h>

namespace nebd {
namespace client {

TEST(MetaCacheTest, AddTest) {
    NebdClientMetaCache metaCache;
    ASSERT_NO_THROW(metaCache.AddFileInfo({1, "/file1",
                                           FileLock("/file1.lock")}));
    ASSERT_NO_THROW(metaCache.AddFileInfo({2, "/file2",
                                           FileLock("/file2.lock")}));
    ASSERT_NO_THROW(metaCache.AddFileInfo({1, "/flie3",
                                           FileLock("/file4.lock")}));
}

TEST(MetaCacheTest, RemoveTest) {
    NebdClientMetaCache metaCache;
    ASSERT_NO_THROW(metaCache.AddFileInfo({1, "/file1",
                                           FileLock("/file1.lock")}));
    ASSERT_NO_THROW(metaCache.RemoveFileInfo(1));
    ASSERT_NO_THROW(metaCache.RemoveFileInfo(2));
}

TEST(MetaCacheTest, GetTest) {
    NebdClientMetaCache metaCache;
    ASSERT_NO_THROW(metaCache.AddFileInfo({1, "/file1",
                                           FileLock("/file1.lock")}));

    NebdClientFileInfo fileInfo;
    ASSERT_EQ(0, metaCache.GetFileInfo(1, &fileInfo));
    ASSERT_EQ(1, fileInfo.fd);
    ASSERT_STREQ("/file1", fileInfo.fileName.c_str());

    ASSERT_EQ(-1, metaCache.GetFileInfo(2, &fileInfo));
}

TEST(MetaCacheTest, GetAllTest) {
    NebdClientMetaCache metaCache;

    ASSERT_EQ(0, metaCache.GetAllFileInfo().size());

    ASSERT_NO_THROW(metaCache.AddFileInfo({1, "/file1",
                                           FileLock("/file1.lock")}));
    ASSERT_EQ(1, metaCache.GetAllFileInfo().size());

    ASSERT_NO_THROW(metaCache.AddFileInfo({1, "/file1",
                                           FileLock("/file1.lock")}));
    ASSERT_EQ(1, metaCache.GetAllFileInfo().size());

    ASSERT_NO_THROW(metaCache.AddFileInfo({2, "/file2",
                                           FileLock("/file2.lock")}));
    ASSERT_EQ(2, metaCache.GetAllFileInfo().size());

    NebdClientFileInfo fileInfo;
    ASSERT_NO_THROW(metaCache.RemoveFileInfo(2));
    ASSERT_EQ(1, metaCache.GetAllFileInfo().size());

    ASSERT_NO_THROW(metaCache.RemoveFileInfo(2));
    ASSERT_EQ(1, metaCache.GetAllFileInfo().size());

    ASSERT_NO_THROW(metaCache.RemoveFileInfo(1));
    ASSERT_EQ(0, metaCache.GetAllFileInfo().size());
}

}  // namespace client
}  // namespace nebd

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
