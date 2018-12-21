/*
 * Project: curve
 * Created Date: Monday December 24th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include "src/fs/local_filesystem.h"

namespace curve {
namespace fs {

class LocalFSFactoryTest : public testing::Test {
 public:
    LocalFSFactoryTest() {}
    ~LocalFSFactoryTest() {}
};

TEST_F(LocalFSFactoryTest, CreateTest) {
    std::shared_ptr<LocalFileSystem> lfs1 =
        LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
    ASSERT_NE(lfs1, nullptr);
    std::shared_ptr<LocalFileSystem> lfs2 =
        LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
    // singleton
    ASSERT_EQ(lfs1.get(), lfs2.get());
}

}  // namespace fs
}  // namespace curve
