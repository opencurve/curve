/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * Created Date: 2022-10-18
 * Author: xuchaojie
 */

// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved

// Author: ZhengPengFei (zhengpengfei@baidu.com)
// Date: 2017/06/16 10:29:05


#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "src/chunkserver/filesystem_adaptor/curve_filesystem_adaptor.h"

namespace curve {
namespace chunkserver {

using ::curve::fs::LocalFileSystem;
using ::curve::fs::LocalFileSystemOption;
using ::curve::fs::LocalFsFactory;
using ::curve::fs::FileSystemType;

using ::testing::Return;
using ::testing::Invoke;
using ::testing::_;

const char FILEPOOL_DIR[] = "./TestFileSystemAdaptorSuits/filePool/";

class TestFileSystemAdaptorSuits : public testing::Test {
 protected:
    void SetUp() {
        lfs_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        ASSERT_EQ(0, lfs_->Init(lfsOption_));

        if (lfs_->DirExists(FILEPOOL_DIR)) {
            ASSERT_EQ(0, lfs_->Delete(FILEPOOL_DIR));
        }

        uint32_t chunksize = 4096;
        uint32_t metapagesize = 4096;

        std::string filePoolMetaPath =
            "./TestFileSystemAdaptorSuits/filePool.meta";
        ASSERT_EQ(0, lfs_->Mkdir(FILEPOOL_DIR));
        int ret = FilePoolHelper::PersistEnCodeMetaInfo(
            lfs_, chunksize, metapagesize, FILEPOOL_DIR,
            filePoolMetaPath.c_str());
        if (ret == -1) {
            LOG(ERROR) << "persist chunkfile pool meta info failed!";
            return;
        }

        filePool_ = std::make_shared<FilePool>(lfs_);
        filePoolOptions_.getFileFromPool = false;
        filePoolOptions_.fileSize = 16 * 1024;
        filePoolOptions_.metaPageSize = 4 * 1024;
        filePoolOptions_.metaFileSize = 4 * 1024;
        filePoolOptions_.retryTimes = 5;

        const std::string filePoolPath = FILEPOOL_DIR;
        memcpy(filePoolOptions_.filePoolDir,
            filePoolPath.c_str(), filePoolPath.size());
        memcpy(filePoolOptions_.metaPath,
            filePoolMetaPath.c_str(), filePoolMetaPath.size());
        filePool_->Initialize(filePoolOptions_);
    }
    void TearDown() {}

 protected:
    LocalFileSystemOption lfsOption_;
    std::shared_ptr<LocalFileSystem> lfs_;

    FilePoolOptions filePoolOptions_;
    std::shared_ptr<FilePool> filePool_;
};

TEST_F(TestFileSystemAdaptorSuits, read_write) {
    lfs_->Delete("test_file");
    lfs_->Delete("test_file1");

    scoped_refptr<CurveFilesystemAdaptor> fs =
        new CurveFilesystemAdaptor(filePool_, lfs_);
    // not from chunkfilepool
    fs->AddToFilterList("test_file");
    fs->AddToFilterList("test_file1");

    butil::File::Error e;
    braft::FileAdaptor* file = fs->open(
        "test_file", O_CREAT | O_TRUNC | O_RDWR, NULL, &e);
    ASSERT_TRUE(file != NULL);
    ASSERT_EQ(file->size(), 0);

    butil::IOBuf data;
    data.append("ccccc");
    ASSERT_EQ(data.size(), file->write(data, 0));
    ASSERT_EQ(data.size(), file->write(data, data.size() * 2));
    ASSERT_EQ(file->size(), data.size() * 3);

    butil::IOPortal portal;
    ASSERT_EQ(data.size(), file->read(&portal, 0, data.size()));
    ASSERT_EQ(portal.to_string(), data.to_string());
    ASSERT_EQ(2, file->read(&portal, data.size() * 3 - 2, 10));
    ASSERT_EQ(0, file->read(&portal, data.size() * 3 + 1, 10));
    delete file;

    file = fs->open("test_file", O_RDWR, NULL, &e);
    portal.clear();
    ASSERT_EQ(data.size(), file->read(&portal, 0, data.size()));
    ASSERT_EQ(portal.to_string(), data.to_string());
    ASSERT_EQ(2, file->read(&portal, data.size() * 3 - 2, 10));
    ASSERT_EQ(0, file->read(&portal, data.size() * 3 + 1, 10));
    delete file;

    file = fs->open("test_file1", O_RDWR, NULL, &e);
    ASSERT_TRUE(file == NULL);
    ASSERT_EQ(butil::File::FILE_ERROR_NOT_FOUND, e);

    lfs_->Delete("test_file");
    lfs_->Delete("test_file1");
}

TEST_F(TestFileSystemAdaptorSuits, read_write_from_filepool) {
    lfs_->Delete("test_chunkfile");
    lfs_->Delete("test_chunkfile1");

    scoped_refptr<CurveFilesystemAdaptor> fs =
        new CurveFilesystemAdaptor(filePool_, lfs_);

    butil::File::Error e;
    braft::FileAdaptor* file = fs->open(
        "test_chunkfile", O_CREAT | O_TRUNC | O_RDWR, NULL, &e);
    ASSERT_TRUE(file != NULL);
    ASSERT_EQ(file->size(),
        filePoolOptions_.fileSize + filePoolOptions_.metaPageSize);

    butil::IOBuf data;
    data.append("ccccc");
    ASSERT_EQ(data.size(), file->write(data, 0));
    ASSERT_EQ(data.size(), file->write(data, data.size() * 2));
    ASSERT_EQ(file->size(),
        filePoolOptions_.fileSize + filePoolOptions_.metaPageSize);

    butil::IOPortal portal;
    ASSERT_EQ(data.size(), file->read(&portal, 0, data.size()));
    ASSERT_EQ(portal.to_string(), data.to_string());
    ASSERT_EQ(10, file->read(&portal, data.size() * 3 - 2, 10));
    ASSERT_EQ(10, file->read(&portal, data.size() * 3 + 1, 10));
    delete file;

    file = fs->open("test_chunkfile", O_RDWR, NULL, &e);
    portal.clear();
    ASSERT_EQ(data.size(), file->read(&portal, 0, data.size()));
    ASSERT_EQ(portal.to_string(), data.to_string());
    ASSERT_EQ(10, file->read(&portal, data.size() * 3 - 2, 10));
    ASSERT_EQ(10, file->read(&portal, data.size() * 3 + 1, 10));
    delete file;

    file = fs->open("test_chunkfile1", O_RDWR, NULL, &e);
    ASSERT_TRUE(file == NULL);
    ASSERT_EQ(butil::File::FILE_ERROR_NOT_FOUND, e);

    lfs_->Delete("test_chunkfile");
    lfs_->Delete("test_chunkfile1");
}

TEST_F(TestFileSystemAdaptorSuits, delete_file) {
    lfs_->Delete("test_file");
    lfs_->Open("test_file", O_CREAT | O_TRUNC | O_RDWR);

    scoped_refptr<CurveFilesystemAdaptor> fs =
        new CurveFilesystemAdaptor(filePool_, lfs_);
    // not from chunkfilepool
    fs->AddToFilterList("test_file");

    ASSERT_TRUE(fs->path_exists("test_file"));
    ASSERT_TRUE(!fs->directory_exists("test_file"));
    ASSERT_TRUE(fs->delete_file("test_file", false));
    ASSERT_TRUE(!fs->path_exists("test_file"));
    ASSERT_TRUE(!fs->directory_exists("test_file"));
    ASSERT_TRUE(fs->delete_file("test_file", false));
    ASSERT_TRUE(!fs->path_exists("test_file"));
    ASSERT_TRUE(!fs->directory_exists("test_file"));

    lfs_->Delete("test_dir/");
    lfs_->Mkdir("test_dir/test_dir/");
    lfs_->Open("test_dir/test_dir/test_file", O_CREAT | O_TRUNC | O_RDWR);

    ASSERT_TRUE(fs->path_exists("test_dir"));
    ASSERT_TRUE(fs->directory_exists("test_dir"));
    ASSERT_TRUE(fs->path_exists("test_dir/test_dir/"));
    ASSERT_TRUE(fs->directory_exists("test_dir/test_dir/"));
    ASSERT_TRUE(fs->path_exists("test_dir/test_dir/test_file"));
    ASSERT_TRUE(!fs->directory_exists("test_dir/test_dir/test_file"));

    ASSERT_TRUE(!fs->delete_file("test_dir", false));
    ASSERT_TRUE(!fs->delete_file("test_dir/test_dir", false));
    ASSERT_TRUE(fs->delete_file("test_dir/test_dir", true));
    ASSERT_TRUE(fs->delete_file("test_dir", false));
}

TEST_F(TestFileSystemAdaptorSuits, delete_file_from_filepool) {
    lfs_->Delete("test_chunkfile");

    scoped_refptr<CurveFilesystemAdaptor> fs =
        new CurveFilesystemAdaptor(filePool_, lfs_);

    butil::File::Error e;
    braft::FileAdaptor* file = fs->open(
        "test_chunkfile", O_CREAT | O_TRUNC | O_RDWR, NULL, &e);
    ASSERT_TRUE(file != NULL);
    ASSERT_EQ(file->size(),
        filePoolOptions_.fileSize + filePoolOptions_.metaPageSize);

    ASSERT_TRUE(fs->path_exists("test_chunkfile"));
    ASSERT_TRUE(!fs->directory_exists("test_chunkfile"));
    ASSERT_TRUE(fs->delete_file("test_chunkfile", false));
    ASSERT_TRUE(!fs->path_exists("test_chunkfile"));
    ASSERT_TRUE(!fs->directory_exists("test_chunkfile"));
    ASSERT_TRUE(fs->delete_file("test_chunkfile", false));
    ASSERT_TRUE(!fs->path_exists("test_chunkfile"));
    ASSERT_TRUE(!fs->directory_exists("test_chunkfile"));

    lfs_->Delete("test_dir/");
    lfs_->Mkdir("test_dir/test_dir/");

    braft::FileAdaptor* file2 = fs->open("test_dir/test_dir/test_chunkfile",
        O_CREAT | O_TRUNC | O_RDWR, NULL, &e);
    ASSERT_TRUE(file2 != NULL);
    ASSERT_EQ(file2->size(),
        filePoolOptions_.fileSize + filePoolOptions_.metaPageSize);

    ASSERT_TRUE(fs->path_exists("test_dir"));
    ASSERT_TRUE(fs->directory_exists("test_dir"));
    ASSERT_TRUE(fs->path_exists("test_dir/test_dir/"));
    ASSERT_TRUE(fs->directory_exists("test_dir/test_dir/"));
    ASSERT_TRUE(fs->path_exists("test_dir/test_dir/test_chunkfile"));
    ASSERT_TRUE(!fs->directory_exists("test_dir/test_dir/test_chunkfile"));

    ASSERT_TRUE(!fs->delete_file("test_dir", false));
    ASSERT_TRUE(!fs->delete_file("test_dir/test_dir", false));
    ASSERT_TRUE(fs->delete_file("test_dir/test_dir", true));
    ASSERT_TRUE(fs->delete_file("test_dir", false));
}

TEST_F(TestFileSystemAdaptorSuits, rename) {
    lfs_->Delete("test_file");
    lfs_->Open("test_file", O_CREAT | O_TRUNC | O_RDWR);

    scoped_refptr<CurveFilesystemAdaptor> fs =
        new CurveFilesystemAdaptor(filePool_, lfs_);
    // not from chunkfilepool
    fs->AddToFilterList("test_file");

    ASSERT_TRUE(fs->rename("test_file", "test_file2"));
    ASSERT_TRUE(fs->rename("test_file2", "test_file2"));

    lfs_->Open("test_file", O_CREAT | O_TRUNC | O_RDWR);
    ASSERT_TRUE(fs->rename("test_file2", "test_file"));
    ASSERT_TRUE(fs->path_exists("test_file"));
    ASSERT_TRUE(!fs->path_exists("test_file2"));

    lfs_->Delete("test_dir");
    lfs_->Mkdir("test_dir");
    ASSERT_TRUE(!fs->rename("test_file", "test_dir"));
    ASSERT_TRUE(fs->rename("test_file", "test_dir/test_file"));

    lfs_->Delete("test_dir1");
    lfs_->Mkdir("test_dir1");
    lfs_->Open("test_dir1/test_file", O_CREAT | O_TRUNC | O_RDWR);
    ASSERT_TRUE(!fs->rename("test_dir", "test_dir1"));

    lfs_->Delete("test_dir1/test_file");
    ASSERT_TRUE(fs->rename("test_dir", "test_dir1"));
    ASSERT_TRUE(!fs->directory_exists("test_dir"));
    ASSERT_TRUE(fs->directory_exists("test_dir1"));
    ASSERT_TRUE(fs->path_exists("test_dir1/test_file"));

    lfs_->Delete("test_dir1");
}

TEST_F(TestFileSystemAdaptorSuits, create_directory) {
    lfs_->Delete("test_dir/");

    scoped_refptr<CurveFilesystemAdaptor> fs =
        new CurveFilesystemAdaptor(filePool_, lfs_);

    butil::File::Error error;
    ASSERT_TRUE(fs->create_directory("test_dir", &error, false));
    ASSERT_TRUE(fs->create_directory("test_dir", &error, false));
    ASSERT_TRUE(!fs->create_directory(
        "test_dir/test_dir/test_dir", &error, false));
    ASSERT_EQ(error, butil::File::FILE_ERROR_NOT_FOUND);
    ASSERT_TRUE(fs->create_directory(
        "test_dir/test_dir/test_dir", &error, true));
    ASSERT_TRUE(fs->create_directory("test_dir/test_dir", &error, true));

    lfs_->Open("test_dir/test_file", O_CREAT | O_TRUNC | O_RDWR);
    ASSERT_TRUE(!fs->create_directory("test_dir/test_file", &error, true));
    ASSERT_EQ(error, butil::File::FILE_ERROR_EXISTS);

    ASSERT_TRUE(!braft::create_sub_directory(
        "test_dir/test_dir2", "test_dir2/test2", fs, &error));
    ASSERT_EQ(error, butil::File::FILE_ERROR_NOT_FOUND);

    ASSERT_TRUE(braft::create_sub_directory(
        "test_dir", "test_dir2/test2", fs, &error));
    ASSERT_TRUE(fs->directory_exists("test_dir/test_dir2/test2"));

    lfs_->Delete("test_dir/");
}

TEST_F(TestFileSystemAdaptorSuits, directory_reader) {
    lfs_->Delete("test_dir/");
    lfs_->Mkdir("test_dir/test_dir");
    lfs_->Open("test_dir/test_file", O_CREAT | O_TRUNC | O_RDWR);

    scoped_refptr<CurveFilesystemAdaptor> fs =
        new CurveFilesystemAdaptor(filePool_, lfs_);
    // not from chunkfilepool
    fs->AddToFilterList("test_file");

    braft::DirReader* dir_reader = fs->directory_reader("test_dir");
    std::set<std::string> names;
    names.insert("test_dir");
    names.insert("test_file");
    ASSERT_TRUE(dir_reader->is_valid());
    while (dir_reader->next())  {
        std::string n = dir_reader->name();
        ASSERT_EQ(1, names.count(n));
        names.erase(dir_reader->name());
    }
    ASSERT_TRUE(names.empty());
    delete dir_reader;

    lfs_->Delete("test_dir/");
    dir_reader = fs->directory_reader("test_dir");
    ASSERT_TRUE(!dir_reader->is_valid());
    delete dir_reader;
}

TEST_F(TestFileSystemAdaptorSuits, create_sub_directory) {
    lfs_->Delete("test_dir/");
    lfs_->Mkdir("test_dir");

    scoped_refptr<CurveFilesystemAdaptor> fs =
        new CurveFilesystemAdaptor(filePool_, lfs_);

    std::string parent_path = "test_dir/sub1/";
    ASSERT_FALSE(braft::create_sub_directory(parent_path, "/", fs, NULL));
    ASSERT_FALSE(braft::create_sub_directory(parent_path, "", fs, NULL));
    ASSERT_FALSE(braft::create_sub_directory(parent_path, "/sub2", fs, NULL));
    ASSERT_FALSE(braft::create_sub_directory(
        parent_path, "/sub2/sub3", fs, NULL));
    ASSERT_FALSE(braft::create_sub_directory(
        parent_path, "sub4/sub5", fs, NULL));
    ASSERT_FALSE(fs->directory_exists(parent_path + "sub2/sub3"));
    ASSERT_FALSE(fs->directory_exists(parent_path + "sub4/sub5"));
    ASSERT_FALSE(fs->directory_exists(parent_path));
    ASSERT_TRUE(fs->create_directory(parent_path, NULL, false));
    ASSERT_TRUE(braft::create_sub_directory(
        parent_path, "/sub2/sub3", fs, NULL));
    ASSERT_TRUE(braft::create_sub_directory(
        parent_path, "sub4/sub5", fs, NULL));
    ASSERT_TRUE(fs->directory_exists(parent_path + "sub2/sub3"));
    ASSERT_TRUE(fs->directory_exists(parent_path + "sub4/sub5"));
    ASSERT_FALSE(braft::create_sub_directory(
        parent_path, "../sub4/sub5", fs, NULL));
    lfs_->Delete("test_dir/");
}

}  // namespace chunkserver
}  // namespace curve
