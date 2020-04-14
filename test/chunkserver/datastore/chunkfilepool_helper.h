/*
 * Project: curve
 * File Created: Tuesday, 29th January 2019 11:44:59 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#ifndef TEST_CHUNKSERVER_DATASTORE_CHUNKFILEPOOL_HELPER_H_
#define TEST_CHUNKSERVER_DATASTORE_CHUNKFILEPOOL_HELPER_H_

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <fcntl.h>
#include <climits>
#include <memory>
#include <string>

#include "src/fs/local_filesystem.h"

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

void allocateChunk(std::shared_ptr<LocalFileSystem> fsptr,
                   uint32_t num,
                   std::string poolDir,
                   uint32_t chunkSize);

#endif  // TEST_CHUNKSERVER_DATASTORE_CHUNKFILEPOOL_HELPER_H_
