/*
 * Project: curve
 * File Created: Tuesday, 29th January 2019 11:44:59 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#ifndef CURVE_CHUNKSERVER_CHUNKFILEPOOL_HELPER
#define CURVE_CHUNKSERVER_CHUNKFILEPOOL_HELPER

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <fcntl.h>
#include <climits>
#include <memory>

#include "src/fs/local_filesystem.h"

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

void allocateChunk(std::shared_ptr<LocalFileSystem> fsptr, uint32_t num);

#endif  // !CURVE_CHUNKSERVER_CHUNKFILEPOOL_HELPER
