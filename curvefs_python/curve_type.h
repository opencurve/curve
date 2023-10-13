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

/**
 * Project: curve
 * Date: Fri May 15 15:46:59 CST 2020
 * Author: wuhanqing
 */

#ifndef CURVEFS_PYTHON_CURVE_TYPE_H_
#define CURVEFS_PYTHON_CURVE_TYPE_H_

#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include <string>

#define CURVE_INODE_DIRECTORY 0
#define CURVE_INODE_PAGEFILE 1
#define CURVEINODE_APPENDFILE 2
#define CURVE_INODE_APPENDECFILE 3

#define CURVE_ERROR_OK 0
// The file or directory already exists
#define CURVE_ERROR_EXISTS 1
// Operation failed
#define CURVE_ERROR_FAILED 2
// Prohibit IO
#define CURVE_ERROR_DISABLEIO 3
// Authentication failed
#define CURVE_ERROR_AUTHFAIL 4
// Removing
#define CURVE_ERROR_DELETING 5
// File does not exist
#define CURVE_ERROR_NOTEXIST 6
// In the snapshot
#define CURVE_ERROR_UNDER_SNAPSHOT 7
// During non snapshot periods
#define CURVE_ERROR_NOT_UNDERSNAPSHOT 8
// Delete Error
#define CURVE_ERROR_DELETE_ERROR 9
// Segment not allocated
#define CURVE_ERROR_NOT_ALLOCATE 10
// Operation not supported
#define CURVE_ERROR_NOT_SUPPORT 11
// Directory is not empty
#define CURVE_ERROR_NOT_EMPTY 12
// Prohibit shrinkage
#define CURVE_ERROR_NO_SHRINK_BIGGER_FILE 13
// Session does not exist
#define CURVE_ERROR_SESSION_NOTEXISTS 14
// File occupied
#define CURVE_ERROR_FILE_OCCUPIED 15
// Parameter error
#define CURVE_ERROR_PARAM_ERROR 16
// MDS side storage error
#define CURVE_ERROR_INTERNAL_ERROR 17
// CRC check error
#define CURVE_ERROR_CRC_ERROR 18
// There is an issue with the request parameter
#define CURVE_ERROR_INVALID_REQUEST 19
// There is a problem with the disk
#define CURVE_ERROR_DISK_FAIL 20
// Insufficient space
#define CURVE_ERROR_NO_SPACE 21
// IO misalignment
#define CURVE_ERROR_NOT_ALIGNED 22
// File closed, fd not available
#define CURVE_ERROR_BAD_FD 23
// File length not supported
#define CURVE_ERROR_LENGTH_NOT_SUPPORT 24

// File Status
#define CURVE_FILE_CREATED 0
#define CURVE_FILE_DELETING 1
#define CURVE_FILE_CLONING 2
#define CURVE_FILE_CLONEMETAINSTALLED 3
#define CURVE_FILE_CLONED 4
#define CURVE_FILE_BEINGCLONED 5

// Unknown error
#define CURVE_ERROR_UNKNOWN 100

#define CURVE_OP_READ 0
#define CURVE_OP_WRITE 1

#define CLUSTERIDMAX 256

typedef void (*AioCallBack)(struct AioContext* context);
typedef struct AioContext {
    unsigned long offset;  // NOLINT
    unsigned long length;  // NOLINT
    int ret;
    int op;
    AioCallBack cb;
    void* buf;
} AioContext_t;

typedef struct UserInfo {
    char owner[256];
    char password[256];
} UserInfo_t;

typedef struct FileInfo {
    uint64_t id;
    uint64_t parentid;
    int filetype;
    uint64_t length;
    uint64_t ctime;
    char filename[256];
    char owner[256];
    int fileStatus;
    uint64_t stripeUnit;
    uint64_t stripeCount;
} FileInfo_t;

typedef struct DirInfos {
    char* dirpath;
    UserInfo_t* userinfo;
    uint64_t dirsize;
    FileInfo_t* fileinfo;
} DirInfos_t;

struct CreateContext {
    std::string name;
    size_t length;
    UserInfo user;
    std::string poolset;
    uint64_t stripeUnit;
    uint64_t stripeCount;
};

#endif  // CURVEFS_PYTHON_CURVE_TYPE_H_
