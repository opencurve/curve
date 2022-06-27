/*
 *  Copyright (c) 2022 NetEase Inc.
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
 *
 * History:
 *          2022/05/05  xuchaojie  Initial version
 */

#ifndef INCLUDE_CLIENT_LIBCURVE_DEFINE_H_
#define INCLUDE_CLIENT_LIBCURVE_DEFINE_H_

#include <unistd.h>
#include <stdint.h>

enum LIBCURVE_ERROR {
    // success
    OK                      = 0,
    // volume or dir exists
    EXISTS                  = 1,
    // failed
    FAILED                  = 2,
    // disable IO
    DISABLEIO               = 3,
    // authentication failed
    AUTHFAIL                = 4,
    // volume is deleting state
    DELETING                = 5,
    // volume not exist
    NOTEXIST                = 6,
    // under snapshot
    UNDER_SNAPSHOT          = 7,
    // not under snapshot
    NOT_UNDERSNAPSHOT       = 8,
    // delete error
    DELETE_ERROR            = 9,
    // segment not allocated
    NOT_ALLOCATE            = 10,
    // operation not supported
    NOT_SUPPORT             = 11,
    // directory not empty
    NOT_EMPTY               = 12,
    // no shrinkage
    NO_SHRINK_BIGGER_FILE   = 13,
    // session not exist
    SESSION_NOTEXISTS       = 14,
    // volume occupied
    FILE_OCCUPIED           = 15,
    // parameter error
    PARAM_ERROR             = 16,
    // internal error
    INTERNAL_ERROR           = 17,
    // CRC error
    CRC_ERROR               = 18,
    // parameter invalid
    INVALID_REQUEST         = 19,
    // disk fail
    DISK_FAIL               = 20,
    // not enough space
    NO_SPACE                = 21,
    // io not aligned
    NOT_ALIGNED             = 22,
    // volume file is being closed, fd unavailable
    BAD_FD                  = 23,
    // volume file length is not supported
    LENGTH_NOT_SUPPORT      = 24,
    // session not exist
    SESSION_NOT_EXIST       = 25,
    // status error
    STATUS_NOT_MATCH        = 26,
    // delete the file being cloned
    DELETE_BEING_CLONED     = 27,
    // this version of client not support snapshot
    CLIENT_NOT_SUPPORT_SNAPSHOT = 28,
    // snapshot is forbid now
    SNAPSTHO_FROZEN = 29,
    // You must retry it until success
    RETRY_UNTIL_SUCCESS = 30,
    // EPOCH_TOO_OLD
    EPOCH_TOO_OLD = 31,

    // unknown error
    UNKNOWN                 = 100
};

typedef enum LIBCURVE_OP {
    LIBCURVE_OP_READ,
    LIBCURVE_OP_WRITE,
    LIBCURVE_OP_DISCARD,
    LIBCURVE_OP_MAX,
} LIBCURVE_OP;


typedef void (*LibCurveAioCallBack)(struct CurveAioContext* context);

typedef struct CurveAioContext {
    off_t               offset;
    size_t              length;
    int                 ret;
    LIBCURVE_OP         op;
    LibCurveAioCallBack cb;
    void*               buf;
} CurveAioContext;

#endif  // INCLUDE_CLIENT_LIBCURVE_DEFINE_H_
