/*
 * Project: curve
 * File Created: Monday, 18th February 2019 11:02:39 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_LIBCURVE_DEFINE_H
#define CURVE_LIBCURVE_DEFINE_H
#include <stdint.h>

enum FileType {
    INODE_DIRECTORY = 0,
    INODE_PAGEFILE = 1,
    INODE_APPENDFILE = 2,
    INODE_APPENDECFILE = 3,
};

enum CreateFileErrorType {
    FILE_CREATE_OK,
    FILE_ALREADY_EXISTS,
    FILE_CREATE_FAILED,
    FILE_CREATE_UNKNOWN
};

enum OpenFileErrorType {
    FILE_OPEN_OK,
    FILE_OPEN_FAILED,
    FILE_OPEN_UNKNOWN
};

typedef enum LIBCURVE_ERROR {
    LIBCURVE_ERROR_NOERROR,
    LIBCURVE_ERROR_DISABLEIO,
    LIBCURVE_ERROR_UNKNOWN,
    LIBCURVE_ERROR_MAX,
} LIBCURVE_ERROR;

typedef enum LIBCURVE_OP {
    LIBCURVE_OP_READ,
    LIBCURVE_OP_WRITE,
    LIBCURVE_OP_MAX,
} LIBCURVE_OP;

typedef void (*LibCurveAioCallBack)(struct CurveAioContext* context);

typedef struct CurveAioContext {
    off_t offset;
    size_t length;
    int ret;
    LIBCURVE_OP op;
    LIBCURVE_ERROR err;
    LibCurveAioCallBack cb;
    void* buf;
} CurveAioContext;

typedef struct FInfo {
    uint64_t        id;
    char            filename[4096];
    uint64_t        parentid;
    FileType        filetype;
    uint32_t        chunksize;
    uint32_t        segmentsize;
    uint64_t        length;
    uint64_t        ctime;
    uint64_t        seqnum;
} FInfo_t;

#endif  // !CURVE_LIBCURVE_DEFINE_H
