/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 2:07:05 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_LIBCURVE_INTERFACE_H
#define CURVE_LIBCURVE_INTERFACE_H

#include <unistd.h>
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

typedef struct FInfo {
    uint64_t        id;
    char           filename[256];
    uint64_t        parentid;
    FileType        filetype;
//  uint32      owner = 5;
    uint32_t        chunksize;
    uint32_t        segmentsize;
    uint64_t        length;
    uint64_t        ctime;
    uint64_t        snapshotid;
} FInfo_t;

typedef enum LIBCURVE_ERROR {
    LIBCURVE_ERROR_NOERROR,
    LIBCURVE_ERROR_UNKNOWN,
    LIBCURVE_ERROR_MAX,
} LIBCURVE_ERROR;

typedef enum LIBCURVE_OP {
    LIBCURVE_OP_READ,
    LIBCURVE_OP_WRITE,
    LIBCURVE_OP_MAX,
} LIBCURVE_OP;

struct CurveAioContext;

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


#ifdef __cplusplus
extern "C" {
#endif

// forward define
struct CurveAioContext;
struct FInfo;

int Init(const char* conf_path);
void UnInit();
CreateFileErrorType CreateFile(const char* name, size_t size);
/**
 * block device invoke open to create new device
 * block device layer will pass the vdisk name to client
 * Open will return a fd, for block device read or write
 */
int Open(const char* filename);
int Read(int fd, char* buf, off_t offset, size_t length);
int Write(int fd, const char* buf, off_t offset, size_t length);
int AioRead(int fd, CurveAioContext* aioctx);
int AioWrite(int fd, CurveAioContext* aioctx);
FInfo GetInfo(const char* filename);
void Close(int fd);
void Unlink(const char* filename);


#ifdef __cplusplus
}
#endif

#endif  // !CURVE_LIBCURVE_INTERFACE_H
