/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 2:07:05 pm
 * Author:
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_LIBCURVE_INTERFACE_H  //NOLINT
#define CURVE_LIBCURVE_INTERFACE_H

#include <unistd.h>
#include <stdint.h>
#include <vector>
#include <map>

#include "src/client/libcurve_define.h"
#ifdef __cplusplus
extern "C" {
#endif

#define CURVE_INODE_DIRECTORY 0
#define CURVE_INODE_PAGEFILE 1
#define CURVEINODE_APPENDFILE 2
#define CURVE_INODE_APPENDECFILE 3

#define CURVE_ERROR_OK  0
#define CURVE_ERROR_EXISTS 1
#define CURVE_ERROR_FAILED 2
#define CURVE_ERROR_DISABLEIO 3
#define CURVE_ERROR_AUTHFAIL 4   // 认证失败
#define CURVE_ERROR_DELETING 5
#define CURVE_ERROR_NOTEXIST 6
#define CURVE_ERROR_UNDER_SNAPSHOT 7
#define CURVE_ERROR_NOT_UNDERSNAPSHOT 8
#define CURVE_ERROR_DELETE_ERROR 9
#define CURVE_ERROR_UNKNOWN 10


#define CURVE_OP_READ 0
#define CURVE_OP_WRITE 1


typedef void (*AioCallBack)(struct AioContext* context);

typedef struct AioContext {
    unsigned long offset;  //NOLINT
    unsigned long length;  //NOLINT
    int ret;
    int op;
    int err;
    AioCallBack cb;
    void* buf;
} AioContext_t;

typedef struct UserInfo {
    char owner[256];
    char password[256];
} UserInfo_t;

typedef struct FileInfo {
    uint64_t      id;
    uint64_t      parentid;
    int           filetype;
    uint64_t      length;
    uint64_t      ctime;
} FileInfo_t;


int Init(const char* path);
int Open4Qemu(const char* filename);
int Open(const char* filename, UserInfo_t* info);
int Create(const char* filename, UserInfo_t* info, size_t size);

// 同步读写
int Read(int fd, char* buf, unsigned long offset, unsigned long length);   //NOLINT
int Write(int fd, const char* buf, unsigned long offset, unsigned long length);  //NOLINT

// 异步读写
int AioRead(int fd, AioContext* aioctx);
int AioWrite(int fd, AioContext* aioctx);

// 获取文件的基本信息
int StatFile4Qemu(const char* filename, FileInfo_t* finfo);
int StatFile(const char* filename, UserInfo_t* info, FileInfo_t* finfo);

int Close(int fd);

int Rename(UserInfo_t* info, const char* oldpath, const char* newpath);
int Extend(const char* filename, UserInfo_t* info, uint64_t size);
int Unlink(const char* filename, UserInfo_t* info);
int Listdir(const char* dirpath, UserInfo_t* info, FileInfo_t** fileinfos);
int Mkdir(const char* dirpath, UserInfo_t* info);
int Rmdir(const char* dirpath, UserInfo_t* info);

void UnInit();

#ifdef __cplusplus
}
#endif

#endif  // !CURVE_LIBCURVE_INTERFACE_H  //NOLINT
