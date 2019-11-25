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
// 文件或者目录已存在
#define CURVE_ERROR_EXISTS 1
// 操作失败
#define CURVE_ERROR_FAILED 2
// 禁止IO
#define CURVE_ERROR_DISABLEIO 3
// 认证失败
#define CURVE_ERROR_AUTHFAIL 4
// 正在删除
#define CURVE_ERROR_DELETING 5
// 文件不存在
#define CURVE_ERROR_NOTEXIST 6
// 快照中
#define CURVE_ERROR_UNDER_SNAPSHOT 7
// 非快照期间
#define CURVE_ERROR_NOT_UNDERSNAPSHOT 8
// 删除错误
#define CURVE_ERROR_DELETE_ERROR 9
// segment未分配
#define CURVE_ERROR_NOT_ALLOCATE 10
// 操作不支持
#define CURVE_ERROR_NOT_SUPPORT 11
// 目录非空
#define CURVE_ERROR_NOT_EMPTY 12
// 禁止缩容
#define CURVE_ERROR_NO_SHRINK_BIGGER_FILE 13
// session不存在
#define CURVE_ERROR_SESSION_NOTEXISTS 14
// 文件被占用
#define CURVE_ERROR_FILE_OCCUPIED 15
// 参数错误
#define CURVE_ERROR_PARAM_ERROR 16
// MDS一侧存储错误
#define CURVE_ERROR_INTERNAL_ERROR 17
// crc检查错误
#define CURVE_ERROR_CRC_ERROR 18
// request参数存在问题
#define CURVE_ERROR_INVALID_REQUEST 19
// 磁盘存在问题
#define CURVE_ERROR_DISK_FAIL 20
// 空间不足
#define CURVE_ERROR_NO_SPACE 21
// IO未对齐
#define CURVE_ERROR_NOT_ALIGNED 22
// 文件被关闭，fd不可用
#define CURVE_ERROR_BAD_FD 23
// 文件长度不支持
#define CURVE_ERROR_LENGTH_NOT_SUPPORT 24
// 未知错误
#define CURVE_ERROR_UNKNOWN 100

#define CURVE_OP_READ 0
#define CURVE_OP_WRITE 1

#define CLUSTERIDMAX 256


typedef void (*AioCallBack)(struct AioContext* context);
typedef struct AioContext {
    unsigned long offset;  //NOLINT
    unsigned long length;  //NOLINT
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
    uint64_t      id;
    uint64_t      parentid;
    int           filetype;
    uint64_t      length;
    uint64_t      ctime;
    char          filename[256];
    char          owner[256];
} FileInfo_t;

typedef struct DirInfos {
    char*         dirpath;
    UserInfo_t*   userinfo;
    uint64_t      dirsize;
    FileInfo_t*   fileinfo;
} DirInfos_t;

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
int ChangeOwner(const char* filename, const char* owner, UserInfo_t* info);
int Close(int fd);

int Rename(UserInfo_t* info, const char* oldpath, const char* newpath);
int Extend(const char* filename, UserInfo_t* info, uint64_t size);
int Unlink(const char* filename, UserInfo_t* info);
int DeleteForce(const char* filename, UserInfo_t* info);
DirInfos_t* OpenDir(const char* dirpath, UserInfo_t* userinfo);
void CloseDir(DirInfos_t* dirinfo);
int Listdir(DirInfos_t *dirinfo);
int Mkdir(const char* dirpath, UserInfo_t* info);
int Rmdir(const char* dirpath, UserInfo_t* info);

void UnInit();

int GetClusterId(char* buf = nullptr, int len = 0);

#ifdef __cplusplus
}
#endif

#endif  // !CURVE_LIBCURVE_INTERFACE_H  //NOLINT
