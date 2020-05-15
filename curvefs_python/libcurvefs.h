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

#include "curvefs_python/curve_type.h"

#ifdef __cplusplus
extern "C" {
#endif

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
