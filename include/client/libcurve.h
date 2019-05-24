/*
 * Project: curve
 * File Created: Sunday, 28th April 2019 3:11:27 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef INCLUDE_CLIENT_LIBCURVE_H_
#define INCLUDE_CLIENT_LIBCURVE_H_

#include <unistd.h>
#include <stdint.h>
#include <vector>
#include <map>

#include "src/client/libcurve_define.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * 初始化系统
 * @param: path为配置文件路径
 * @return: 成功返回0，否则返回-1.
 */
int Init(const char* path);

/**
 * 打开文件，qemu打开文件的方式
 * @param: filename文件名, filename中包含用户信息
 *         例如：/1.img_userinfo_
 * @return: 返回文件fd
 */
int Open4Qemu(const char* filename);

/**
 * 打开文件，非qemu场景
 * @param: filename文件名
 * @param: userinfo为要打开的文件的用户信息
 * @return: 返回文件fd
 */
int Open(const char* filename, const C_UserInfo_t* userinfo);

/**
 * 创建文件
 * @param: filename文件名
 * @param: userinfo是当前打开或创建时携带的user信息
 * @param: size文件长度，当create为true的时候以size长度创建文件
 * @return: 成功返回 0, 失败返回小于0，可能有多种可能，比如内部错误，或者文件已存在
 */
int Create(const char* filename,
           const C_UserInfo_t* userinfo,
           size_t size);

/**
 * 同步模式读
 * @param: fd为当前open返回的文件描述符
 * @param: buf为当前待读取的缓冲区
 * @param：offset文件内的偏移
 * @parma：length为待读取的长度
 * @return: 成功返回读取长度, 否则-LIBCURVE_ERROR::FAILED等
 */
int Read(int fd, char* buf, off_t offset, size_t length);

/**
 * 同步模式写
 * @param: fd为当前open返回的文件描述符
 * @param: buf为当前待写入的缓冲区
 * @param：offset文件内的偏移
 * @parma：length为待读取的长度
 * @return: 成功返回 写入长度,否则-LIBCURVE_ERROR::FAILED等
 */
int Write(int fd, const char* buf, off_t offset, size_t length);

/**
 * 异步模式读
 * @param: fd为当前open返回的文件描述符
 * @param: aioctx为异步读写的io上下文，保存基本的io信息
 * @return: 成功返回 0,否则-LIBCURVE_ERROR::FAILED
 */
int AioRead(int fd, CurveAioContext* aioctx);

/**
 * 异步模式写
 * @param: fd为当前open返回的文件描述符
 * @param: aioctx为异步读写的io上下文，保存基本的io信息
 * @return: 成功返回 0,否则-LIBCURVE_ERROR::FAILED
 */
int AioWrite(int fd, CurveAioContext* aioctx);

/**
 * 重命名文件
 * @param: userinfo是用户信息
 * @param: oldpath源路径
 * @param: newpath目标路径
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int Rename(const C_UserInfo_t* userinfo, const char* oldpath, const char* newpath);   // NOLINT

/**
 * 扩展文件
 * @param: userinfo是用户信息
 * @param: filename文件名
 * @param: newsize新的size
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int Extend(const char* filename, const C_UserInfo_t* userinfo, uint64_t newsize);     // NOLINT

/**
 * 扩展文件,Qemu场景在线扩容
 * @param: filename文件名
 * @param: newsize新的size
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int Extend4Qemu(const char* filename, int64_t newsize);     // NOLINT


/**
 * 删除文件
 * @param: userinfo是用户信息
 * @param: filename待删除的文件名
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int Unlink(const char* filename, const C_UserInfo_t* userinfo);

/**
 * 枚举目录内容
 * @param: userinfo是用户信息
 * @param: dirpath是目录路径
 * @param[out]: filestatVec为文件夹内的文件信息
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int Listdir(const char* dirpath,
            const C_UserInfo_t* userinfo,
            FileStatInfo** filestatVec);

/**
 * 创建目录
 * @param: userinfo是用户信息
 * @param: dirpath是目录路径
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int Mkdir(const char* dirpath, const C_UserInfo_t* userinfo);

/**
 * 删除目录
 * @param: userinfo是用户信息
 * @param: dirpath是目录路径
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int Rmdir(const char* dirpath, const C_UserInfo_t* userinfo);

/**
 * 获取文件信息
 * @param: filename文件名
 * @param: userinfo是用户信息
 * @param: finfo是出参，携带当前文件的基础信息
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int StatFile(const char* filename,
             const C_UserInfo_t* userinfo,
             FileStatInfo* finfo);

/**
 * 获取文件信息
 * @param: filename文件名
 * @param: finfo是出参，携带当前文件的基础信息
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int StatFile4Qemu(const char* filename, FileStatInfo* finfo);

/**
 * close通过fd找到对应的instance进行删除
 * @param: fd为当前open返回的文件描述符
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int Close(int fd);

void UnInit();

#ifdef __cplusplus
}
#endif

#endif  // INCLUDE_CLIENT_LIBCURVE_H_
