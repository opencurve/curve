/*
 * Project: curve
 * File Created: Monday, 18th February 2019 11:02:39 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_LIBCURVE_DEFINE_H_
#define SRC_CLIENT_LIBCURVE_DEFINE_H_
#include <stdint.h>

#define IO_ALIGNED_BLOCK_SIZE 4096

enum FileType {
    INODE_DIRECTORY = 0,
    INODE_PAGEFILE = 1,
    INODE_APPENDFILE = 2,
    INODE_APPENDECFILE = 3,
};

enum LIBCURVE_ERROR {
    // 操作成功
    OK                      = 0,
    // 文件或者目录已存在
    EXISTS                  = 1,
    // 操作失败
    FAILED                  = 2,
    // 禁止IO
    DISABLEIO               = 3,
    // 认证失败
    AUTHFAIL                = 4,
    // 正在删除
    DELETING                = 5,
    // 文件不存在
    NOTEXIST                = 6,
    // 快照中
    UNDER_SNAPSHOT          = 7,
    // 非快照期间
    NOT_UNDERSNAPSHOT       = 8,
    // 删除错误
    DELETE_ERROR            = 9,
    // segment未分配
    NOT_ALLOCATE            = 10,
    // 操作不支持
    NOT_SUPPORT             = 11,
    // 目录非空
    NOT_EMPTY               = 12,
    // 禁止缩容
    NO_SHRINK_BIGGER_FILE   = 13,
    // session不存在
    SESSION_NOTEXISTS       = 14,
    // 文件被占用
    FILE_OCCUPIED           = 15,
    // 参数错误
    PARAM_ERROR             = 16,
    // MDS一侧存储错误
    INTERNAL_ERROR           = 17,
    // crc检查错误
    CRC_ERROR               = 18,
    // request参数存在问题
    INVALID_REQUEST         = 19,
    // 磁盘存在问题
    DISK_FAIL               = 20,
    // 空间不足
    NO_SPACE                = 21,
    // IO未对齐
    NOT_ALIGNED             = 22,
    // 文件正在被关闭，fd不可用
    BAD_FD                  = 23,
    // 文件长度不满足要求
    LENGTH_NOT_SUPPORT      = 24,
    // 未知错误
    UNKNOWN                 = 100
};

const char* ErrorNum2ErrorName(LIBCURVE_ERROR err);

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
    LibCurveAioCallBack cb;
    void* buf;
} CurveAioContext;

typedef struct FileStatInfo {
    uint64_t        id;
    uint64_t        parentid;
    FileType        filetype;
    uint64_t        length;
    uint64_t        ctime;
} FileStatInfo_t;

// 存储用户信息
typedef struct C_UserInfo {
    // 当前执行的owner信息, owner信息需要以'\0'结尾
    char owner[256];
    // 当owner="root"的时候，需要提供password作为计算signature的key
    // password信息需要以'\0'结尾
    char password[256];
} C_UserInfo_t;

#endif  // SRC_CLIENT_LIBCURVE_DEFINE_H_
