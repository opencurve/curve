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

/*
 * Project: curve
 * File Created: Sunday, 28th April 2019 3:11:27 pm
 * Author: tongguangxun
 */

#ifndef INCLUDE_CLIENT_LIBCURVE_H_
#define INCLUDE_CLIENT_LIBCURVE_H_

#include <unistd.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <string>

#define IO_ALIGNED_BLOCK_SIZE 4096
#define PATH_MAX_SIZE         4096
#define NAME_MAX_SIZE         256

enum FileType {
    INODE_DIRECTORY = 0,
    INODE_PAGEFILE = 1,
    INODE_APPENDFILE = 2,
    INODE_APPENDECFILE = 3,
    INODE_SNAPSHOT_PAGEFILE = 4
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
    // session不存在
    SESSION_NOT_EXIST       = 25,
    // 状态异常
    STATUS_NOT_MATCH        = 26,
    // 删除文件正常被克隆
    DELETE_BEING_CLONED     = 27,
    // client版本不支持快照
    CLIENT_NOT_SUPPORT_SNAPSHOT = 28,
    // snapshot功能禁用中
    SNAPSTHO_FROZEN = 29,
    // You must retry it until success
    RETRY_UNTIL_SUCCESS = 30,
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
    char            filename[NAME_MAX_SIZE];
    char            owner[NAME_MAX_SIZE];
    int             fileStatus;
    uint64_t        stripeUnit;
    uint64_t        stripeCount;
} FileStatInfo_t;

// 存储用户信息
typedef struct C_UserInfo {
    // 当前执行的owner信息, owner信息需要以'\0'结尾
    char owner[NAME_MAX_SIZE];
    // 当owner="root"的时候，需要提供password作为计算signature的key
    // password信息需要以'\0'结尾
    char password[NAME_MAX_SIZE];
} C_UserInfo_t;

typedef struct DirInfo {
    // 当前listdir的目录路径
    char*            dirpath;
    // 当前listdir操作的用户信息
    C_UserInfo_t*    userinfo;
    // 当前dir大小，也就是文件数量
    uint64_t         dirSize;
    // 当前dir的内的文件信息内容，是一个数组
    // fileStat是这个数组的头，数组大小为dirSize
    FileStatInfo_t*  fileStat;
} DirInfo_t;

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
 * create file with stripe
 * @param: filename  file name
 * @param: userinfo  user info
 * @param: size      file size
 * @param: stripeUnit block in stripe size
 * @param: stripeCount stripe count in one stripe
 *
 * @return: success return 0, fail return less than 0
 */
int Create2(const char* filename,
           const C_UserInfo_t* userinfo,
           size_t size, uint64_t stripeUnit, uint64_t stripeCount);

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
 * 强制删除文件, unlink删除文件在mds一侧并不是真正的删除，
 * 而是放到了垃圾回收站，当使用DeleteForce接口删除的时候是直接删除
 * @param: userinfo是用户信息
 * @param: filename待删除的文件名
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int DeleteForce(const char* filename, const C_UserInfo_t* userinfo);

/**
 * 在获取目录内容之前先打开文件夹
 * @param: userinfo是用户信息
 * @param: dirpath是目录路径
 * @return: 成功返回一个非空的DirInfo_t指针，否则返回一个空指针
 */
DirInfo_t* OpenDir(const char* dirpath, const C_UserInfo_t* userinfo);

/**
 * 枚举目录内容, 用户OpenDir成功之后才能list
 * @param[in][out]: dirinfo为OpenDir返回的指针, 内部会将mds返回的信息放入次结构中
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int Listdir(DirInfo_t* dirinfo);

/**
 * 关闭打开的文件夹
 * @param: dirinfo为opendir返回的dir信息
 */
void CloseDir(DirInfo_t* dirinfo);

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
 * 变更owner
 * @param: filename待变更的文件名
 * @param: newOwner新的owner信息
 * @param: userinfo执行此操作的user信息，只有root用户才能执行变更
 * @return: 成功返回0，
 *          否则返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int ChangeOwner(const char* filename,
                const char* newOwner,
                const C_UserInfo_t* userinfo);

/**
 * close通过fd找到对应的instance进行删除
 * @param: fd为当前open返回的文件描述符
 * @return: 成功返回 0,
 *          否则可能返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
 */
int Close(int fd);

void UnInit();

/**
 * @brief: 获取集群id, id用UUID标识
 * @param: buf存放集群id
 * @param: buf的长度
 * @return: 成功返回0, 否则返回-LIBCURVE_ERROR::FAILED
 */
int GetClusterId(char* buf, int len);

#ifdef __cplusplus
}
#endif

namespace curve {
namespace client {

class FileClient;

enum class UserDataType {
    RawBuffer,  // char*
    IOBuffer   // butil::IOBuf*
};

// 存储用户信息
typedef struct UserInfo {
    // 当前执行的owner信息
    std::string owner;
    // 当owner=root的时候，需要提供password作为计算signature的key
    std::string password;

    UserInfo() = default;

    UserInfo(const std::string& own, const std::string& pwd)
      : owner(own), password(pwd) {}

    bool Valid() const {
        return !owner.empty();
    }
} UserInfo_t;

struct OpenFlags {
    bool exclusive;

    OpenFlags() : exclusive(true) {}
};

class CurveClient {
 public:
    CurveClient();
    virtual ~CurveClient();

    /**
     * 初始化
     * @param configPath 配置文件路径
     * @return 返回错误码
     */
    virtual int Init(const std::string& configPath);

    /**
     * 反初始化
     */
    virtual void UnInit();

    /**
     * 打开文件
     * @param filename 文件名，格式为：文件名_用户名_
     * @param[out] sessionId session Id
     * @return 成功返回fd，失败返回-1
     */
    virtual int Open(const std::string& filename,
                     const OpenFlags& openflags);

    /**
     * 重新打开文件
     * @param filename 文件名，格式为：文件名_用户名_
     * @param sessionId session Id
     * @param[out] newSessionId reOpen之后的新sessionId
     * @return 成功返回fd，失败返回-1
     */
    virtual int ReOpen(const std::string& filename,
                       const OpenFlags& openflags);

    /**
     * 关闭文件
     * @param fd 文件fd
     * @return 返回错误码
     */
    virtual int Close(int fd);

    /**
     * 扩展文件
     * @param filename 文件名，格式为：文件名_用户名_
     * @param newsize 扩展后的大小
     * @return 返回错误码
     */
    virtual int Extend(const std::string& filename,
                       int64_t newsize);

    /**
     * 获取文件大小
     * @param filename 文件名，格式为：文件名_用户名_
     * @return 返回错误码
     */
    virtual int64_t StatFile(const std::string& filename);

    /**
     * 异步读
     * @param fd 文件fd
     * @param aioctx 异步读写的io上下文
     * @param dataType type of user buffer
     * @return 返回错误码
     */
    virtual int AioRead(int fd, CurveAioContext* aioctx, UserDataType dataType);

    /**
     * 异步写
     * @param fd 文件fd
     * @param aioctx 异步读写的io上下文
     * @param dataType type of user buffer
     * @return 返回错误码
     */
    virtual int AioWrite(int fd, CurveAioContext* aioctx,
                         UserDataType dataType);

    /**
     * 测试使用，设置fileclient
     * @param client 需要设置的fileclient
     */
    void SetFileClient(FileClient* client);

 private:
    FileClient* fileClient_{nullptr};
};

}  // namespace client
}  // namespace curve

#endif  // INCLUDE_CLIENT_LIBCURVE_H_
