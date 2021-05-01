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
    // operation succeed
    OK                      = 0,
    // file or directory already exists
    EXISTS                  = 1,
    // operation failed
    FAILED                  = 2,
    // disable IO
    DISABLEIO               = 3,
    // authentication failed
    AUTHFAIL                = 4,
    // deleting
    DELETING                = 5,
    // file not exists
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
    // disable shrinkage
    NO_SHRINK_BIGGER_FILE   = 13,
    // session not exists
    SESSION_NOTEXISTS       = 14,
    // file occupied
    FILE_OCCUPIED           = 15,
    // param error
    PARAM_ERROR             = 16,
    // MDS storage error
    INTERNAL_ERROR           = 17,
    // crc check error
    CRC_ERROR               = 18,
    // invalid request param
    INVALID_REQUEST         = 19,
    // disk error
    DISK_FAIL               = 20,
    // space not enough
    NO_SPACE                = 21,
    // IO not aligned
    NOT_ALIGNED             = 22,
    // fd under close, not available
    BAD_FD                  = 23,
    // file length not supported
    LENGTH_NOT_SUPPORT      = 24,
    // session not exists
    SESSION_NOT_EXIST       = 25,
    // invalid status
    STATUS_NOT_MATCH        = 26,
    // deleted file being cloned
    DELETE_BEING_CLONED     = 27,
    // snapshot not supported at this client version
    CLIENT_NOT_SUPPORT_SNAPSHOT = 28,
    // disable snapshot
    SNAPSHOT_FROZEN = 29,
    // unknown error
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

// store user information
typedef struct C_UserInfo {
    // information of the current owner, which should be '\0' terminated
    char owner[NAME_MAX_SIZE];
    // when owner="root", you need to provide the password as the key
    // to calculate the signature
    // password should also be '\0' terminated
    char password[NAME_MAX_SIZE];
} C_UserInfo_t;

typedef struct DirInfo {
    // directory path
    char*            dirpath;
    // user info
    C_UserInfo_t*    userinfo;
    // directory size (i.e. number of files)
    uint64_t         dirSize;
    // an array of file infos within this directory
    // fileStat is the head of this array, while dirSize is the size
    FileStatInfo_t*  fileStat;
} DirInfo_t;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Initialize the system
 * @param path config file path
 * @return success return 0, fail return -1
 */
int Init(const char* path);

/**
 * @brief open file (for QEMU)
 * @param filename filename, which contains user info (e.g. /1.img_userinfo_)
 * @return fd
 */
int Open4Qemu(const char* filename);

/**
 * @brief open file (for non-QEMU)
 * @param filename filename
 * @param userinfo user info of the opened file
 * @return fd
 */
int Open(const char* filename, const C_UserInfo_t* userinfo);

/**
 * @brief create file
 * @param filename filename
 * @param userinfo user info
 * @param size file size
 * @return success return 0, fail return less than 0, and there
 *          could be different returned values for different
 *          reasons (e.g. internal error, file already exists)
 */
int Create(const char* filename,
           const C_UserInfo_t* userinfo,
           size_t size);

/**
 * @brief create file with stripe
 * @param filename  file name
 * @param userinfo  user info
 * @param size      file size
 * @param stripeUnit block in stripe size
 * @param stripeCount stripe count in one stripe
 *
 * @return success return 0, fail return less than 0
 */
int Create2(const char* filename,
           const C_UserInfo_t* userinfo,
           size_t size, uint64_t stripeUnit, uint64_t stripeCount);

/**
 * @brief synchronous read
 * @param fd fd returned by open
 * @param buf buffer to be read
 * @param offset offset within the file
 * @param length length to be read
 * @return success return bytes read, fail return error code less than 0 like -LIBCURVE_ERROR::FAILED
 */
int Read(int fd, char* buf, off_t offset, size_t length);

/**
 * @brief synchronous write
 * @param fd fd returned by open
 * @param buf buffer to be written
 * @param offset offset within the file
 * @param length length to be written
 * @return success return bytes written, fail return error code less than 0 like -LIBCURVE_ERROR::FAILED
 */
int Write(int fd, const char* buf, off_t offset, size_t length);

/**
 * @brief asynchronous read
 * @param fd fd returned by open
 * @param aioctx I/O context of the async read/write, saving the basic I/O info
 * @return success return 0, fail return -LIBCURVE_ERROR::FAILED
 */
int AioRead(int fd, CurveAioContext* aioctx);

/**
 * @brief asynchronous write
 * @param fd fd returned by open
 * @param aioctx I/O context of the async read/write, saving the basic I/O info
 * @return success return 0, fail return -LIBCURVE_ERROR::FAILED
 */
int AioWrite(int fd, CurveAioContext* aioctx);

/**
 * @brief rename file
 * @param userinfo user info
 * @param oldpath source path
 * @param newpath target path
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int Rename(const C_UserInfo_t* userinfo, const char* oldpath, const char* newpath);   // NOLINT

/**
 * @brief extend file (for non-QEMU)
 * @param userinfo user info
 * @param filename filename
 * @param newsize new size
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int Extend(const char* filename, const C_UserInfo_t* userinfo, uint64_t newsize);     // NOLINT

/**
 * @brief extend file (for QEMU)
 * @param filename filename
 * @param newsize new size
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int Extend4Qemu(const char* filename, int64_t newsize);     // NOLINT


/**
 * @brief delete file
 * @param userinfo user info
 * @param filename filename of the file to be deleted
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int Unlink(const char* filename, const C_UserInfo_t* userinfo);

/**
 * @brief forced delete file, unlink just put file into trash rather than delete
 * it on MDS side, while DeleteForce delete file exactly on MDS side 
 * @param userinfo user info
 * @param filename filename of the file to be deleted
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int DeleteForce(const char* filename, const C_UserInfo_t* userinfo);

/**
 * @brief recover file
 * @param userinfo user info
 * @param filename filename
 * @param fileid file id
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int Recover(const char* filename, const C_UserInfo_t* userinfo,
                                  uint64_t fileId);

/**
 * @brief open directory before getting the content
 * @param userinfo user info
 * @param dirpath directory path
 * @return success return a non-null pointer DirInfo_t, otherwise return nullptr
 */
DirInfo_t* OpenDir(const char* dirpath, const C_UserInfo_t* userinfo);

/**
 * @brief list all the contents in the directory, only could be called after calling OpenDir successfully
 * @param[in][out] dirinfo is the pointer returned by OpenDir, which contains info returned from MDS
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int Listdir(DirInfo_t* dirinfo);

/**
 * @brief close opened directory
 * @param dirinfo is the directory info returned by OpenDir
 */
void CloseDir(DirInfo_t* dirinfo);

/**
 * @brief create directory
 * @param userinfo user info
 * @param dirpath target path
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int Mkdir(const char* dirpath, const C_UserInfo_t* userinfo);

/**
 * @brief delete directory
 * @param userinfo user info
 * @param dirpath directory path
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int Rmdir(const char* dirpath, const C_UserInfo_t* userinfo);

/**
 * @brief get file info (for non-QEMU)
 * @param filename filename
 * @param userinfo user info
 * @param[out] finfo basic infomations of the file
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int StatFile(const char* filename,
             const C_UserInfo_t* userinfo,
             FileStatInfo* finfo);

/**
 * @brief get file info (for QEMU)
 * @param filename filename
 * @param[out] finfo basic infomations of the file
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int StatFile4Qemu(const char* filename, FileStatInfo* finfo);

/**
 * @brief change the owner of the file
 * @param filename filename
 * @param newOwner new owner
 * @param userinfo user info that made the operation, only root user
 *         could change it
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int ChangeOwner(const char* filename,
                const char* newOwner,
                const C_UserInfo_t* userinfo);

/**
 * @brief find the corresponding instance with fd and delete it
 * @param fd fd returned by open
 * @return success return 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int Close(int fd);

void UnInit();

/**
 * @brief get cluster id, which is identified by UUID
 * @param buf put the cluster id here
 * @param len size of the buf
 * @return success return 0, otherwise return -LIBCURVE_ERROR::FAILED
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

// Store user info
typedef struct UserInfo {
    // info of the current owner
    std::string owner;
    // when owner="root", you need to provide the password as the key
    // to calculate the signature
    std::string password;

    UserInfo() = default;

    UserInfo(const std::string& own, const std::string& pwd)
      : owner(own), password(pwd) {}

    bool Valid() const {
        return !owner.empty();
    }
} UserInfo_t;

class CurveClient {
 public:
    CurveClient();
    virtual ~CurveClient();

    /**
     * @brief Initialization
     * @param configPath config file path
     * @return error code
     */
    virtual int Init(const std::string& configPath);

    /**
     * @brief Uninitialization
     */
    virtual void UnInit();

    /**
     * @brief open file
     * @param filename filename, format: filename_username_
     * @param[out] sessionId session Id
     * @return success return fd, fail return -1
     */
    virtual int Open(const std::string& filename,
                     std::string* sessionId);

    /**
     * @brief reopen file
     * @param filename filename, format: filename_username_
     * @param sessionId session Id
     * @param[out] newSessionId new sessionId after reOpen
     * @return success return fd, fail return -1
     */
    virtual int ReOpen(const std::string& filename,
                       const std::string& sessionId,
                       std::string* newSessionId);

    /**
     * @brief close file
     * @param fd fd
     * @return error code
     */
    virtual int Close(int fd);

    /**
     * @brief extendd file
     * @param filename filename, format: filename_username_
     * @param newsize new size after extending
     * @return error code
     */
    virtual int Extend(const std::string& filename,
                       int64_t newsize);

    /**
     * @brief get file size
     * @param filename filename, format: filename_username_
     * @return error code
     */
    virtual int64_t StatFile(const std::string& filename);

    /**
     * @brief asynchronous read
     * @param fd fd
     * @param aioctx aioctx I/O context of the async read/write
     * @param dataType type of user buffer
     * @return error code
     */
    virtual int AioRead(int fd, CurveAioContext* aioctx, UserDataType dataType);

    /**
     * @brief asynchronous write
     * @param fd fd
     * @param aioctx aioctx I/O context of the async read/write
     * @param dataType type of user buffer
     * @return error code
     */
    virtual int AioWrite(int fd, CurveAioContext* aioctx,
                         UserDataType dataType);

    /**
     * @brief test use, set the fileclient
     * @param client fileclient to be set
     */
    void SetFileClient(FileClient* client);

 private:
    FileClient* fileClient_{nullptr};
};

}  // namespace client
}  // namespace curve

#endif  // INCLUDE_CLIENT_LIBCURVE_H_
