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

#include "libcurve_define.h"  // NOLINT

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
    uint32_t        blocksize;
} FileStatInfo_t;

//Storing User Information
typedef struct C_UserInfo {
    //The current owner information needs to end with'\0'
    char owner[NAME_MAX_SIZE];
    //When owner="root", password needs to be provided as the key for calculating the signature
    //password information needs to end with '\0'
    char password[NAME_MAX_SIZE];
} C_UserInfo_t;

typedef struct DirInfo {
    //The directory path of the current listdir
    char*            dirpath;
    //User information for the current listdir operation
    C_UserInfo_t*    userinfo;
    //The current dir size, which is the number of files
    uint64_t         dirSize;
    //The file information content within the current dir is an array
    //fileStat is the header of this array, with an array size of dirSize
    FileStatInfo_t*  fileStat;
} DirInfo_t;

#ifdef __cplusplus
extern "C" {
#endif

const char* LibCurveErrorName(LIBCURVE_ERROR err);

/**
 *Initialize the system
 * @param: path is the configuration file path
 * @return: Successfully returns 0, otherwise returns -1
 */
int Init(const char* path);

/**
 *Open a file , the way qemu to open a file
 * @param: filename File name, which contains user information
 *          For example:/1.img_userinfo_
 * @return: Return the file fd
 */
int Open4Qemu(const char* filename);


/**
 * increase epoch
 * @param: filename, filename include userinfo
 *         e.g: /1.img_userinfo_
 * @return: 0 for success, -1 for fail
 */
int IncreaseEpoch(const char* filename);

/**
 *Open file, non qemu scene
 * @param: filename File name
 * @param: userinfo is the user information of the file to be opened
 * @return: Return the file fd
 */
int Open(const char* filename, const C_UserInfo_t* userinfo);

/**
 *Create File
 * @param: filename File name
 * @param: userinfo is the user information that is currently carried when opening or creating
 * @param: size file length. When create is true, create a file with size length
 * @return: Success returns 0, failure returns less than 0, and there may be multiple possibilities, such as internal errors or the file already exists
 */
int Create(const char* filename,
           const C_UserInfo_t* userinfo,
           size_t size);

/**
 *Synchronous mode reading
 * @param: fd is the file descriptor returned by the current open
 * @param: buf is the current buffer to be read
 * @param: Offset The offset within the file
 * @param: length is the length to be read
 * @return: Successfully returned the read length, otherwise -LIBCURVE_ERROR::FAILED, etc
 */
int Read(int fd, char* buf, off_t offset, size_t length);

/**
 *Synchronous mode write
 * @param: fd is the file descriptor returned by the current open
 * @param: buf is the current buffer to be written
 * @param: Offset The offset within the file
 * @parma: length is the length to be read
 * @return: Successfully returned the write length, otherwise - LIBCURVE_ERROR::FAILED, etc
 */
int Write(int fd, const char* buf, off_t offset, size_t length);

/**
 * @brief Synchronous discard operation
 * @param fd file descriptor
 * @param offset discard offset
 * @param length discard length
 * @return On success, return 0.
 *         On error, returns a negative value.
 */
int Discard(int fd, off_t offset, size_t length);

/**
 *Asynchronous mode read
 * @param: fd is the file descriptor returned by the current open
 * @param: aioctx is an asynchronous read/write IO context that stores basic IO information
 * @return: Successfully returns 0, otherwise - LIBCURVE_ERROR::FAILED
 */
int AioRead(int fd, CurveAioContext* aioctx);

/**
 *Asynchronous mode write
 * @param: fd is the file descriptor returned by the current open
 * @param: aioctx is an asynchronous read/write IO context that stores basic IO information
 * @return: Successfully returns 0, otherwise -LIBCURVE_ERROR::FAILED
 */
int AioWrite(int fd, CurveAioContext* aioctx);

/**
 * @brief Asynchronous discard operation
 * @param fd file descriptor
 * @param aioctx async request context
 * @return 0 means success, otherwise it means failure
 */
int AioDiscard(int fd, CurveAioContext* aioctx);

/**
 *Rename File
 * @param: userinfo is the user information
 * @param: oldpath source path
 * @param: newpath Target Path
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int Rename(const C_UserInfo_t* userinfo, const char* oldpath, const char* newpath);   // NOLINT

/**
 *Extension file
 * @param: userinfo is the user information
 * @param: filename File name
 * @param: newsize New size
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int Extend(const char* filename, const C_UserInfo_t* userinfo, uint64_t newsize);     // NOLINT

/**
 *Expanding files, Qemu scene online expansion
 * @param: filename File name
 * @param: newsize New size
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int Extend4Qemu(const char* filename, int64_t newsize);     // NOLINT


/**
 *Delete files
 * @param: userinfo is the user information
 * @param: filename The file name to be deleted
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int Unlink(const char* filename, const C_UserInfo_t* userinfo);

/**
 *Forced deletion of files, unlink deletion of files on the mds side is not a true deletion,
 *Instead, it was placed in the garbage collection bin, and when deleted using the DeleteForce interface, it was directly deleted
 * @param: userinfo is the user information
 * @param: filename The file name to be deleted
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int DeleteForce(const char* filename, const C_UserInfo_t* userinfo);

/**
 * recover file
 * @param: userinfo
 * @param: filename
 * @param: fileid
 * @return: success 0, otherwise return
 *          -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED and so on
 */
int Recover(const char* filename, const C_UserInfo_t* userinfo,
                                  uint64_t fileId);

/**
 *Open the folder before obtaining directory content
 * @param: userinfo is the user information
 * @param: dirpath is the directory path
 * @return: Successfully returned a non empty DirInfo_ Pointer t, otherwise return a null pointer
 */
DirInfo_t* OpenDir(const char* dirpath, const C_UserInfo_t* userinfo);

/**
 *Enumerate directory contents, only after the user OpenDir is successful can they be listed
 * @param[in][out]: dirinfo is the pointer returned by OpenDir, which internally places the information returned by mds into the substructures
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int Listdir(DirInfo_t* dirinfo);

/**
 *Close Open Folder
 * @param: dirinfo is the dir information returned by opendir
 */
void CloseDir(DirInfo_t* dirinfo);

/**
 *Create directory
 * @param: userinfo is the user information
 * @param: dirpath is the directory path
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int Mkdir(const char* dirpath, const C_UserInfo_t* userinfo);

/**
 *Delete directory
 * @param: userinfo is the user information
 * @param: dirpath is the directory path
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int Rmdir(const char* dirpath, const C_UserInfo_t* userinfo);

/**
 *Obtain file information
 * @param: filename File name
 * @param: userinfo is the user information
 * @param: finfo is an output parameter that carries the basic information of the current file
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int StatFile(const char* filename,
             const C_UserInfo_t* userinfo,
             FileStatInfo* finfo);

/**
 *Obtain file information
 * @param: filename File name
 * @param: finfo is an output parameter that carries the basic information of the current file
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int StatFile4Qemu(const char* filename, FileStatInfo* finfo);

/**
 *Change owner
 * @param: filename The file name to be changed
 * @param: newOwner New owner information
 * @param: userinfo The user information for performing this operation, only the root user can perform changes
 * @return: Successfully returned 0,
 *          Otherwise, return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int ChangeOwner(const char* filename,
                const char* newOwner,
                const C_UserInfo_t* userinfo);

/**
 *close and delete the corresponding instance through fd
 * @param: fd is the file descriptor returned by the current open
 * @return: Successfully returned 0,
 *          Otherwise, it may return to -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED, etc
 */
int Close(int fd);

void UnInit();

/**
 * @brief: Obtain the cluster ID, which is identified by UUID
 * @param: buf Storage Cluster ID
 * @param: The length of buf
 * @return: Successfully returns 0, otherwise returns -LIBCURVE_ERROR::FAILED
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

//Storing User Information
typedef struct UserInfo {
    //Current owner information for execution
    std::string owner;
    //When owner=root, password needs to be provided as the key for calculating the signature
    std::string password;

    UserInfo() = default;

    UserInfo(const std::string& own, const std::string& pwd = "")
      : owner(own), password(pwd) {}

    bool Valid() const {
        return !owner.empty();
    }
} UserInfo_t;

inline bool operator==(const UserInfo& lhs, const UserInfo& rhs) {
    return lhs.owner == rhs.owner && lhs.password == rhs.password;
}

struct OpenFlags {
    bool exclusive;
    std::string confPath;

    OpenFlags() : exclusive(true) {}
};

class CurveClient {
 public:
    CurveClient();
    virtual ~CurveClient();

    /**
     *Initialize
     * @param configPath Configuration file path
     * @return returns an error code
     */
    virtual int Init(const std::string& configPath);

    /**
     *Deinitialization
     */
    virtual void UnInit();

    /**
     * increase epoch
     * @param: filename, filename include userinfo
     *         e.g: /1.img_userinfo_
     * @return: 0 for success, -1 for fail
     */
    virtual int IncreaseEpoch(const std::string& filename);

    /**
     *Open File
     * @param filename File name, format: File name_ User name_
     * @param[out] session Id session Id
     * @return successfully returns fd, failure returns -1
     */
    virtual int Open(const std::string& filename,
                     const OpenFlags& openflags);

    /**
     *Reopen File
     * @param filename File name, format: File name_ User name_
     * @param sessionId session Id
     * @param[out] newSessionId New SessionId after reOpen
     * @return successfully returns fd, failure returns -1
     */
    virtual int ReOpen(const std::string& filename,
                       const OpenFlags& openflags);

    /**
     *Close File
     * @param fd file fd
     * @return returns an error code
     */
    virtual int Close(int fd);

    /**
     *Extension file
     * @param filename File name, format: File name_ User name_
     *The expanded size of  @param newsize
     * @return returns an error code
     */
    virtual int Extend(const std::string& filename,
                       int64_t newsize);

    /**
     *Get File Size
     * @param fd file fd
     * @return returns an error code
     */
    virtual int64_t StatFile(int fd, FileStatInfo* fileStat);

    /**
     *Asynchronous reading
     * @param fd file fd
     * @param aioctx asynchronous read/write IO context
     * @param dataType type of user buffer
     * @return returns an error code
     */
    virtual int AioRead(int fd, CurveAioContext* aioctx, UserDataType dataType);

    /**
     *Asynchronous writing
     * @param fd file fd
     * @param aioctx asynchronous read/write IO context
     * @param dataType type of user buffer
     * @return returns an error code
     */
    virtual int AioWrite(int fd, CurveAioContext* aioctx,
                         UserDataType dataType);

    /**
     * @brief Async Discard
     * @param fd file descriptor
     * @param aioctx async request context
     * @return return error code, 0(LIBCURVE_ERROR::OK) means success
     */
    virtual int AioDiscard(int fd, CurveAioContext* aioctx);

    /**
     * Test usage, set fileclient
     * @param client The fileclient that needs to be set
     */
    void SetFileClient(FileClient* client);

 private:
    FileClient* fileClient_{nullptr};
};

}  // namespace client
}  // namespace curve

#endif  // INCLUDE_CLIENT_LIBCURVE_H_
