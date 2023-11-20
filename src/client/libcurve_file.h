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
 * File Created: Monday, 13th February 2019 9:46:54 am
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_LIBCURVE_FILE_H_
#define SRC_CLIENT_LIBCURVE_FILE_H_

#include <unistd.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "include/client/libcurve.h"
#include "src/client/chunkserver_broadcaster.h"
#include "src/client/client_common.h"
#include "src/client/file_instance.h"
#include "src/common/concurrent/rw_lock.h"

// TODO(tongguangxun): Add key function trace function
namespace curve {
namespace client {

using curve::common::BthreadRWLock;

class FileClient {
 public:
    FileClient();
    virtual ~FileClient() = default;

    /**
     * file object initialization function
     * @param: Configuration file path
     */
    virtual int Init(const std::string& configpath);

    /**
     * Open or create a file
     * @param: filename File name
     * @param: userinfo is the user information for operating the file
     * @return: Return the file fd
     */
    virtual int Open(const std::string& filename, const UserInfo_t& userinfo,
                     const OpenFlags& openflags = {});

    /**
     * Open the file. This only creates an fd and does not interact with mds.
     * There is no session renewal This Open interface is mainly provided for
     * data copying in snapshot clone image systems
     * @param: filename File name
     * @param: userinfo Current user information
     * @param disableStripe enable/disable stripe feature for a stripe file
     * @return: Return the file fd
     */
    virtual int Open4ReadOnly(const std::string& filename,
                              const UserInfo_t& userinfo,
                              bool disableStripe = false);

    /**
     * @brief increase epoch
     *
     * @param filename file name
     * @param userinfo user info
     *
     * @return 0 for success, -1 for fail
     */
    int IncreaseEpoch(const std::string& filename, const UserInfo_t& userinfo);

    /**
     * Create File
     * @param: filename File name
     * @param: userinfo is the user information that is currently carried when
     * opening or creating
     * @param: size file length. When create is true, create a file with size
     * length
     * @return: Success returns 0, failure may have multiple possibilities
     *          For example, internal errors or files that already exist
     */
    virtual int Create(const std::string& filename, const UserInfo_t& userinfo,
                       size_t size);

    /**
     * Create file with parameters
     * @return: success return 0,  fail return less than 0
     */
    virtual int Create2(const CreateFileContext& context);

    /**
     * Synchronous mode reading
     * @param: fd is the file descriptor returned by the current open
     * @param: buf is the current buffer to be read
     * @param: offset within the file
     * @parma: length is the length to be read
     * @return: Successfully returned the number of bytes read, otherwise an
     * error code less than 0 will be returned
     */
    virtual int Read(int fd, char* buf, off_t offset, size_t length);

    /**
     * Synchronous mode write
     * @param: fd is the file descriptor returned by the current open
     * @param: buf is the current buffer to be written
     * @param: offset within the file
     * @parma: length is the length to be read
     * @return: Successfully returns the number of bytes written, otherwise
     * returns an error code less than 0
     */
    virtual int Write(int fd, const char* buf, off_t offset, size_t length);

    /**
     * @brief Synchronous discard operation
     * @param fd file descriptor
     * @param offset discard offset
     * @param length discard length
     * @return On success, returns 0.
     *         On error, returns a negative value.
     */
    virtual int Discard(int fd, off_t offset, size_t length);

    /**
     * Asynchronous mode read
     * @param: fd is the file descriptor returned by the current open
     * @param: aioctx is an asynchronous read/write IO context that stores basic
     * IO information
     * @param dataType type of aioctx->buf, default is `UserDataType::RawBuffer`
     * @return: Successfully returned the number of bytes read, otherwise an
     * error code less than 0 will be returned
     */
    virtual int AioRead(int fd, CurveAioContext* aioctx,
                        UserDataType dataType = UserDataType::RawBuffer);

    /**
     * Asynchronous mode write
     * @param: fd is the file descriptor returned by the current open
     * @param: aioctx is an asynchronous read/write IO context that stores basic
     * IO information
     * @param dataType type of aioctx->buf, default is `UserDataType::RawBuffer`
     * @return: Successfully returns the number of bytes written, otherwise
     * returns an error code less than 0
     */
    virtual int AioWrite(int fd, CurveAioContext* aioctx,
                         UserDataType dataType = UserDataType::RawBuffer);

    /**
     * @brief Asynchronous discard operation
     * @param fd file descriptor
     * @param aioctx async request context
     * @return 0 means success, otherwise it means failure
     */
    virtual int AioDiscard(int fd, CurveAioContext* aioctx);

    /**
     * Rename File
     * @param: userinfo is the user information
     * @param: oldpath Yuanlujin
     * @param: newpath Target Path
     */
    virtual int Rename(const UserInfo_t& userinfo, const std::string& oldpath,
                       const std::string& newpath);

    /**
     * Extension file
     * @param: userinfo is the user information
     * @param: filename File name
     * @param: newsize New size
     */
    virtual int Extend(const std::string& filename, const UserInfo_t& userinfo,
                       uint64_t newsize);

    /**
     * Delete files
     * @param: userinfo is the user information
     * @param: filename The file name to be deleted
     * @param: deleteforce=true can only be used to delete from the recycle bin,
     * false means to put it in the trash can
     */
    virtual int Unlink(const std::string& filename, const UserInfo_t& userinfo,
                       bool deleteforce = false);

    /**
     * recycle file
     * @param: userinfo
     * @param: filename
     * @param: fileId
     */
    virtual int Recover(const std::string& filename, const UserInfo_t& userinfo,
                        uint64_t fileId);

    /**
     * Enumerate directory contents
     * @param: userinfo is the user information
     * @param: dirpath is the directory path
     * @param[out]: filestatVec File information in the current folder
     */
    virtual int Listdir(const std::string& dirpath, const UserInfo_t& userinfo,
                        std::vector<FileStatInfo>* filestatVec);

    /**
     * Create directory
     * @param: userinfo is the user information
     * @param: dirpath is the directory path
     */
    virtual int Mkdir(const std::string& dirpath, const UserInfo_t& userinfo);

    /**
     * Delete directory
     * @param: userinfo is the user information
     * @param: dirpath is the directory path
     */
    virtual int Rmdir(const std::string& dirpath, const UserInfo_t& userinfo);

    /**
     * Obtain file information
     * @param: filename File name
     * @param: userinfo is the user information
     * @param: finfo is an output parameter that carries the basic information
     * of the current file
     * @return: Success returns int::OK, otherwise an error code less than 0
     * will be returned
     */
    virtual int StatFile(const std::string& filename,
                         const UserInfo_t& userinfo, FileStatInfo* finfo);

    /**
     * stat file
     * @param: fd is file descriptor.
     * @param: finfo is an output para, carry the base info of current file.
     * @return: returns int::ok if success,
     *          otherwise returns an error code less than 0
     */
    virtual int StatFile(int fd, FileStatInfo* finfo);

    /**
     * Change owner
     * @param: filename The file name to be changed
     * @param: newOwner New owner information
     * @param: userinfo The user information for performing this operation, only
     * the root user can perform changes
     * @return: Successfully returned 0,
     *         Otherwise, return to
     * -LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED, etc
     */
    virtual int ChangeOwner(const std::string& filename,
                            const std::string& newOwner,
                            const UserInfo_t& userinfo);
    /**
     * close and delete the corresponding instance through fd
     * @param: fd is the file descriptor returned by the current open
     * @return: Success returns int::OK, otherwise an error code less than 0
     * will be returned
     */
    virtual int Close(int fd);

    /**
     * Deconstruct and recycle resources
     */
    virtual void UnInit();

    /**
     * @brief: Obtain cluster ID
     * @param: buf Storage Cluster ID
     * @param: The length of buf
     * @return: Success returns 0, failure returns -LIBCURVE_ERROR::FAILED
     */
    int GetClusterId(char* buf, int len);

    /**
     * @brief Get cluster ID
     * @return Successfully returned cluster ID, failed returned empty
     */
    std::string GetClusterId();

    /**
     * @brief to obtain file information for testing purposes
     * @param fd file handle
     * @param[out] finfo file information
     * @return success returns 0, failure returns -LIBCURVE_ERROR::FAILED
     */
    int GetFileInfo(int fd, FInfo* finfo);

    // List all poolsets' name in cluster
    std::vector<std::string> ListPoolset();

    /**
     * Test usage to obtain the current number of mounted files
     * @return Returns the current number of mounted files
     */
    uint64_t GetOpenedFileNum() const { return openedFileNum_.get_value(); }

 private:
    static void BuildFileStatInfo(const FInfo_t& fi, FileStatInfo* finfo);

    bool StartDummyServer();

 private:
    BthreadRWLock rwlock_;

    // The file descriptor returned upwards, for QEMU, one vdisk corresponds to
    // one file descriptor
    std::atomic<uint64_t> fdcount_;

    // Each vdisk has a FileInstance, which is mapped to the corresponding
    // instance through the returned fd
    std::unordered_map<int, FileInstance*> fileserviceMap_;
    // <filename, FileInstance>
    std::unordered_map<std::string, FileInstance*> fileserviceFileNameMap_;

    // FileClient Configuration
    ClientConfig clientconfig_;

    // Global mdsclient corresponding to fileclient
    std::shared_ptr<MDSClient> mdsClient_;

    // chunkserver client
    std::shared_ptr<ChunkServerClient> csClient_;
    // chunkserver broadCaster
    std::shared_ptr<ChunkServerBroadCaster> csBroadCaster_;

    // Is initialization successful
    bool inited_;

    // Number of mounted files
    bvar::Adder<uint64_t> openedFileNum_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_LIBCURVE_FILE_H_
