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
#include <string>
#include <unordered_map>
#include <vector>

#include "include/client/libcurve.h"
#include "src/client/client_common.h"
#include "src/client/config_info.h"
#include "src/client/file_instance.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/uuid.h"

// TODO(tongguangxun) :添加关键函数trace功能
namespace curve {
namespace client {

using curve::common::BthreadRWLock;

class LoggerGuard {
 public:
    explicit LoggerGuard(const std::string& confpath) {
        InitInternal(confpath);
    }

    ~LoggerGuard() { google::ShutdownGoogleLogging(); }

 private:
    static void InitInternal(const std::string& confpath);
};

// FileClient is the management class of vdisk,
// a QEMU corresponds to multiple vdisks
class FileClient {
 public:
    FileClient();
    virtual ~FileClient() = default;

    /**
     * @brief initialization
     * @param config file path
     */
    virtual int Init(const std::string& configpath);

    /**
     * @brief open or create file
     * @param filename filename
     * @param userinfo user info
     * @return fd
     */
    virtual int Open(const std::string& filename,
                     const UserInfo_t& userinfo,
                     std::string* sessionId = nullptr);

    /**
     * @brief reopen file
     * @param filename filename
     * @param sessionId the sessionId returned when the file was opened last time
     * @param userinfo user info
     * @return fd
     */

    virtual int ReOpen(const std::string& filename,
                       const std::string& sessionId,
                       const UserInfo& userInfo,
                       std::string* newSessionId);

    /**
     * @brief open file, it just creates an fd, but does not interact with
     * mds, so there is no session renewal
     * 
     * this Open interface is mainly provided to the snapshot clone mirroring system
     * for data copy use
     * @param filename filename
     * @param userinfo user info
     * @param disableStripe enable/disable stripe feature for a stripe file
     * @return fd
     */
    virtual int Open4ReadOnly(const std::string& filename,
                              const UserInfo_t& userinfo,
                              bool disableStripe = false);

    /**
     * @brief create file
     * @param filename filename
     * @param userinfo user info
     * @param size file size
     * @return success return 0, fail return less than 0, and there
     *          could be different returned values for different
     *          reasons (e.g. internal error, file already exists)
     */
    virtual int Create(const std::string& filename,
                       const UserInfo_t& userinfo,
                       size_t size);

    /**
     * @brief create file with stripe
     * @param filename file name
     * @param userinfo user info
     * @param size file size
     * @param stripeUnit block in stripe size
     * @param stripeCount stripe count in one stripe
     * @return success return 0, fail return less than 0
     *
     */
    virtual int Create2(const std::string& filename,
                        const UserInfo_t& userinfo,
                        size_t size, uint64_t stripeUnit,
                        uint64_t stripeCount);

    /**
     * @brief synchronous read
     * @param fd fd returned by open
     * @param buf buffer to be read
     * @param offset offset within the file
     * @param length length to be read
     * @return success return bytes read, fail return error code less than 0
     */
    virtual int Read(int fd, char* buf, off_t offset, size_t length);

    /**
     * @brief synchronous write
     * @param fd fd returned by open
     * @param buf buffer to be written
     * @param offset offset within the file
     * @param length length to be written
     * @return success return bytes written, fail return error code less than 0
     */
    virtual int Write(int fd, const char* buf, off_t offset, size_t length);

    /**
     * @brief asynchronous read
     * @param fd fd returned by open
     * @param aioctx I/O context of the async read/write, saving the basic I/O info
     * @param dataType type of aioctx->buf, default is `UserDataType::RawBuffer`
     * @return success return bytes read, fail return error code less than 0
     */
    virtual int AioRead(int fd, CurveAioContext* aioctx,
                        UserDataType dataType = UserDataType::RawBuffer);

    /**
     * @brief asynchronous write
     * @param fd fd returned by open
     * @param aioctx I/O context of the async read/write, saving the basic I/O info
     * @param dataType type of aioctx->buf, default is `UserDataType::RawBuffer`
     * @return success return bytes written, fail return error code less than 0
     */
    virtual int AioWrite(int fd, CurveAioContext* aioctx,
                         UserDataType dataType = UserDataType::RawBuffer);

    /**
     * @brief rename file
     * @param userinfo user info
     * @param oldpath source path
     * @param newpath target path
     */
    virtual int Rename(const UserInfo_t& userinfo,
                       const std::string& oldpath,
                       const std::string& newpath);

    /**
     * @brief extend file
     * @param userinfo user info
     * @param filename filename
     * @param newsize new size
     */
    virtual int Extend(const std::string& filename,
                       const UserInfo_t& userinfo,
                       uint64_t newsize);

    /**
     * @brief delete file
     * @param userinfo user info
     * @param filename filename of the file to be deleted
     * @param deleteforce delete without putting into the trash if true, otherwise
     *         just put the file into the trash
     */
    virtual int Unlink(const std::string& filename,
                       const UserInfo_t& userinfo,
                       bool deleteforce = false);

    /**
     * @brief recycle file
     * @param userinfo user info
     * @param filename filename
     * @param fileId file id
     */
    virtual int Recover(const std::string& filename,
                        const UserInfo_t& userinfo,
                        uint64_t fileId);

    /**
     * @brief list all the contents in the directory
     * @param userinfo user info
     * @param dirpath target path
     * @param[out] filestatVec file information in the current directory
     */
    virtual int Listdir(const std::string& dirpath,
                        const UserInfo_t& userinfo,
                        std::vector<FileStatInfo>* filestatVec);

    /**
     * @brief create directory
     * @param userinfo user info
     * @param dirpath target path
     */
    virtual int Mkdir(const std::string& dirpath, const UserInfo_t& userinfo);

    /**
     * @brief delete directory
     * @param userinfo user info
     * @param dirpath target path
     */
    virtual int Rmdir(const std::string& dirpath, const UserInfo_t& userinfo);

    /**
     * @brief get file info
     * @param filename filename
     * @param userinfo user info
     * @param[out] finfo basic infomations of the file
     * @return success return int::OK, fail return error code less than 0
     */
    virtual int StatFile(const std::string& filename,
                         const UserInfo_t& userinfo,
                         FileStatInfo* finfo);

    /**
     * @brief change the owner of the file
     * @param filename filename
     * @param newOwner new owner
     * @param userinfo user info that made the operation, only root user
     *         could change it
     * @return success return 0, fail return error code less than 0, like
     *          -LIBCURVE_ERROR::FAILED, -LIBCURVE_ERROR::AUTHFAILED
     */
    virtual int ChangeOwner(const std::string& filename,
                            const std::string& newOwner,
                            const UserInfo_t& userinfo);
    /**
     * @brief find the corresponding instance with fd and delete it
     * @param fd fd returned by open
     * @return success return int::OK, fail return error code less than 0
     */
    virtual int Close(int fd);

    /**
     * @brief destructuring, and recycling resources
     */
    virtual void UnInit();

    /**
     * @brief get cluster id
     * @param buf put the cluster id in this
     * @param len size of the buf
     * @return success return 0, fail return -LIBCURVE_ERROR::FAILED
     */
    int GetClusterId(char* buf, int len);

    /**
     * @brief get cluster id
     * @return success return the cluster id, fail return nullptr
     */
    std::string GetClusterId();

    /**
     * @brief test use, get file info
     * @param fd fd
     * @param[out] finfo file info
     * @return success return 0, fail return -LIBCURVE_ERROR::FAILED
     */
    int GetFileInfo(int fd, FInfo* finfo);

    /**
     * @brief test use, get the number of mounted files
     * @return return the number of mounted files
     */
    uint64_t GetOpenedFileNum() const {
        return openedFileNum_.get_value();
    }

    /**
     * @brief test use, set the mdsclient_
     */
    void SetMdsClient(MDSClient* client) {
        mdsClient_ = client;
    }

    /**
     * @brief test use, set the clientconfig_
     */
    void SetClientConfig(ClientConfig cfg) {
        clientconfig_ = cfg;
    }

    const ClientConfig& GetClientConfig() {
        return clientconfig_;
    }

    /**
     * @brief test use, get the fileserviceMap_
     */
    std::unordered_map<int, FileInstance*>& GetFileServiceMap() {
        return fileserviceMap_;
    }

 private:
    bool StartDummyServer();

    bool CheckAligned(off_t offset, size_t length) {
        return (offset % IO_ALIGNED_BLOCK_SIZE == 0) &&
               (length % IO_ALIGNED_BLOCK_SIZE == 0);
    }

 private:
    BthreadRWLock rwlock_;

    // file descriptor to be returned, for QEMU a vdisk corresponds
    // to a file descriptor
    std::atomic<uint64_t> fdcount_;

    // each vdisk has a FileInstance, which is mapped to the corresponding
    // instance through the returned fd
    std::unordered_map<int, FileInstance*> fileserviceMap_;

    // FileClient config
    ClientConfig clientconfig_;

    // the global mdsclient corresponding to the fileclient
    MDSClient* mdsClient_;

    // init success or not
    bool inited_;

    // the number of mounted files
    bvar::Adder<uint64_t> openedFileNum_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_LIBCURVE_FILE_H_
