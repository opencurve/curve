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
 * File Created: Tuesday, 25th September 2018 4:58:12 pm
 * Author: tongguangxun
 */
#ifndef SRC_CLIENT_FILE_INSTANCE_H_
#define SRC_CLIENT_FILE_INSTANCE_H_

#include <memory>
#include <string>

#include "include/client/libcurve.h"
#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/iomanager4file.h"
#include "src/client/lease_executor.h"
#include "src/client/mds_client.h"
#include "src/client/service_helper.h"

namespace curve {
namespace client {

class CURVE_CACHELINE_ALIGNMENT FileInstance {
 public:
    FileInstance() = default;
    ~FileInstance() = default;

    /**
     * Initialize
     * @param: filename The filename used to initialize the iomanager's metric
     * information.
     * @param: mdsclient The global mds client.
     * @param: userinfo User information.
     * @param: fileservicopt The configuration options for the fileclient.
     * @param: clientMetric Metric information to be collected on the client
     * side.
     * @param: readonly Whether to open in read-only mode.
     * @return: Returns true on success, otherwise returns false.
     */
    bool Initialize(const std::string& filename,
                    const std::shared_ptr<MDSClient>& mdsclient,
                    const UserInfo& userinfo, const OpenFlags& openflags,
                    const FileServiceOption& fileservicopt,
                    bool readonly = false);
    /**
     * Open File
     * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise
     * LIBCURVE_ERROR::FAILED
     */
    int Open(std::string* sessionId = nullptr);

    /**
     * Synchronous mode read
     * @param: buf    The current buffer to be read
     * @param: offset The offset within the file
     * @param: length The length to be read
     * @return: Success returns the true length of the write, -1 indicates
     * failure
     */
    int Read(char* buf, off_t offset, size_t length);
    /**
     * Synchronous mode write
     * @param: buf    The current buffer to be written
     * @param: offset The offset within the file
     * @parma: length The length to be read
     * @return: Success returns the true length of the write, -1 indicates
     * failure
     */
    int Write(const char* buf, off_t offset, size_t length);
    /**
     * Asynchronous mode read.
     * @param: aioctx The I/O context for asynchronous read/write, which holds
     * basic I/O information
     * @param: dataType type of user buffer
     * @return: 0 on success, less than 0 on failure
     */
    int AioRead(CurveAioContext* aioctx, UserDataType dataType);
    /**
     * Asynchronous mode write.
     * @param: aioctx An asynchronous read/write IO context that stores basic IO
     * information
     * @param: dataType type of user buffer
     * @return: 0 indicates success, less than 0 indicates failure
     */
    int AioWrite(CurveAioContext* aioctx, UserDataType dataType);

    /**
     * @param offset discard offset
     * @param length discard length
     * @return On success, returns 0.
     *         On error, returns a negative value.
     */
    int Discard(off_t offset, size_t length);

    /**
     * @brief Asynchronous discard operation
     * @param aioctx async request context
     * @return 0 means success, otherwise it means failure
     */
    int AioDiscard(CurveAioContext* aioctx);

    int Close();

    void UnInitialize();

    IOManager4File* GetIOManager4File() { return &iomanager4file_; }

    /**
     * Obtain a release to test code usage
     */
    LeaseExecutor* GetLeaseExecutor() const { return leaseExecutor_.get(); }

    int GetFileInfo(const std::string& filename, FInfo_t* fi,
                    FileEpoch_t* fEpoch);

    void UpdateFileEpoch(const FileEpoch_t& fEpoch) {
        iomanager4file_.UpdateFileEpoch(fEpoch);
    }

    /**
     * @brief Get the file information corresponding to the current instance
     *
     * @return The information of the file corresponding to the current instance
     */
    FInfo GetCurrentFileInfo() const { return finfo_; }

    static FileInstance* NewInitedFileInstance(
        const FileServiceOption& fileServiceOption,
        const std::shared_ptr<MDSClient>& mdsclient,
        const std::string& filename, const UserInfo& userInfo,
        const OpenFlags& openflags, bool readonly);

    static FileInstance* Open4Readonly(
        const FileServiceOption& opt,
        const std::shared_ptr<MDSClient>& mdsclient,
        const std::string& filename, const UserInfo& userInfo,
        const OpenFlags& openflags = DefaultReadonlyOpenFlags());

 private:
    void StopLease();

 private:
    // Save file information for the current file
    FInfo finfo_;

    // The initialization configuration information of the current FileInstance
    FileServiceOption fileopt_;

    // MDSClient is the only exit for FileInstance to communicate with mds
    std::shared_ptr<MDSClient> mdsclient_;

    // Each file holds a lease for communication with MDS, and the LeaseExecutor
    // is the renewal executor
    std::unique_ptr<LeaseExecutor> leaseExecutor_;

    // IOManager4File is used to manage all IO sent to the chunkserver end
    IOManager4File iomanager4file_;

    // Whether to open in read-only mode
    bool readonly_ = false;

    // offset and length must align with `blocksize_`
    // 4096 for backward compatibility
    size_t blocksize_ = 4096;
};

bool CheckAlign(off_t off, size_t length, size_t blocksize);

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_FILE_INSTANCE_H_
