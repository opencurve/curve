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
 * Project: nebd
 * Created Date: Tuesday March 3rd 2020
 * Author: yangyaokai
 */

#ifndef NEBD_SRC_PART2_FILE_ENTITY_H_
#define NEBD_SRC_PART2_FILE_ENTITY_H_

#include <brpc/closure_guard.h>
#include <limits.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>

#include "nebd/proto/client.pb.h"
#include "nebd/src/common/rw_lock.h"
#include "nebd/src/common/timeutility.h"
#include "nebd/src/part2/define.h"
#include "nebd/src/part2/metafile_manager.h"
#include "nebd/src/part2/request_executor.h"
#include "nebd/src/part2/util.h"

namespace nebd {
namespace server {

using nebd::common::BthreadRWLock;
using nebd::common::ReadLockGuard;
using nebd::common::TimeUtility;
using nebd::common::WriteLockGuard;
using OpenFlags = nebd::client::ProtoOpenFlags;

class NebdFileInstance;
class NebdRequestExecutor;
using NebdFileInstancePtr = std::shared_ptr<NebdFileInstance>;

// When processing user requests, it is necessary to add a read write lock to
// avoid user IO still not being processed when closing For asynchronous IO, the
// read lock can only be released on return, so it is encapsulated as a Closure
// Assign the closure value to NebdServerAioContext before sending an
// asynchronous request
class NebdRequestReadLockClosure : public Closure {
 public:
    explicit NebdRequestReadLockClosure(BthreadRWLock& rwLock)  // NOLINT
        : rwLock_(rwLock), done_(nullptr) {
        rwLock_.RDLock();
    }
    ~NebdRequestReadLockClosure() {}

    void Run() {
        std::unique_ptr<NebdRequestReadLockClosure> selfGuard(this);
        brpc::ClosureGuard doneGuard(done_);
        rwLock_.Unlock();
    }

    void SetClosure(Closure* done) { done_ = done; }

    Closure* GetClosure() { return done_; }

 private:
    BthreadRWLock& rwLock_;
    Closure* done_;
};

struct NebdFileEntityOption {
    int fd;
    std::string fileName;
    MetaFileManagerPtr metaFileManager_;
};

class NebdFileEntity : public std::enable_shared_from_this<NebdFileEntity> {
 public:
    NebdFileEntity();
    virtual ~NebdFileEntity();

    /**
     * Initialize File Entity
     * @param option: Initialize parameters
     * @return returns 0 for success, -1 for failure
     */
    virtual int Init(const NebdFileEntityOption& option);
    /**
     * Open File
     * @return successfully returns fd, failure returns -1
     */
    virtual int Open(const OpenFlags* openflags);
    /**
     * Reopen the file and reuse the previous backend storage connection if it
     * still exists Otherwise, establish a new connection with the backend
     * storage
     * @param xattr: Information required for file reopening
     * @return successfully returns fd, failure returns -1
     */
    virtual int Reopen(const ExtendAttribute& xattr);
    /**
     *Close File
     * @param removeMeta: Do you want to remove the file metadata record? True
     *means remove, false means not remove If it is a close request passed from
     *part1, this parameter is true If it is a close request initiated by the
     *heartbeat manager, this parameter is false
     * @return returns 0 for success, -1 for failure
     */
    virtual int Close(bool removeMeta);
    /**
     * Expand file capacity
     * @param newsize: New file size
     * @return returns 0 for success, -1 for failure
     */
    virtual int Extend(int64_t newsize);
    /**
     * Obtain file information
     * @param fileInfo[out]: File information
     * @return returns 0 for success, -1 for failure
     */
    virtual int GetInfo(NebdFileInfo* fileInfo);
    /**
     * Asynchronous request to reclaim the specified area space
     * @param aioctx: Asynchronous request context
     * @return returns 0 for success, -1 for failure
     */
    virtual int Discard(NebdServerAioContext* aioctx);
    /**
     * Asynchronous request to read the content of the specified area
     * @param aioctx: Asynchronous request context
     * @return returns 0 for success, -1 for failure
     */
    virtual int AioRead(NebdServerAioContext* aioctx);
    /**
     * Asynchronous request, writing data to a specified area
     * @param aioctx: Asynchronous request context
     * @return returns 0 for success, -1 for failure
     */
    virtual int AioWrite(NebdServerAioContext* aioctx);
    /**
     * Asynchronous requests, flush file caching
     * @param aioctx: Asynchronous request context
     * @return returns 0 for success, -1 for failure
     */
    virtual int Flush(NebdServerAioContext* aioctx);
    /**
     * Invalidate the specified file cache
     * @return returns 0 for success, -1 for failure
     */
    virtual int InvalidCache();

    virtual std::string GetFileName() const { return fileName_; }

    virtual int GetFd() const { return fd_; }

    virtual void UpdateFileTimeStamp(uint64_t timestamp) {
        timeStamp_.store(timestamp);
    }

    virtual uint64_t GetFileTimeStamp() const { return timeStamp_.load(); }

    virtual NebdFileStatus GetFileStatus() const { return status_.load(); }

 private:
    /**
     * Update file status, including meta information files and memory status
     * @param fileInstancea: The file context information returned by open or
     * reopen
     * @return: Success returns 0, failure returns -1
     */
    int UpdateFileStatus(NebdFileInstancePtr fileInstance);
    /**
     * Request Unified Processing Function
     * @param task: The actual request to execute the function body
     * @return: Success returns 0, failure returns -1
     */
    using ProcessTask = std::function<int(void)>;
    int ProcessSyncRequest(ProcessTask task);
    int ProcessAsyncRequest(ProcessTask task, NebdServerAioContext* aioctx);

    // Ensure that the file is in an open state, and if not, attempt to open it
    // Unable to open or failed to open, returns false,
    // If the file is in the open state, return true
    bool GuaranteeFileOpened();

 private:
    // File read/write lock, apply read lock before processing requests, and
    // apply write lock when closing files Avoiding pending requests during
    // close
    BthreadRWLock rwLock_;
    // Mutex lock, used for mutual exclusion between open and close
    bthread::Mutex fileStatusMtx_;
    // The unique identifier assigned by the nebd server to this file
    int fd_;
    // File Name
    std::string fileName_;
    std::unique_ptr<OpenFlags> openFlags_;
    // The current state of the file, where 'opened' indicates that the file is
    // open and 'closed' indicates that the file is closed
    std::atomic<NebdFileStatus> status_;
    // The timestamp of the last time the file received a heartbeat
    std::atomic<uint64_t> timeStamp_;
    // When the file is opened by the executor, contextual information is
    // returned for subsequent file request processing
    NebdFileInstancePtr fileInstance_;
    // Pointer to the executor corresponding to the file
    NebdRequestExecutor* executor_;
    // Metadata Persistence Management
    MetaFileManagerPtr metaFileManager_;
};
using NebdFileEntityPtr = std::shared_ptr<NebdFileEntity>;
std::ostream& operator<<(std::ostream& os, const NebdFileEntity& entity);

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_FILE_ENTITY_H_
