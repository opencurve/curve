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
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 */

#ifndef NEBD_SRC_PART2_FILE_MANAGER_H_
#define NEBD_SRC_PART2_FILE_MANAGER_H_

#include <brpc/closure_guard.h>
#include <limits.h>
#include <memory>
#include <sstream>
#include <string>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "nebd/src/common/rw_lock.h"
#include "nebd/src/common/name_lock.h"
#include "nebd/src/part2/define.h"
#include "nebd/src/part2/util.h"
#include "nebd/src/part2/file_entity.h"
#include "nebd/src/part2/metafile_manager.h"
#include "nebd/proto/client.pb.h"

namespace nebd {
namespace server {

using nebd::common::NameLock;
using nebd::common::NameLockGuard;
using nebd::common::WriteLockGuard;
using nebd::common::ReadLockGuard;
using OpenFlags = nebd::client::ProtoOpenFlags;

using FileEntityMap = std::unordered_map<int, NebdFileEntityPtr>;
class NebdFileManager {
 public:
    explicit NebdFileManager(MetaFileManagerPtr metaFileManager);
    virtual ~NebdFileManager();
    /**
     * Stop FileManager and release FileManager resources
     * @return returns 0 for success, -1 for failure
     */
    virtual int Fini();
    /**
     * Start FileManager
     * @return returns 0 for success, -1 for failure
     */
    virtual int Run();
    /**
     * Open File
     * @param filename: The filename of the file
     * @return successfully returns fd, failure returns -1
     */
    virtual int Open(const std::string& filename, const OpenFlags* flags);
    /**
     * Close File
     * @param fd: fd of the file
     * @param removeRecord: Do you want to remove the file record? True means remove, false means not remove
     * If it is a close request passed from part1, this parameter is true
     * If it is a close request initiated by the heartbeat manager, this parameter is false
     * @return returns 0 for success, -1 for failure
     */
    virtual int Close(int fd, bool removeRecord);
    /**
     * Expand file capacity
     * @param fd: fd of the file
     * @param newsize: New file size
     * @return returns 0 for success, -1 for failure
     */
    virtual int Extend(int fd, int64_t newsize);
    /**
     * Obtain file information
     * @param fd: fd of the file
     * @param fileInfo[out]: File information
     * @return returns 0 for success, -1 for failure
     */
    virtual int GetInfo(int fd, NebdFileInfo* fileInfo);
    /**
     * Asynchronous request to reclaim the specified area space
     * @param fd: fd of the file
     * @param aioctx: Asynchronous request context
     * @return returns 0 for success, -1 for failure
     */
    virtual int Discard(int fd, NebdServerAioContext* aioctx);
    /**
     * Asynchronous request to read the content of the specified area
     * @param fd: fd of the file
     * @param aioctx: Asynchronous request context
     * @return returns 0 for success, -1 for failure
     */
    virtual int AioRead(int fd, NebdServerAioContext* aioctx);
    /**
     * Asynchronous request, writing data to a specified area
     * @param fd: fd of the file
     * @param aioctx: Asynchronous request context
     * @return returns 0 for success, -1 for failure
     */
    virtual int AioWrite(int fd, NebdServerAioContext* aioctx);
    /**
     * Asynchronous requests, flush file caching
     * @param fd: fd of the file
     * @param aioctx: Asynchronous request context
     * @return returns 0 for success, -1 for failure
     */
    virtual int Flush(int fd, NebdServerAioContext* aioctx);
    /**
     * Invalidate the specified file cache
     * @param fd: fd of the file
     * @return returns 0 for success, -1 for failure
     */
    virtual int InvalidCache(int fd);

    // Obtain the specified entity from the map based on fd
    // If entity already exists, return entity pointer; otherwise, return nullptr
    virtual NebdFileEntityPtr GetFileEntity(int fd);

    virtual FileEntityMap GetFileEntityMap();

    // Output all file states to a string
    std::string DumpAllFileStatus();

    // set public for test
    // Load file records from metafile at startup and reopen the file
    int Load();

 private:
     // Assign new available fds, fds are not allowed to duplicate existing ones
     // Successfully returned available fd, failed returned -1
    int GenerateValidFd();
    // Obtain file entity based on file name
    // If entity exists, directly return the entity pointer
    // If the entity does not exist, create a new entity, insert a map, and then return
    NebdFileEntityPtr GetOrCreateFileEntity(const std::string& fileName);
    // Generate file entity based on fd and file name,
    // If fd already exists for entity, directly return the entity pointer
    // If the entity does not exist, generate a new entity, insert a map, and then return
    NebdFileEntityPtr GenerateFileEntity(int fd, const std::string& fileName);
    //Delete the entity corresponding to the specified fd
    void RemoveEntity(int fd);

 private:
    // The current running status of the filemanager, where true indicates running and false indicates not running
    std::atomic<bool> isRunning_;
    // File name lock, lock files with the same name
    NameLock nameLock_;
    // Fd distributor
    FdAllocator fdAlloc_;
    // nebd server file record management
    MetaFileManagerPtr metaFileManager_;
    // file map read write protection lock
    RWLock rwLock_;
    // Mapping of file fd and file entities
    FileEntityMap fileMap_;
};
using NebdFileManagerPtr = std::shared_ptr<NebdFileManager>;

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_FILE_MANAGER_H_
