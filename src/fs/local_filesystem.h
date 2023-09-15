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
 * File Created: 18-10-31
 * Author: yangyaokai
 */

#ifndef SRC_FS_LOCAL_FILESYSTEM_H_
#define SRC_FS_LOCAL_FILESYSTEM_H_

#include <inttypes.h>
#include <assert.h>
#include <sys/stat.h>
#include <butil/iobuf.h>
#include <memory>
#include <vector>
#include <map>
#include <string>
#include <cstring>
#include <mutex>  // NOLINT

#include "src/fs/fs_common.h"

using std::vector;
using std::map;
using std::string;

namespace curve {
namespace fs {

struct LocalFileSystemOption {
    bool enableRenameat2;
    LocalFileSystemOption() : enableRenameat2(false) {}
};

class LocalFileSystem {
 public:
     LocalFileSystem() {}
    virtual ~LocalFileSystem() {}

    /**
     * Initialize file system
     * If the file system has not been formatted yet, it will be formatted first,
     * Then mount the file system,
     * Formatted or mounted file systems will not be repeatedly formatted or mounted
     * @param option: initialization parameters
     */
    virtual int Init(const LocalFileSystemOption& option) = 0;

    /**
     * Obtain the file system status information where the file or directory is located
     * @param path: The file or directory path under the file system to obtain
     * @param info[out]: File system status information
     * @return Successfully returned 0
     */
    virtual int Statfs(const string& path, struct FileSystemInfo* info) = 0;

    /**
     * Open file handle
     * @param path: File path
     * @param flags: flags for manipulating file methods
     * This flag uses the definition of the POSIX file system
     * @return successfully returns the file handle id, while failure returns a negative value
     */
    virtual int Open(const string& path, int flags) = 0;

    /**
     * Close file handle
     * @param fd: file handle id
     * @return Successfully returned 0
     */
    virtual int Close(int fd) = 0;

    /**
     * Delete files or directories
     * If the deleted object is a directory, the files or subdirectories under the directory will be deleted
     * @param path: The path to a file or directory
     * @return Successful return returns 0
     */
    virtual int Delete(const string& path) = 0;

    /**
     * Create directory
     * @param dirPath: Directory path
     * @return Successfully returned 0
     */
    virtual int Mkdir(const string& dirPath) = 0;

    /**
     * Determine if the directory exists
     * @param dirPath: Directory path
     * @return returns true, otherwise returns false
     */
    virtual bool DirExists(const string& dirPath) = 0;

    /**
     * Determine if the file exists
     * @param dirPath: Directory path
     * @return returns true, otherwise returns false
     */
    virtual bool FileExists(const string& filePath) = 0;

    /**
     * Rename File/Directory
     * Renaming or moving files or directories to a different path will not overwrite existing files
     * @param oldPath: Path to the original file or directory
     * @param newPath: New file or directory path
     * The new file or directory does not exist before renaming, otherwise an error will be returned
     * @param flags: The mode used for renaming, with a default value of 0
     * Optional RENAME_EXCHANGE, RENAME_EXCHANGE, RENAME_WHITEOUT three modes
     * https://manpages.debian.org/testing/manpages-dev/renameat2.2.en.html
     * @return Successfully returned 0
     */
    virtual int Rename(const string& oldPath,
                       const string& newPath,
                       unsigned int flags = 0) {
        return DoRename(oldPath, newPath, flags);
    }

    /**
     * List all files and directory names under the specified path
     * @param dirPath: Directory path
     * @param name[out]: All directories and file names under the directory
     * @return Successfully returned 0
     */
    virtual int List(const string& dirPath, vector<std::string>* names) = 0;

    /**
     * Read data from the specified area of the file
     * @param fd: File handle id, obtained through the Open interface
     * @param buffer: buffer for receiving and reading data
     * @param offset: The starting offset of the read area
     * @param length: The length of the read data
     * @return returns the length of the data successfully read, while failure returns -1
     */
    virtual int Read(int fd, char* buf, uint64_t offset, int length) = 0;

    /**
     * Write data to the specified area of the file
     * @param fd: File handle id, obtained through the Open interface
     * @param buffer: The buffer of the data to be written
     * @param offset: The starting offset of the write area
     * @param length: The length of the written data
     * @return returns the length of successfully written data, while failure returns -1
     */
    virtual int Write(int fd, const char* buf, uint64_t offset, int length) = 0;

    /**
     * Write data to the specified area of the file
     * @param fd: File handle id, obtained through the Open interface
     * @param buf: Data to be written
     * @param offset: The starting offset of the write area
     * @param length: The length of the written data
     * @return returns the length of successfully written data, while failure returns -1
     */
    virtual int Write(int fd, butil::IOBuf buf, uint64_t offset,
                      int length) = 0;

    /**
     * @brief sync one fd
     *
     * @param fd : file descriptor
     *
     * @return succcess return 0, otherwsie reutrn -1
     */
    virtual int Sync(int fd) = 0;

    /**
     * Append data to the end of the file
     * @param fd: File handle id, obtained through the Open interface
     * @param buffer: buffer for data to be added
     * @param length: Append the length of the data
     * @return returns the length of successfully added data, while failure returns -1
     */
    virtual int Append(int fd, const char* buf, int length) = 0;

    /**
     * File pre allocation/excavation (not implemented)
     * @param fd: File handle id, obtained through the Open interface
     * @param op: Specify the type of operation, pre allocation or excavation
     * @param offset: The starting offset of the operating area
     * @param length: The length of the operation area
     * @return Successfully returned 0
     */
    virtual int Fallocate(int fd, int op, uint64_t offset, int length) = 0;

    /**
     * Obtain specified file status information
     * @param fd: File handle id, obtained through the Open interface
     * @param info[out]: Information about the file system
     * The stat structure is the same as the stat used in the POSIX interface
     * @return Successfully returned 0
     */
    virtual int Fstat(int fd, struct stat* info) = 0;

    /**
     * Flush file data and metadata to disk
     * @param fd: File handle id, obtained through the Open interface
     * @return Successfully returned 0
     */
    virtual int Fsync(int fd) = 0;

 private:
    virtual int DoRename(const string& /* oldPath */,
                         const string& /* newPath */,
                         unsigned int /* flags */) { return -1; }
};


class LocalFsFactory {
 public:
    /**
     * Creating File System Objects
     * The factory method of the local file system creates corresponding objects based on the type passed in
     * The file system created by this interface will automatically initialize
     * @param type: File system type
     * @param deviceID: Device number
     * @return returns the local file system object pointer
     */
    static std::shared_ptr<LocalFileSystem> CreateFs(FileSystemType type,
                                                const std::string& deviceID);
};

}  // namespace fs
}  // namespace curve
#endif  // SRC_FS_LOCAL_FILESYSTEM_H_
