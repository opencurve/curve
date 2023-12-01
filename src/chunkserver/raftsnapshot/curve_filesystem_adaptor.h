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
 * File Created: Monday, 10th June 2019 2:20:12 pm
 * Author: tongguangxun
 */
#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILESYSTEM_ADAPTOR_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILESYSTEM_ADAPTOR_H_

#include <braft/file_system_adaptor.h>
#include <google/protobuf/message.h>

#include <memory>
#include <string>
#include <vector>

#include "src/chunkserver/datastore/file_pool.h"
#include "src/chunkserver/raftsnapshot/curve_file_adaptor.h"

/**
 * The purpose of RaftSnapshotFilesystemAdaptor is to take over the logic of
 * creating chunk files for internal snapshots in braft. Currently, within
 * Curve, we directly retrieve pre-formatted chunk files from the chunk file
 * pool. However, within braft, the creation of chunk files during an install
 * snapshot process does not interact with the chunk file pool. Therefore, we
 * want the install snapshot process to also be able to retrieve chunk files
 * directly from the chunk file pool. To achieve this, we have implemented a
 * hook in the file system operations within the install snapshot process. This
 * hook allows us to use the file system interface provided by Curve for file
 * creation and deletion.
 */

using curve::chunkserver::FilePool;
using curve::fs::LocalFileSystem;

namespace curve {
namespace chunkserver {
/**
 * CurveFilesystemAdaptor inherits from Raft's PosixFileSystemAdaptor class.
 * Within the Raft framework, it uses the PosixFileSystemAdaptor class for file
 * operations during snapshots. However, we only want to use the `getchunk` and
 * `recyclechunk` interfaces provided by the chunkfilepool when creating or
 * deleting files. Therefore, in this context, we have only implemented the
 * `open` and `delete_file` interfaces. Other interfaces are still used with the
 * original internal Raft interfaces when called.
 */
class CurveFilesystemAdaptor : public braft::PosixFileSystemAdaptor {
 public:
    /**
     * Constructor
     * @param: chunkfilepool is used to retrieve and recycle chunk files
     * @param: lfs is used for some file operations, such as opening or deleting
     * directories
     */
    CurveFilesystemAdaptor(std::shared_ptr<FilePool> filePool,
                           std::shared_ptr<LocalFileSystem> lfs);
    CurveFilesystemAdaptor();
    virtual ~CurveFilesystemAdaptor();

    /**
     * Open the file, use open inside the raft to create a file, and return the
     * FileAdaptor structure
     * @param: path is the current path to be opened
     * @param: oflag is the parameter for opening a file
     * @param: file_meta is the meta information of the current file, which is
     * not used internally
     * @param: e is the error code for opening the file
     * @return: FileAdaptor is a class within Raft that encapsulates a file
     * descriptor (fd). After opening a path with the `open` call, all
     * subsequent read and write operations on that file are performed through a
     * pointer to this FileAdaptor class. It internally defines the following
     * operations: class PosixFileAdaptor : public FileAdaptor { friend class
     * PosixFileSystemAdaptor; public: PosixFileAdaptor(int fd) : _fd(fd) {}
     *             virtual ~PosixFileAdaptor();
     *
     *             virtual ssize_t write(const butil::IOBuf& data,
     *                                  off_t offset);
     *             virtual ssize_t read(butil::IOPortal* portal,
     *                                  off_t offset, size_t size);
     *             virtual ssize_t size();
     *             virtual bool sync();
     *             virtual bool close();
     *
     *           private:
     *             int _fd;
     *          };
     */
    virtual braft::FileAdaptor* open(
        const std::string& path, int oflag,
        const ::google::protobuf::Message* file_meta, butil::File::Error* e);
    /**
     * Delete the file or directory corresponding to the path
     * @param: path is the file path to be deleted
     * @param: Recursive whether to recursively delete
     * @return: Successfully returns true, otherwise returns false
     */
    virtual bool delete_file(const std::string& path, bool recursive);

    /**
     * Rename to a new path.
     * Why override the rename function?
     * Raft internally uses the rename function of the local file system. If the
     * target new path already exists as a file, it will overwrite that file.
     * This behavior leads to the creation of a 'temp_snapshot_meta' file, which
     * is set up to ensure the atomic modification of the 'snapshot_meta' file.
     * Using rename helps ensure the atomicity of modifying the 'snapshot_meta'
     * file. However, if the 'temp_snapshot_meta' file is allocated from the
     * chunk file pool and renamed directly, the chunk file used by the
     * 'temp_snapshot_meta' file will never be released. In this situation, a
     * significant number of pre-allocated chunks can be consumed. Therefore,
     * the rename function is overridden here to first release the resources
     * associated with the new path, and then perform the rename operation.
     * @param: old_path - The old file path
     * @param: new_path - The new file path
     */
    virtual bool rename(const std::string& old_path,
                        const std::string& new_path);

    // Set which files to filter and do not retrieve them from chunkfilepool
    // Delete these files directly during recycling without entering the
    // chunkfilepool
    void SetFilterList(const std::vector<std::string>& filter);

 private:
    /**
     * Recursive recycling of directory content
     * @param: path is the directory path to be recycled
     * @return: Successfully returns true, otherwise returns false
     */
    bool RecycleDirRecursive(const std::string& path);

    /**
     * Check if the file needs to be filtered
     */
    bool NeedFilter(const std::string& filename);

 private:
    // Due to the need to pass in metapage information when obtaining new chunks
    // in the chunkfile pool Create a temporary metapage here, whose content is
    // irrelevant as the snapshot will overwrite this part of the content
    char* tempMetaPageContent;
    // Our own file system, where the file system performs some opening and
    // deleting directory operations
    std::shared_ptr<LocalFileSystem> lfs_;
    // Pointer to operate chunkfilepool, this FilePool_ Related to copysetnode
    // Chunkfilepool_ It should be globally unique, ensuring the atomicity of
    // the chunkfilepool operation
    std::shared_ptr<FilePool> chunkFilePool_;
    // Filter the list and do not retrieve file names from chunkfilepool in the
    // current vector Delete these files directly during recycling without
    // entering the chunkfilepool
    std::vector<std::string> filterList_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILESYSTEM_ADAPTOR_H_
