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
 *The purpose of the RaftSnapshotFilesystemAdaptor is to take over the braft
 *The logic for creating chunk files through internal snapshots, currently within the curve
 *Will directly retrieve the formatted chunk file from the chunkfilepool
 *However, within braft, install snapshots will also create chunk files
 *This creation file is not chunkfilepool aware, so we want to install it
 *Snapshots can also directly retrieve chunk files from the chunkfilepool, so
 *We have done a layer of hooks on the file system in the install snapshot process, and
 *Simply use the file system interface provided by Curve to create and delete files.
 */

using curve::fs::LocalFileSystem;
using curve::chunkserver::FilePool;

namespace curve {
namespace chunkserver {
/**
 *The CurveFilesystemAdaptor inherits the PosixFileSystemAdaptor class from the raft, where the
 *Internally, its snapshot uses the PosixFileSystemAdaptor class for file operations, as we only want to create files on it
 *Or when deleting files, use the getchunk and recyclechunk interfaces provided by chunkfilepool, so here
 *We only implemented open and delete_file has two interfaces. Other interfaces still use the internal workings of the original raft when called
 *Interface for.
 */
class CurveFilesystemAdaptor : public braft::PosixFileSystemAdaptor {
 public:
    /**
     *Constructor
     * @param: chunkfilepool is used to retrieve and recycle chunk files
     * @param: lfs is used for some file operations, such as opening or deleting directories
     */
    CurveFilesystemAdaptor(std::shared_ptr<FilePool> filePool,
                                  std::shared_ptr<LocalFileSystem> lfs);
    CurveFilesystemAdaptor();
    virtual ~CurveFilesystemAdaptor();

    /**
     *Open the file, use open inside the raft to create a file, and return the FileAdaptor structure
     * @param: path is the current path to be opened
     * @param: oflag is the parameter for opening a file
     * @param: file_meta is the meta information of the current file, which is not used internally
     * @param: e is the error code for opening the file
     * @return: FileAdapter is a class that encapsulates fd internally in raft, where fd is the return value of opening a path
     *All subsequent reads and writes to this file are done through the FileAdaptor pointer, which internally encapsulates the
     *The internal definition of read and write operations is as follows.
     *          class PosixFileAdaptor : public FileAdaptor {
     *              friend class PosixFileSystemAdaptor;
     *           public:
     *             PosixFileAdaptor(int fd) : _fd(fd) {}
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
    virtual braft::FileAdaptor* open(const std::string& path, int oflag,
                              const ::google::protobuf::Message* file_meta,
                              butil::File::Error* e);
    /**
     *Delete the file or directory corresponding to the path
     * @param: path is the file path to be deleted
     * @param: Recursive whether to recursively delete
     * @return: Successfully returns true, otherwise returns false
     */
    virtual bool delete_file(const std::string& path, bool recursive);

    /**
     *Rename to new path
     *Why overload rename?
     *Due to the fact that raft internally uses the rename of the local file system, if the target new path
     *If a file already exists, it will be overwritten. This way, the raft will create temp_snapshot_meta
     *File, this is to ensure atomic modification snapshots_ Set for meta files, and then ensure through rename
     *Modify Atomicity of snapshot_meta file modification. If this temp_snapshot_meta is derived from chunkfilpool
     *If you rename it directly, this temp_snapshot_meta files occupied by meta files
     *You will never receive it back, in which case a large amount of pre allocated chunks will be consumed. Therefore, overloading rename here requires first
     *Recycle the new path and then rename it,
     * @param: old_path Old File Path
     * @param: new_path New File Path
     */
    virtual bool rename(const std::string& old_path,
                       const std::string& new_path);

    //Set which files to filter and do not retrieve them from chunkfilepool
    //Delete these files directly during recycling without entering the chunkfilepool
    void SetFilterList(const std::vector<std::string>& filter);

 private:
   /**
    *Recursive recycling of directory content
    * @param: path is the directory path to be recycled
    * @return: Successfully returns true, otherwise returns false
    */
    bool RecycleDirRecursive(const std::string& path);

    /**
     *Check if the file needs to be filtered
     */
    bool NeedFilter(const std::string& filename);

 private:
    //Due to the need to pass in metapage information when obtaining new chunks in the chunkfile pool
    //Create a temporary metapage here, whose content is irrelevant as the snapshot will overwrite this part of the content
    char*  tempMetaPageContent;
    //Our own file system, where the file system performs some opening and deleting directory operations
    std::shared_ptr<LocalFileSystem> lfs_;
    //Pointer to operate chunkfilepool, this FilePool_ Related to copysetnode
    //Chunkfilepool_ It should be globally unique, ensuring the atomicity of the chunkfilepool operation
    std::shared_ptr<FilePool> chunkFilePool_;
    //Filter the list and do not retrieve file names from chunkfilepool in the current vector
    //Delete these files directly during recycling without entering the chunkfilepool
    std::vector<std::string> filterList_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILESYSTEM_ADAPTOR_H_
