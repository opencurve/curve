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
 * RaftSnapshotFilesystemAdaptor is aimed at taking over braft's snapshot logic
 * for creating chunk files. Currently, curve fetches the formatted chunk files
 * directly from chunkfilepool. But due to install snapshot, braft also creates
 * chunk files internally that can't sense chunkfilepool，We therefore want
 * install snapshot to be able to fetch chunk files directly from chunkfilepool,
 * so we make a layer of hook in the file system during install snapshot process
 * and you can just create or delete files with the file system interface
 * provided by curve.
*/

using curve::fs::LocalFileSystem;
using curve::chunkserver::FilePool;

namespace curve {
namespace chunkserver {
    /**
     * CurveFilesystemAdaptor inherits from raft's PosixFileSystemAdaptor class,
     * which is used by the file operations of raft's snapshot.Since we only
     * want to use getchunk and recyclechunk interfaces provided by
     * chunkfilepool when creating or deleting files, we have only implemented
     * the open and delete_file interfaces here. The other interfaces still use
     * the original raft's internal interfaces when they are called.
    */
class CurveFilesystemAdaptor : public braft::PosixFileSystemAdaptor {
 public:
    /**
     * constructor
     * @param: chunkfilepool Get and recycle chunk files
     * @param: lfs Do some file operations，such as opening and deleting
     *         directories
     */
    CurveFilesystemAdaptor(std::shared_ptr<FilePool> filePool,
                                  std::shared_ptr<LocalFileSystem> lfs);
    CurveFilesystemAdaptor();
    virtual ~CurveFilesystemAdaptor();

    /**
     * use open to create a new file in raft，and return the structure of
     * FileAdaptor
     * @param: path The path of the file to be opened
     * @param: oflag The params of opening files
     * @param: file_meta The meta info of the current file，which has not been
     *         used
     * @param: e The error code when opening files
     * @return: FileAdaptor is a class that encapsulates fd inside raft，fd is
     *          the return value after opening the path.All following reads and
     *          writes to the file are done through the FileAdaptor pointer,
     *          which encapsulates the read and write operations.
     *          The definitions are as follows:
     *
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
     * delete the file or directory corresponding to the path
     * @param: path The path of the file to be deleted
     * @param: recursive Whether to delete recursively
     * @return: return true if succeeded, otherwise false
     */
    virtual bool delete_file(const std::string& path, bool recursive);

    /**
     * rename to the new path
     * why we reload rename？
     * Since raft internally uses rename on the local file system, if the
     * target new path already has a file, then it will overwrite that file.
     * This creates a temp_snapshot_meta file in raft, which is set up to ensure
     * atomic modification of the snapshot_meta file, and then rename
     * temp_snapshot_meta file to finally ensure atomic modification of the
     * snapshot_meta file. If the temp_snapshot_meta is taken from the
     * chunkfilpool, and renamed directly, the chunk file occupied by the
     * temp_snapshot_meta file will never be recycled, and many pre-allocated
     * chunks will be consumed in this case. So we firstly reload rename,
     * and then recycle new path.
     * @param: old_path Old file path
     * @param: new_path New file path
     */
    virtual bool rename(const std::string& old_path,
                       const std::string& new_path);

    // set files to be filtered，which are not fetched from chunkfilepool
    // delete these files directly when recycling，not into chunkfilepool
    void SetFilterList(const std::vector<std::string>& filter);

 private:
   /**
    * recycle the contents directory recursively
    * @param: path The path of the directory to be recycled
    * @return: return true if succeeded, otherwise false
    */
    bool RecycleDirRecursive(const std::string& path);

    /**
     * check whether files need to be filtered
     */
    bool NeedFilter(const std::string& filename);

 private:
    // Since chunkfile pool needs metapage info when fetching a new chunk,
    // so we create a temporary metapage here whose contents are unimportant
    // as the snapshot will overwrite this part
    char*  tempMetaPageContent;
    // Our own file system will do some open and delete operations here
    std::shared_ptr<LocalFileSystem> lfs_;
    // Pointer to operate chunkfilepool，both FilePool_ here and copysetnode's
    // chunkfilepool_ are unique globally，to make sure the atomicity when
    // operating chunkfilepool
    std::shared_ptr<FilePool> chunkFilePool_;
    // Filter list, none of the file names in the current vector are fetched
    // from the chunkfilepool
    // Delete these files directly when recycling，not into chunkfilepool
    std::vector<std::string> filterList_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILESYSTEM_ADAPTOR_H_
