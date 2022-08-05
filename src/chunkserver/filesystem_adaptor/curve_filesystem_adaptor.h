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
#ifndef SRC_CHUNKSERVER_FILESYSTEM_ADAPTOR_CURVE_FILESYSTEM_ADAPTOR_H_
#define SRC_CHUNKSERVER_FILESYSTEM_ADAPTOR_CURVE_FILESYSTEM_ADAPTOR_H_

#include <braft/file_system_adaptor.h>
#include <google/protobuf/message.h>

#include <memory>
#include <string>
#include <vector>

#include "src/chunkserver/datastore/file_pool.h"
#include "src/chunkserver/filesystem_adaptor/curve_file_adaptor.h"

using curve::fs::LocalFileSystem;
using curve::chunkserver::FilePool;

using butil::FilePath;
using butil::File;

namespace curve {
namespace chunkserver {

/**
 * CurveFilesystemAdaptor inherits braft::FileSystemAdaptor
 * and is implemented using the interface of filePool and LocalFileSystem.
 */
class CurveFilesystemAdaptor : public braft::FileSystemAdaptor {
 public:
    /**
     * 构造函数
     * @param: chunkfilepool用于获取和回收chunk文件
     * @param: lfs用于进行一些文件操作，比如打开或者删除目录
     */
    CurveFilesystemAdaptor(std::shared_ptr<FilePool> filePool,
                           std::shared_ptr<LocalFileSystem> lfs);
    CurveFilesystemAdaptor();
    virtual ~CurveFilesystemAdaptor();

    /**
     * 打开文件，在raft内部使用open来创建一个文件，并返回FileAdaptor结构
     * @param: path是当前待打开的路径
     * @param: oflag为打开文件参数
     * @param: file_meta是当前文件的meta信息，这个参数内部未使用
     * @param: e为打开文件是的错误码
     * @return: FileAdaptor是raft内部封装fd的一个类，fd是open打开path的返回值
     *          后续所有对于该文件的读写都是通过该FileAdaptor指针进行的，其内部封装了
     *          读写操作，其内部定义如下。
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
    braft::FileAdaptor* open(const std::string& path, int oflag,
                              const ::google::protobuf::Message* file_meta,
                              butil::File::Error* e) override;
    /**
     * 删除path对应的文件或目录
     * @param: path是待删除的文件路径
     * @param: recursive是否递归删除
     * @return: 成功返回true，否则返回false
     */
    bool delete_file(const std::string& path, bool recursive) override;

    /**
     * rename到新路径
     * 为什么要重载rename？
     * 由于raft内部使用的是本地文件系统的rename，如果目标new path
     * 已经存在文件，那么就会覆盖该文件。这样raft内部会创建temp_snapshot_meta
     * 文件，这个是为了保证原子修改snapshot_meta文件而设置的，然后通过rename保证
     * 修改snapshot_meta文件修改的原子性。如果这个temp_snapshot_meta是从chunkfilpool
     * 取的，那么如果直接rename，这个temp_snapshot_meta文件所占用的chunk文件
     * 就永远收不回来了，这种情况下会消耗大量的预分配chunk，所以这里重载rename，先
     * 回收new path，然后再rename,
     * @param: old_path旧文件路径
     * @param: new_path新文件路径
     */
    bool rename(const std::string& old_path,
                       const std::string& new_path) override;

    bool link(const std::string& old_path,
              const std::string& new_path) override;

    bool create_directory(const std::string& path,
                                  butil::File::Error* error,
                                  bool create_parent_directories) override;

    bool path_exists(const std::string& path) override;

    bool directory_exists(const std::string& path) override;

    braft::DirReader* directory_reader(const std::string& path) override;

    bool CreateDirectoryAndGetError(
        const FilePath& full_path,
        File::Error* error,
        bool create_parents);

    // 设置过滤哪些文件，这些文件不从chunkfilepool取
    // 回收的时候也直接删除这些文件，不进入chunkfilepool
    void SetFilterList(const std::vector<std::string>& filter);

    std::shared_ptr<FilePool> GetFilePool() const {
        return filePool_;
    }

    std::shared_ptr<LocalFileSystem> GetLocalFileSystem() const {
        return lfs_;
    }

 private:
   /**
    * 递归回收目录内容
    * @param: path为待回收的目录路径
    * @return: 成功返回true，否则返回false
    */
    bool RecycleDirRecursive(const std::string& path);

    /**
     * 查看文件是否需要过滤
     */
    bool NeedFilter(const std::string& filename);

 private:
    // 由于chunkfile pool获取新的chunk时需要传入metapage信息
    // 这里创建一个临时的metapage，其内容无关紧要，因为快照会覆盖这部分内容
    char*  tempMetaPageContent;
    // 我们自己的文件系统，这里文件系统会做一些打开及删除目录操作
    std::shared_ptr<LocalFileSystem> lfs_;
    // 操作chunkfilepool的指针，这个FilePool_与copysetnode的
    // chunkfilepool_应该是全局唯一的，保证操作chunkfilepool的原子性
    std::shared_ptr<FilePool> filePool_;
    // 过滤名单，在当前vector中的文件名，都不从chunkfilepool中取文件
    // 回收的时候也直接删除这些文件，不进入chunkfilepool
    std::vector<std::string> filterList_;
};

class CurveDirReader : public braft::DirReader {
    friend class CurveFilesystemAdaptor;
 public:
    virtual ~CurveDirReader();

    // Check if the dir reader is valid
    virtual bool is_valid() const;

    // Move to next entry in the directory
    // Return true if a entry can be found, false otherwise
    virtual bool next();

    // Get the name of current entry
    virtual const char* name() const;

 protected:
    CurveDirReader(const std::string& path,
        const std::shared_ptr<LocalFileSystem> &lfs);

 private:
    std::string path_;
    std::shared_ptr<LocalFileSystem> lfs_;
    DIR *dir_;
    struct dirent *dirIter_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_FILESYSTEM_ADAPTOR_CURVE_FILESYSTEM_ADAPTOR_H_
