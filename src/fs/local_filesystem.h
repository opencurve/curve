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
     * 初始化文件系统
     * 如果文件系统还未格式化，首先会格式化，
     * 然后挂载文件系统，
     * 已经格式化或者已经挂载的文件系统不会重复格式化或挂载
     * @param option：初始化参数
     */
    virtual int Init(const LocalFileSystemOption& option) = 0;

    /**
     * 获取文件或目录所在的文件系统状态信息
     * @param path: 要获取的文件系统下的文件或目录路径
     * @param info[out]： 文件系统状态信息
     * @return 成功返回0
     */
    virtual int Statfs(const string& path, struct FileSystemInfo* info) = 0;

    /**
     * 打开文件句柄
     * @param path：文件路径
     * @param flags：操作文件方式的flag
     * 此flag使用POSIX文件系统的定义
     * @return 成功返回文件句柄id，失败返回负值
     */
    virtual int Open(const string& path, int flags) = 0;

    /**
     * 关闭文件句柄
     * @param fd: 文件句柄id
     * @return 成功返回0
     */
    virtual int Close(int fd) = 0;

    /**
     * 删除文件或目录
     * 如果删除对象为目录，会删除目录下的文件或子目录
     * @param path：文件或目录的路径
     * return 成功返回0
     */
    virtual int Delete(const string& path) = 0;

    /**
     * 创建目录
     * @param dirPath: 目录路径
     * @return 成功返回0
     */
    virtual int Mkdir(const string& dirPath) = 0;

    /**
     * 判断目录是否存在
     * @param dirPath：目录路径
     * @return 存在返回true，否则返回false
     */
    virtual bool DirExists(const string& dirPath) = 0;

    /**
     * 判断文件是否存在
     * @param dirPath：目录路径
     * @return 存在返回true，否则返回false
     */
    virtual bool FileExists(const string& filePath) = 0;

    /**
     * 重命名文件/目录
     * 将文件或目录重命名或者移到其他路径,不会覆盖已存在的文件
     * @param oldPath：原文件或目录路径
     * @param newPath：新的文件或目录路径
     * 新的文件或目录在重命名之前不存在，否则返回错误
     * @param flags:重命名使用的模式，默认值为0
     * 可选择RENAME_EXCHANGE、RENAME_EXCHANGE、RENAME_WHITEOUT三种模式
     * https://manpages.debian.org/testing/manpages-dev/renameat2.2.en.html
     * @return 成功返回0
     */
    virtual int Rename(const string& oldPath,
                       const string& newPath,
                       unsigned int flags = 0) {
        return DoRename(oldPath, newPath, flags);
    }

    /**
     * 列举指定路径下的所有文件和目录名
     * @param dirPath：目录路径
     * @param name[out]：目录下的所有目录和文件名
     * @return 成功返回0
     */
    virtual int List(const string& dirPath, vector<std::string>* names) = 0;

    /**
     * 从文件指定区域读取数据
     * @param fd：文件句柄id，通过Open接口获取
     * @param buf：接收读取数据的buffer
     * @param offset：读取区域的起始偏移
     * @param length：读取数据的长度
     * @return 返回成功读取到的数据长度，失败返回-1
     */
    virtual int Read(int fd, char* buf, uint64_t offset, int length) = 0;

    /**
     * 向文件指定区域写入数据
     * @param fd：文件句柄id，通过Open接口获取
     * @param buf：待写入数据的buffer
     * @param offset：写入区域的起始偏移
     * @param length：写入数据的长度
     * @return 返回成功写入的数据长度，失败返回-1
     */
    virtual int Write(int fd, const char* buf, uint64_t offset, int length) = 0;

    /**
     * 向文件指定区域写入数据
     * @param fd：文件句柄id，通过Open接口获取
     * @param buf：待写入数据
     * @param offset：写入区域的起始偏移
     * @param length：写入数据的长度
     * @return 返回成功写入的数据长度，失败返回-1
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
     * 向文件末尾追加数据
     * @param fd：文件句柄id，通过Open接口获取
     * @param buf：待追加数据的buffer
     * @param length：追加数据的长度
     * @return 返回成功追加的数据长度，失败返回-1
     */
    virtual int Append(int fd, const char* buf, int length) = 0;

    /**
     * 文件预分配/挖洞（未实现）
     * @param fd：文件句柄id，通过Open接口获取
     * @param op：指定操作类型，预分配还是挖洞
     * @param offset：操作区域的起始偏移
     * @param length：操作区域的长度
     * @return 成功返回0
     */
    virtual int Fallocate(int fd, int op, uint64_t offset, int length) = 0;

    /**
     * 获取指定文件状态信息
     * @param fd：文件句柄id，通过Open接口获取
     * @param info[out]：文件系统的信息
     * stat结构同POSIX接口中使用的stat
     * @return 成功返回0
     */
    virtual int Fstat(int fd, struct stat* info) = 0;

    /**
     * 将文件数据和元数据刷新到磁盘
     * @param fd：文件句柄id，通过Open接口获取
     * @return 成功返回0
     */
    virtual int Fsync(int fd) = 0;

 private:
    virtual int DoRename(const string& oldPath,
                         const string& newPath,
                         unsigned int flags) { return -1; }
};


class LocalFsFactory {
 public:
    /**
     * 创建文件系统对象
     * 本地文件系统的工厂方法，根据传入的类型，创建相应的对象
     * 由该接口创建的文件系统会自动进行初始化
     * @param type：文件系统类型
     * @param deviceID: 设备的编号
     * @return 返回本地文件系统对象指针
     */
    static std::shared_ptr<LocalFileSystem> CreateFs(FileSystemType type,
                                                const std::string& deviceID);
};

}  // namespace fs
}  // namespace curve
#endif  // SRC_FS_LOCAL_FILESYSTEM_H_
