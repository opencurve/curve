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

using FileEntityMap = std::unordered_map<int, NebdFileEntityPtr>;
class NebdFileManager {
 public:
    explicit NebdFileManager(MetaFileManagerPtr metaFileManager);
    virtual ~NebdFileManager();
    /**
     * 停止FileManager并释放FileManager资源
     * @return 成功返回0，失败返回-1
     */
    virtual int Fini();
    /**
     * 启动FileManager
     * @return 成功返回0，失败返回-1
     */
    virtual int Run();
    /**
     * 打开文件
     * @param filename: 文件的filename
     * @return 成功返回fd，失败返回-1
     */
    virtual int Open(const std::string& filename);
    /**
     * 关闭文件
     * @param fd: 文件的fd
     * @param removeRecord: 是否要移除文件记录，true表示移除，false表示不移除
     * 如果是part1传过来的close请求，此参数为true
     * 如果是heartbeat manager发起的close请求，此参数为false
     * @return 成功返回0，失败返回-1
     */
    virtual int Close(int fd, bool removeRecord);
    /**
     * 给文件扩容
     * @param fd: 文件的fd
     * @param newsize: 新的文件大小
     * @return 成功返回0，失败返回-1
     */
    virtual int Extend(int fd, int64_t newsize);
    /**
     * 获取文件信息
     * @param fd: 文件的fd
     * @param fileInfo[out]: 文件信息
     * @return 成功返回0，失败返回-1
     */
    virtual int GetInfo(int fd, NebdFileInfo* fileInfo);
    /**
     * 异步请求，回收指定区域空间
     * @param fd: 文件的fd
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int Discard(int fd, NebdServerAioContext* aioctx);
    /**
     * 异步请求，读取指定区域内容
     * @param fd: 文件的fd
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int AioRead(int fd, NebdServerAioContext* aioctx);
    /**
     * 异步请求，写数据到指定区域
     * @param fd: 文件的fd
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int AioWrite(int fd, NebdServerAioContext* aioctx);
    /**
     * 异步请求，flush文件缓存
     * @param fd: 文件的fd
     * @param aioctx: 异步请求上下文
     * @return 成功返回0，失败返回-1
     */
    virtual int Flush(int fd, NebdServerAioContext* aioctx);
    /**
     * 使指定文件缓存失效
     * @param fd: 文件的fd
     * @return 成功返回0，失败返回-1
     */
    virtual int InvalidCache(int fd);

    // 根据fd从map中获取指定的entity
    // 如果entity已存在，返回entity指针，否则返回nullptr
    virtual NebdFileEntityPtr GetFileEntity(int fd);

    virtual FileEntityMap GetFileEntityMap();

    // 将所有文件状态输出到字符串
    std::string DumpAllFileStatus();

    // set public for test
    // 启动时从metafile加载文件记录，并reopen文件
    int Load();

 private:
     // 分配新的可用的fd，fd不允许和已经存在的重复
     // 成功返回的可用fd，失败返回-1
    int GenerateValidFd();
    // 根据文件名获取file entity
    // 如果entity存在，直接返回entity指针
    // 如果entity不存在，则创建新的entity，并插入map，然后返回
    NebdFileEntityPtr GetOrCreateFileEntity(const std::string& fileName);
    // 根据fd和文件名生成file entity，
    // 如果fd对于的entity已存在,直接返回entity指针
    // 如果entity不存在，则生成新的entity，并插入map，然后返回
    NebdFileEntityPtr GenerateFileEntity(int fd, const std::string& fileName);
    // 删除指定fd对应的entity
    void RemoveEntity(int fd);

 private:
    // 当前filemanager的运行状态，true表示正在运行，false标为未运行
    std::atomic<bool> isRunning_;
    // 文件名锁，对同名文件加锁
    NameLock nameLock_;
    // fd分配器
    FdAllocator fdAlloc_;
    // nebd server 文件记录管理
    MetaFileManagerPtr metaFileManager_;
    // file map 读写保护锁
    RWLock rwLock_;
    // 文件fd和文件实体的映射
    FileEntityMap fileMap_;
};
using NebdFileManagerPtr = std::shared_ptr<NebdFileManager>;

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_FILE_MANAGER_H_
