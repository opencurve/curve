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
 * File Created: Monday, 10th December 2018 9:54:34 am
 * Author: tongguangxun
 */

#ifndef SRC_CHUNKSERVER_DATASTORE_FILE_POOL_H_
#define SRC_CHUNKSERVER_DATASTORE_FILE_POOL_H_

#include <glog/logging.h>

#include <set>
#include <mutex>  // NOLINT
#include <vector>
#include <string>
#include <memory>
#include <deque>
#include <atomic>

#include "src/fs/local_filesystem.h"
#include "include/curve_compiler_specific.h"

using curve::fs::LocalFileSystem;
namespace curve {
namespace chunkserver {

struct FilePoolOptions {
    bool        getFileFromPool;
    // it should be set when getFileFromPool=false
    char        filePoolDir[256];
    uint32_t    fileSize;
    uint32_t    metaPageSize;
    char        metaPath[256];
    uint32_t    metaFileSize;
    // retry times for get file
    uint16_t    retryTimes;

    FilePoolOptions() {
        getFileFromPool = true;
        metaFileSize = 4096;
        fileSize = 0;
        metaPageSize = 0;
        retryTimes = 5;
        ::memset(metaPath, 0, 256);
        ::memset(filePoolDir, 0, 256);
    }

    FilePoolOptions& operator=(const FilePoolOptions& other) {
        getFileFromPool   = other.getFileFromPool;
        metaFileSize     = other.metaFileSize;
        fileSize    = other.fileSize;
        retryTimes   = other.retryTimes;
        metaPageSize = other.metaPageSize;
        ::memcpy(metaPath, other.metaPath, 256);
        ::memcpy(filePoolDir, other.filePoolDir, 256);
        return *this;
    }

    FilePoolOptions(const FilePoolOptions& other) {
        getFileFromPool   = other.getFileFromPool;
        metaFileSize     = other.metaFileSize;
        fileSize    = other.fileSize;
        retryTimes   = other.retryTimes;
        metaPageSize = other.metaPageSize;
        ::memcpy(metaPath, other.metaPath, 256);
        ::memcpy(filePoolDir, other.filePoolDir, 256);
    }
};

typedef struct FilePoolState {
    // 预分配的chunk还有多少没有被datastore使用
    uint64_t    preallocatedChunksLeft;
    // chunksize
    uint32_t    chunkSize;
    // metapage size
    uint32_t    metaPageSize;
} FilePoolState_t;

class FilePoolHelper {
 public:
    static const char* kFileSize;
    static const char* kMetaPageSize;
    static const char* kFilePoolPath;
    static const char* kCRC;
    static const uint32_t kPersistSize;

     /**
     * 持久化chunkfile pool meta信息
     * @param[in]: 用于持久化的文件系统
     * @param[in]: chunkSize每个chunk的大小
     * @param[in]: metaPageSize每个chunkfile的metapage大小
     * @param[in]: FilePool_path是chunk池的路径
     * @param[in]: persistPathmeta信息要持久化的路径
     * @return: 成功0， 否则-1
     */
    static int PersistEnCodeMetaInfo(std::shared_ptr<LocalFileSystem> fsptr,
                               uint32_t fileSize,
                               uint32_t metaPageSize,
                               const std::string& filepoolPath,
                               const std::string& persistPath);

    /**
     * 从持久化的meta数据中解析出当前chunk池子的信息
     * @param[in]: 用于持久化的文件系统
     * @param[in]: metafile路径
     * @param[in]: meta文件大小
     * @param[out]: chunkSize每个chunk的大小
     * @param[out]: metaPageSize每个chunkfile的metapage大小
     * @param[out]: FilePool_path是chunk池的路径
     * @return: 成功0， 否则-1
     */
    static int DecodeMetaInfoFromMetaFile(
                                  std::shared_ptr<LocalFileSystem> fsptr,
                                  const std::string& metaFilePath,
                                  uint32_t metaFileSize,
                                  uint32_t* fileSize,
                                  uint32_t* metaPageSize,
                                  std::string* filepoolPath);
};

class CURVE_CACHELINE_ALIGNMENT FilePool {
 public:
    explicit FilePool(std::shared_ptr<LocalFileSystem> fsptr);
    virtual ~FilePool() = default;

    /**
     * 初始化函数
     * @param: cfop是配置选项
     */
    virtual bool Initialize(const FilePoolOptions& cfop);
    /**
     * datastore通过GetChunk接口获取新的chunk，GetChunk内部会将metapage原子赋值后返回。
     * @param: chunkpath是新的chunkfile路径
     * @param: metapage是新的chunk的metapage信息
     */
    virtual int GetFile(const std::string& chunkpath, char* metapage);
    /**
     * datastore删除chunk直接回收，不真正删除
     * @param: chunkpath是需要回收的chunk路径
     */
    virtual int RecycleFile(const std::string& chunkpath);
    /**
     * 获取当前chunkfile pool大小
     */
    virtual size_t Size();
    /**
     * 获取FilePool的分配状态
     */
    virtual FilePoolState_t GetState();
    /**
     * 获取当前FilePool的option配置信息
     */
    virtual FilePoolOptions GetFilePoolOpt() {
        return poolOpt_;
    }
    /**
     * 析构,释放资源
     */
    virtual void UnInitialize();

    /**
     * 测试使用
     */
    virtual void SetLocalFileSystem(std::shared_ptr<LocalFileSystem> fs) {
        CHECK(fs != nullptr) << "fs ptr allocate failed!";
        fsptr_ = fs;
    }

 private:
    // 从chunkfile pool目录中遍历预分配的chunk信息
    bool ScanInternal();
    // 检查chunkfile pool预分配是否合法
    bool CheckValid();
    /**
     * 为新的chunkfile进行metapage赋值
     * @param: sourcepath为要写入的文件路径
     * @param: page为要写入的metapage信息
     * @return: 成功返回true，否则返回false
     */
    bool WriteMetaPage(const std::string& sourcepath, char* page);
    /**
     * 直接分配chunk，不从FilePool获取
     * @param: chunkpath为datastore中chunk文件的路径
     * @return: 成功返回0，否则返回小于0
     */
    int AllocateChunk(const std::string& chunkpath);

 private:
    // 保护tmpChunkvec_
    std::mutex mtx_;

    // 当前FilePool的预分配文件，文件夹路径
    std::string currentdir_;

    // chunkserver端封装的底层文件系统接口，提供操作文件的基本接口
    std::shared_ptr<LocalFileSystem> fsptr_;

    // 内存中持有的chunkfile pool中的文件名的数字格式
    std::vector<uint64_t> tmpChunkvec_;

    // 当前最大的文件名数字格式
    std::atomic<uint64_t> currentmaxfilenum_;

    // FilePool配置选项
    FilePoolOptions poolOpt_;

    // FilePool分配状态
    FilePoolState_t currentState_;
};
}   // namespace chunkserver
}   // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_FILE_POOL_H_
