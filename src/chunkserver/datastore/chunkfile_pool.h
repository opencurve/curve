/*
 * Project: curve
 * File Created: Monday, 10th December 2018 9:54:34 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CHUNKSERVER_FILEPOOL_H
#define CHUNKSERVER_FILEPOOL_H

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

// chunkfilepool 配置选项
struct ChunkfilePoolOptions {
    // 配置文件的chunk大小
    uint32_t    chunksize;
    // 配置文件中的metapage大小
    uint32_t    metapagesize;
    // chunkfilepool meta文件地址
    char        metapath[256];
    // cpmetafilesize是chunkfilepool的 metafile信息
    uint32_t    cpmetafilesize;
    // GetChunk重试次数
    uint16_t    retrytimes;

    ChunkfilePoolOptions() {
        cpmetafilesize = 4096;
        chunksize = 0;
        metapagesize = 0;
        retrytimes = 5;
        ::memset(metapath, 0, 256);
    }

    ChunkfilePoolOptions& operator=(const ChunkfilePoolOptions& other) {
        cpmetafilesize     = other.cpmetafilesize;
        chunksize    = other.chunksize;
        retrytimes   = other.retrytimes;
        metapagesize = other.metapagesize;
        ::memcpy(metapath, other.metapath, 256);
        return *this;
    }

    ChunkfilePoolOptions(const ChunkfilePoolOptions& other) {
        cpmetafilesize     = other.cpmetafilesize;
        chunksize    = other.chunksize;
        retrytimes   = other.retrytimes;
        metapagesize = other.metapagesize;
        ::memcpy(metapath, other.metapath, 256);
    }
};

class CURVE_CACHELINE_ALIGNMENT ChunkfilePool {
 public:
    // fsptr 本地文件系统.
    explicit ChunkfilePool(std::shared_ptr<LocalFileSystem> fsptr);
    virtual ~ChunkfilePool() = default;
    virtual bool Initialize(const ChunkfilePoolOptions& cfop);
    /**
     * datastore通过GetChunk接口获取新的chunk，GetChunk内部会将metapage原子赋值后返回。
     * @param: chunkpath是新的chunkfile路径
     * @param: metapage是新的chunk的metapage信息
     */
    virtual int GetChunk(const std::string& chunkpath, char* metapage);
    /**
     * datastore删除chunk直接回收，不真正删除
     * @param: chunkpath是需要回收的chunk路径
     */
    virtual int RecycleChunk(const std::string& chunkpath);
    virtual size_t Size();
    virtual void UnInitialize();

 private:
    // 从chunkfile pool目录中遍历预分配的chunk信息
    bool ScanInternal();
    // 检查chunkfile pool预分配是否合法
    bool CheckValid();
    // 为新的chunkfile进行metapage赋值
    int WriteMetaPage(const std::string& sourcepath, char* page);

 private:
    // 保护tmpChunkvec_
    std::mutex mtx_;

    // 当前chunkfilepool的预分配文件，文件夹路径
    std::string currentdir_;

    // chunkserver端封装的底层文件系统接口，提供操作文件的基本接口
    std::shared_ptr<LocalFileSystem> fsptr_;

    // 内存中持有的chunkfile pool中的文件名的数字格式
    std::vector<uint64_t> tmpChunkvec_;

    // 当前最大的文件名数字格式
    std::atomic<uint64_t> currentmaxfilenum_;

    // chunkfilepool配置选项
    ChunkfilePoolOptions chunkPoolOpt_;
};
}   // namespace chunkserver
}   // namespace curve

#endif
