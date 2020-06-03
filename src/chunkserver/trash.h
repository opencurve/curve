/*
 * Project: curve
 * Created Date: Mon Apr 27th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_TRASH_H_
#define SRC_CHUNKSERVER_TRASH_H_

#include <memory>
#include <string>
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"

using ::curve::common::Thread;
using ::curve::common::Atomic;
using ::curve::common::InterruptibleSleeper;

namespace curve {
namespace chunkserver {
struct TrashOptions{
    // copyset的trash路径
    std::string trashPath;
    // 文件在放入trash中expiredAfteSec秒后，可以被物理回收
    int expiredAfterSec;
    // 扫描trash目录的时间间隔
    int scanPeriodSec;

    std::shared_ptr<LocalFileSystem> localFileSystem;
    std::shared_ptr<ChunkfilePool> chunkfilePool;
};

class Trash {
 public:
    int Init(TrashOptions options);

    int Run();

    int Fini();

    /*
    * @brief DeleteEligibleFileInTrash 回收trash目录下的物理空间
    */
    void DeleteEligibleFileInTrash();

    int RecycleCopySet(const std::string &dirPath);

    /*
    * @brief 获取回收站中chunk的个数
    *
    * @return chunk个数
    */
    uint32_t GetChunkNum() {return chunkNum_.load();}

 private:
    /*
    * @brief DeleteEligibleFileInTrashInterval 每隔一段时间进行trash物理空间回收
    */
    void DeleteEligibleFileInTrashInterval();

    /*
    * @brief NeedDelete 文件是否需要删除，放入trash的时间大于
    *        trash中expiredAfterSec可以删除
    *
    * @param[in] copysetDir copyset的目录路径
    *
    * @return true-可以被删除
    */
    bool NeedDelete(const std::string &copysetDir);

    /*
    * @brief IsCopysetInTrash 是否为回收站中的copyset的目录
    *
    * @param[in] dirName 文目录路径
    *
    * @return true-符合copyset目录命名规则
    */
    bool IsCopysetInTrash(const std::string &dirName);

    /*
    * @brief IsChunkOrSnapShotFile 是否为chunk或snapshot文件
    *
    * @param[in] chunkName 文件名
    *
    * @return true-符合chunk或snapshot文件命名规则
    */
    bool IsChunkOrSnapShotFile(const std::string &chunkName);

    /*
    * @brief RecycleChunksInCopyset 回收指定文件下的chunk
    *
    * @param[in] copysetDir 文件路径
    * @param[in] filename 文件名
    */
    bool RecycleChunksInDir(
        const std::string &copysetDir, const std::string &filename);

    /*
    * @brief RecycleIfChunkfile 如果是chunk文件则回收
    *
    * @param[in] filepath 文件路径
    * @param[in] filename 文件名
    */
    bool RecycleIfChunkfile(
        const std::string &filepath, const std::string &filename);

    /*
    * @brief 统计copyset目录中的chunk个数
    *
    * @param[in] copysetPath chunk所在目录
    * @return 返回chunk个数
    */
    uint32_t CountChunkNumInCopyset(const std::string &copysetPath);

 private:
    // 文件在放入trash中expiredAfteSec秒后，可以被物理回收
    int expiredAfterSec_;

    // 扫描trash目录的时间间隔
    int scanPeriodSec_;

    // 回收站中chunk的个数
    Atomic<uint32_t> chunkNum_;

    // 本地文件系统
    std::shared_ptr<LocalFileSystem> localFileSystem_;

    // chunk池子
    std::shared_ptr<ChunkfilePool> chunkfilePool_;

    // 回收站全路径
    std::string trashPath_;

    // 后台清理回收站的线程
    Thread recycleThread_;

    // false-开始后台任务，true-停止后台任务
    Atomic<bool> isStop_;

    InterruptibleSleeper sleeper_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_TRASH_H_

