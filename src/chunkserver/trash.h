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
 * Created Date: Mon Apr 27th 2019
 * Author: lixiaocui
 */

#ifndef SRC_CHUNKSERVER_TRASH_H_
#define SRC_CHUNKSERVER_TRASH_H_

#include <memory>
#include <string>
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"

using ::curve::common::Thread;
using ::curve::common::Atomic;
using ::curve::common::Mutex;
using ::curve::common::LockGuard;
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
    std::shared_ptr<FilePool> chunkFilePool;
    std::shared_ptr<FilePool> walPool;
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

    /**
     * @brief is WAL or not ?
     *
     * @param fileName file name
     *
     * @retval true yes
     * @retval false no
     */
    static bool IsWALFile(const std::string& fileName);

    /*
     * @brief IsChunkOrSnapShotFile 是否为chunk或snapshot文件
     *
     * @param[in] chunkName 文件名
     *
     * @return true-符合chunk或snapshot文件命名规则
     */
    static bool IsChunkOrSnapShotFile(const std::string& chunkName);

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
    * @brief Recycle Chunkfile and wal file in Copyset
    *
    * @param[in] copysetDir copyset dir
    * @param[in] filename filename
    */
    bool RecycleChunksAndWALInDir(
        const std::string &copysetDir, const std::string &filename);

    /*
    * @brief Recycle Chunkfile
    *
    * @param[in] filepath 文件路径
    * @param[in] filename 文件名
    */
    bool RecycleChunkfile(
        const std::string &filepath, const std::string &filename);

    /**
     * @brief Recycle WAL
     *
     * @param copysetPath copyset dir
     * @param filename file name
     *
     * @retval true   success
     * @retval false  failure
     */
    bool RecycleWAL(const std::string& filepath, const std::string& filename);

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

    Mutex mtx_;

    // 本地文件系统
    std::shared_ptr<LocalFileSystem> localFileSystem_;

    // chunk池子
    std::shared_ptr<FilePool> chunkFilePool_;

    // wal pool
    std::shared_ptr<FilePool> walPool_;

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

