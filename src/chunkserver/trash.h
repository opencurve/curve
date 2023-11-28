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

#include "src/chunkserver/datastore/file_pool.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "src/fs/local_filesystem.h"

using ::curve::common::Atomic;
using ::curve::common::InterruptibleSleeper;
using ::curve::common::LockGuard;
using ::curve::common::Mutex;
using ::curve::common::Thread;

namespace curve {
namespace chunkserver {
struct TrashOptions {
    // The trash path of copyset
    std::string trashPath;
    // The file can be physically recycled after being placed in trash for
    // expiredAfteSec seconds
    int expiredAfterSec;
    // Time interval for scanning the trash directory
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
     * @brief DeleteEligibleFileInTrash recycles the physical space in the trash
     * directory
     */
    void DeleteEligibleFileInTrash();

    int RecycleCopySet(const std::string& dirPath);

    /*
     * @brief Get the number of chunks in the recycle bin
     *
     * @return Number of chunks
     */
    uint32_t GetChunkNum() { return chunkNum_.load(); }

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
     * @brief DeleteEligibleFileInTrashInterval Trash physical space recycling
     * at regular intervals
     */
    void DeleteEligibleFileInTrashInterval();

    /*
     * @brief NeedDelete Does the file need to be deleted, and the time it takes
     * to place the trash is greater than ExpiredAfterSec in trash can be
     * deleted
     *
     * @param[in] copysetDir: copyset directory path
     *
     * @return true-can be deleted
     */
    bool NeedDelete(const std::string& copysetDir);

    /*
     * @brief IsCopysetInTrash Is the directory of the copyset in the recycle
     * bin
     *
     * @param[in] dirName: directory path
     *
     * @return true-Complies with copyset directory naming rules
     */
    bool IsCopysetInTrash(const std::string& dirName);

    /*
     * @brief Recycle Chunkfile and wal file in Copyset
     *
     * @param[in] copysetDir: copyset dir
     * @param[in] filename: filename
     */
    bool RecycleChunksAndWALInDir(const std::string& copysetDir,
                                  const std::string& filename);

    /*
     * @brief Recycle Chunkfile
     *
     * @param[in] filepath: file path
     * @param[in] filename: File name
     */
    bool RecycleChunkfile(const std::string& filepath,
                          const std::string& filename);

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
     * @brief Counts the number of chunks in the copyset directory
     *
     * @param[in] copysetPath: Chunk directory
     * @return the number of chunks
     */
    uint32_t CountChunkNumInCopyset(const std::string& copysetPath);

 private:
    // The file can be physically recycled after being placed in trash for
    // expiredAfterSec seconds
    int expiredAfterSec_;

    // Time interval for scanning the trash directory
    int scanPeriodSec_;

    // Number of chunks in the Recycle Bin
    Atomic<uint32_t> chunkNum_;

    Mutex mtx_;

    // Local File System
    std::shared_ptr<LocalFileSystem> localFileSystem_;

    // chunk Pool
    std::shared_ptr<FilePool> chunkFilePool_;

    // wal pool
    std::shared_ptr<FilePool> walPool_;

    // Recycle Bin Full Path
    std::string trashPath_;

    // Thread for background cleaning of the recycle bin
    Thread recycleThread_;

    // false-Start background task, true-Stop background task
    Atomic<bool> isStop_;

    InterruptibleSleeper sleeper_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_TRASH_H_
