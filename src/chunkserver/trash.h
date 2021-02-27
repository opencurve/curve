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
using ::curve::common::InterruptibleSleeper;

namespace curve {
namespace chunkserver {
struct TrashOptions{
    // copyset trash path
    std::string trashPath;
    // Files can be physically recycled after being placed in trash for expiredAfteSec seconds
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
    * @brief DeleteEligibleFileInTrash Physical space in the trash directory
    */
    void DeleteEligibleFileInTrash();

    int RecycleCopySet(const std::string &dirPath);

    /*
    * @brief Get the number of chunks in the trash
    *
    * @return the number of chunks
    */
    uint32_t GetChunkNum() {return chunkNum_.load();}

 private:
    /*
    * @brief DeleteEligibleFileInTrashInterval Perform trash physical space recycle at certain time intervals
    */
    void DeleteEligibleFileInTrashInterval();

    /*
    * @brief Whether the file needs to be deleted, it can be deleted if it has been in the trash for
    *        longer than the expiredAfterSec in the trash.
    *
    * @param[in] copysetDir copyset directory path
    *
    * @return true-can be deleted
    */
    bool NeedDelete(const std::string &copysetDir);

    /*
    * @brief IsCopysetInTrash whether it is the directory of the copyset in the trash
    *
    * @param[in] dirName Directory path
    *
    * @return true-Conform to copyset directory naming rules
    */
    bool IsCopysetInTrash(const std::string &dirName);

    /*
    * @brief IsChunkOrSnapShotFile Whether it is a chunk or snapshot file
    *
    * @param[in] chunkName File name
    *
    * @return true-Conform to chunk or snapshot file naming rules
    */
    bool IsChunkOrSnapShotFile(const std::string &chunkName);

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
    * @param[in] filepath file path
    * @param[in] filename file name
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
    bool RecycleWAL(
        const std::string &filepath, const std::string &filename);


    /**
     * @brief is WAL or not ?
     *
     * @param fileName file name
     *
     * @retval true yes
     * @retval false no
     */
    bool IsWALFile(const std::string &fileName);

    /*
    * @brief Count the number of chunks in the copyset directory
    *
    * @param[in] copysetPath chunk directory
    * @return Return the number of chunks
    */
    uint32_t CountChunkNumInCopyset(const std::string &copysetPath);

 private:
    // Files can be physically recycled after being placed in trash for expiredAfteSec seconds
    int expiredAfterSec_;

    // Time interval for scanning the trash directory
    int scanPeriodSec_;

    // Number of chunks in the recycle bin
    Atomic<uint32_t> chunkNum_;

    // Local file system
    std::shared_ptr<LocalFileSystem> localFileSystem_;

    // chunk pool
    std::shared_ptr<FilePool> chunkFilePool_;

    // wal pool
    std::shared_ptr<FilePool> walPool_;

    // trash pool
    std::string trashPath_;

    // Backend threads to clean up the trash
    Thread recycleThread_;

    // false-Start background tasks，true-Stop background tasks
    Atomic<bool> isStop_;

    InterruptibleSleeper sleeper_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_TRASH_H_

