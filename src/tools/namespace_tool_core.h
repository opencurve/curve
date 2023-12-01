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
 * Created Date: 2019-12-3
 * Author: charisu
 */

#ifndef SRC_TOOLS_NAMESPACE_TOOL_CORE_H_
#define SRC_TOOLS_NAMESPACE_TOOL_CORE_H_

#include <gflags/gflags.h>
#include <time.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/common/fs_util.h"
#include "src/common/string_util.h"
#include "src/common/timeutility.h"
#include "src/mds/common/mds_define.h"
#include "src/tools/mds_client.h"

using curve::common::ChunkServerLocation;
using curve::mds::FileInfo;
using curve::mds::PageFileChunkInfo;
using curve::mds::PageFileSegment;
using curve::mds::StatusCode;
using curve::mds::topology::kTopoErrCodeSuccess;

namespace curve {
namespace tool {

using curve::mds::topology::PoolsetInfo;

class NameSpaceToolCore {
 public:
    explicit NameSpaceToolCore(std::shared_ptr<MDSClient> client);
    virtual ~NameSpaceToolCore() = default;

    /**
     * @brief Initialize mds client
     * @param mdsAddr Address of mds, supporting multiple addresses separated by
     * ','
     * @return returns 0 for success, -1 for failure
     */
    virtual int Init(const std::string& mdsAddr);

    /**
     * @brief Get file fileInfo
     * @param fileName File name
     * @param[out] fileInfo file fileInfo, valid when the return value is 0
     * @return returns 0 for success, -1 for failure
     */
    virtual int GetFileInfo(const std::string& fileName, FileInfo* fileInfo);

    /**
     * @brief List all fileInfo in the directory
     * @param dirName directory name
     * @param[out] files All fileInfo files in the directory are valid when the
     * return value is 0
     * @return returns 0 for success, -1 for failure
     */
    virtual int ListDir(const std::string& dirName,
                        std::vector<FileInfo>* files);

    /**
     * @brief Get the list of chunkservers in the copyset
     * @param logicalPoolId Logical Pool ID
     * @param copysetId copyset ID
     * @param[out] csLocs List of chunkserver locations, valid when the return
     * value is 0
     * @return returns 0 for success, -1 for failure
     */
    virtual int GetChunkServerListInCopySet(
        const PoolIdType& logicalPoolId, const CopySetIdType& copysetId,
        std::vector<ChunkServerLocation>* csLocs);

    /**
     * @brief Delete file
     * @param fileName File name
     * @param forcedelete: Do you want to force deletion
     * @return returns 0 for success, -1 for failure
     */
    virtual int DeleteFile(const std::string& fileName,
                           bool forcedelete = false);

    /**
     * @brief create pageFile or directory
     * @param fileName file name or dir name
     * @param length File length
     * @param normalFile is file or dir
     * @param stripeUnit stripe unit size
     * @param stripeCount the amount of stripes
     * @return returns 0 for success, -1 for failure
     */
    virtual int CreateFile(const CreateFileContext& ctx);

    /**
     * @brief expansion volume
     * @param fileName File name
     * @param newSize The file length after expansion
     * @return returns 0 for success, -1 for failure
     */
    virtual int ExtendVolume(const std::string& fileName, uint64_t newSize);

    /**
     * @brief Calculate the actual allocated space of a file or directory
     * @param fileName File name
     * @param[out] allocSize The file or directory has already been allocated a
     * size, and a return value of 0 is valid
     * @param[out] allocMap The allocation amount of each pool, valid when
     * returning a value of 0
     * @return returns 0 for success, -1 for failure
     */
    virtual int GetAllocatedSize(const std::string& fileName,
                                 uint64_t* allocSize,
                                 AllocMap* allocMap = nullptr);

    /**
     * @brief Returns the user requested size of files in a file or directory
     * @param fileName File name
     * @param[out] fileSize The size requested by the user in the file or
     * directory, with a return value of 0 being valid
     * @return returns 0 for success, -1 for failure
     */
    virtual int GetFileSize(const std::string& fileName, uint64_t* fileSize);

    /**
     * @brief Get the segment information of the file and output it to segments
     * @param fileName File name
     * @param[out] segments List of segments in the file
     * @return returns the actual allocated size of the file, if it fails, it
     * will be -1
     */
    virtual int GetFileSegments(const std::string& fileName,
                                std::vector<PageFileSegment>* segments);

    /**
     * @brief: Query the ID of the chunk corresponding to the offset and the
     * copyset it belongs to
     * @param fileName File name
     * @param offset Offset in file
     * @param[out] chunkId chunkId, valid when the return value is 0
     * @param[out] copyset The copyset corresponding to the chunk is the pair of
     * logicalPoolId and copysetId
     * @return returns 0 for success, -1 for failure
     */
    virtual int QueryChunkCopyset(const std::string& fileName, uint64_t offset,
                                  uint64_t* chunkId,
                                  std::pair<uint32_t, uint32_t>* copyset);

    /**
     *  @brief clean recycle bin
     *  @param dirName only clean file in the dirName if dirName is not empty
     *  @param expireTime time for file in recycle bin exceed expireTime
               will be deleted
     *  @return return 0 if success, else return -1
     */
    virtual int CleanRecycleBin(const std::string& dirName,
                                const uint64_t expireTime);

    virtual int UpdateFileThrottle(const std::string& fileName,
                                   const std::string& throttleType,
                                   const uint64_t limit, const int64_t burst,
                                   const int64_t burstLength);

    virtual int ListPoolset(std::vector<PoolsetInfo>* poolsets);

 private:
    /**
     * @brief Get the segment information of the file and output it to segments
     * @param fileName File name
     * @param fileInfo The fileInfo of the file
     * @param[out] segments List of segments in the file
     * @return returns the actual allocated size of the file, if it fails, it
     * will be -1
     */
    int GetFileSegments(const std::string& fileName, const FileInfo& fileInfo,
                        std::vector<PageFileSegment>* segments);

    // Client sending RPC to mds
    std::shared_ptr<MDSClient> client_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_NAMESPACE_TOOL_CORE_H_
