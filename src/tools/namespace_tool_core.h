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

#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include <cstdint>
#include <cstring>
#include <utility>

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/common/timeutility.h"
#include "src/common/string_util.h"
#include "src/mds/common/mds_define.h"
#include "src/tools/mds_client.h"

using curve::mds::FileInfo;
using curve::mds::PageFileSegment;
using curve::mds::StatusCode;
using curve::mds::PageFileChunkInfo;
using curve::mds::topology::kTopoErrCodeSuccess;
using curve::common::ChunkServerLocation;

namespace curve {
namespace tool {

class NameSpaceToolCore {
 public:
    explicit NameSpaceToolCore(std::shared_ptr<MDSClient> client);
    virtual ~NameSpaceToolCore() = default;

    /**
     *  @brief 初始化mds client
     *  @param mdsAddr mds的地址，支持多地址，用","分隔
     *  @return 成功返回0，失败返回-1
     */
    virtual int Init(const std::string& mdsAddr);

    /**
     *  @brief 获取文件fileInfo
     *  @param fileName 文件名
     *  @param[out] fileInfo 文件fileInfo，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetFileInfo(const std::string& fileName, FileInfo* fileInfo);

    /**
     *  @brief 将目录下所有的fileInfo列出来
     *  @param dirName 目录名
     *  @param[out] files 目录下的所有文件fileInfo，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListDir(const std::string& dirName,
                        std::vector<FileInfo>* files);

    /**
     *  @brief 获取copyset中的chunkserver列表
     *  @param logicalPoolId 逻辑池id
     *  @param copysetId copyset id
     *  @param[out] csLocs chunkserver位置的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetChunkServerListInCopySet(const PoolIdType& logicalPoolId,
                                     const CopySetIdType& copysetId,
                                     std::vector<ChunkServerLocation>* csLocs);

    /**
     *  @brief 删除文件
     *  @param fileName 文件名
     *  @param forcedelete 是否强制删除
     *  @return 成功返回0，失败返回-1
     */
    virtual int DeleteFile(const std::string& fileName,
                           bool forcedelete = false);

    /**
     *  @brief 创建pageFile文件
     *  @param fileName 文件名
     *  @param length 文件长度
     *  @param stripeUnit stripe unit size
     *  @param stripeCount the amount of stripes
     *  @return 成功返回0，失败返回-1
     */
    virtual int CreateFile(const std::string& fileName, uint64_t length,
                          uint64_t stripeUnit, uint64_t stripeCount);

   /**
     *  @brief 扩容卷
     *  @param fileName 文件名
     *  @param newSize 扩容后的文件长度
     *  @return 成功返回0，失败返回-1
     */
    virtual int ExtendVolume(const std::string& fileName, uint64_t newSize);

    /**
     *  @brief 计算文件或目录实际分配的空间
     *  @param fileName 文件名
     *  @param[out] allocSize 文件或目录已分配大小，返回值为0是有效
     *  @param[out] allocMap 在每个池子的分配量，返回值0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetAllocatedSize(const std::string& fileName,
                                 uint64_t* allocSize,
                                 AllocMap* allocMap = nullptr);

    /**
     *  @brief 返回文件或目录的中的文件的用户申请的大小
     *  @param fileName 文件名
     *  @param[out] fileSize 文件或目录中用户申请的大小，返回值为0是有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetFileSize(const std::string& fileName, uint64_t* fileSize);

    /**
     *  @brief 获取文件的segment信息并输出到segments里面
     *  @param fileName 文件名
     *  @param[out] segments 文件segment的列表
     *  @return 返回文件实际分配大小，失败则为-1
     */
    virtual int GetFileSegments(const std::string& fileName,
                       std::vector<PageFileSegment>* segments);

    /**
     *  @brief 查询offset对应的chunk的id和所属的copyset
     *  @param fileName 文件名
     *  @param offset 文件中的偏移
     *  @param[out] chunkId chunkId，返回值为0时有效
     *  @param[out] copyset chunk对应的copyset，是logicalPoolId和copysetId的pair
     *  @return 成功返回0，失败返回-1
     */
    virtual int QueryChunkCopyset(const std::string& fileName, uint64_t offset,
                          uint64_t* chunkId,
                          std::pair<uint32_t, uint32_t>* copyset);

    /**
     *  @brief 清空回收站
     *  @param fileName 可选参数，如果指定了，就只删除原来在fileName目录下的文件
     *  @return 成功返回0，失败返回-1
     */
    virtual int CleanRecycleBin(const std::string& dirName = "");

 private:
    /**
     *  @brief 获取文件的segment信息并输出到segments里面
     *  @param fileName 文件名
     *  @param fileInfo 文件的fileInfo
     *  @param[out] segments 文件segment的列表
     *  @return 返回文件实际分配大小，失败则为-1
     */
    int GetFileSegments(const std::string& fileName,
                        const FileInfo& fileInfo,
                        std::vector<PageFileSegment>* segments);

    // 向mds发送RPC的client
    std::shared_ptr<MDSClient> client_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_NAMESPACE_TOOL_CORE_H_
