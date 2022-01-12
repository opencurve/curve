/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 18-12-20
 * Author: wudemiao
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_CONF_EPOCH_FILE_H_
#define CURVEFS_SRC_METASERVER_COPYSET_CONF_EPOCH_FILE_H_

#include <memory>
#include <string>

#include "curvefs/proto/copyset.pb.h"
#include "curvefs/src/metaserver/common/types.h"
#include "src/fs/local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

// TODO(wuhanqing): this file is identical to src/chunkserver/conf_epoch_file.h

class ConfEpochFile {
 public:
    explicit ConfEpochFile(curve::fs::LocalFileSystem* fs) : fs_(fs) {}

    /**
     * 加载快照文件中的配置版本
     * @param path:文件路径
     * @param logicPoolID:逻辑池id
     * @param copysetID:复制组id
     * @param epoch:配置版本，出参，返回读取的epoch值
     * @return 0，成功； -1失败
     */
    int Load(const std::string& path, PoolId* poolId, CopysetId* copysetId,
             uint64_t* epoch);

    /**
     * 保存配置版本信息到快照文件中序列化的格式如下，处理head表示长度，使用二
     * 进制，其它都是文本格式，便于必要的时候能够直接用查看，sync保证数据落盘
     * |              head           |          配置版本信息               |
     * | 8 bytes size_t   | uint32_t |              变 长文本              |
     * |     length       |   crc32  | logic pool id | copyset id | epoch |
     * 上面的持久化使用 ‘：’ 分隔
     * @param path:文件路径
     * @param logicPoolID:逻辑池id
     * @param copysetID:复制组id
     * @param epoch:配置版本
     * @return 0成功； -1失败
     */
    int Save(const std::string& path, const PoolId poolId,
             const CopysetId copysetId, const uint64_t epoch);

 private:
    static uint32_t ConfEpochCrc(const ConfEpoch& confEpoch);

    curve::fs::LocalFileSystem* fs_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_CONF_EPOCH_FILE_H_
