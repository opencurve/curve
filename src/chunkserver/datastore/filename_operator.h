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
 * Created Date: Monday December 3rd 2018
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_DATASTORE_FILENAME_OPERATOR_H_
#define SRC_CHUNKSERVER_DATASTORE_FILENAME_OPERATOR_H_

#include <string>
#include <vector>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/string_util.h"

namespace curve {
namespace chunkserver {

using std::string;
using std::vector;

class FileNameOperator {
 public:
    enum class FileType {
        CHUNK,
        SNAPSHOT,
        UNKNOWN,
    };

    struct FileInfo {
        FileType    type;
        ChunkID     id;
        SequenceNum sn;
    };

    FileNameOperator() {}
    virtual ~FileNameOperator() {}

    static inline string GenerateChunkFileName(ChunkID id) {
        return "chunk_" + std::to_string(id);
    }

    static inline string GenerateSnapshotName(ChunkID id, SequenceNum sn) {
        return GenerateChunkFileName(id)
                + "_snap_" + std::to_string(sn);
    }

    static inline FileInfo ParseFileName(const string& fileName) {
        vector<string> elements;
        ::curve::common::SplitString(fileName, "_", &elements);
        FileInfo info;
        info.type = FileType::UNKNOWN;

        // chunk文件名为 chunk_id的格式
        // 快照文件名为 chunk_id_snap_sn 的格式
        // 以“_”分隔文件名，解析文件信息
        // 如果不符合上述格式，则文件类型为UNKNOWN
        if (elements.size() == 2
            && elements[0].compare("chunk") == 0) {
            info.id = std::stoull(elements[1]);
            info.type = FileType::CHUNK;
        } else if (elements.size() == 4
                   && elements[0].compare("chunk") == 0
                   && elements[2].compare("snap") == 0) {
            info.id = std::stoull(elements[1]);
            info.sn = std::stoull(elements[3]);
            info.type = FileType::SNAPSHOT;
        }

        return info;
    }
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_FILENAME_OPERATOR_H_
