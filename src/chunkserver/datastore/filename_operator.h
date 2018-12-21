/*
 * Project: curve
 * Created Date: Monday December 3rd 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_FILENAME_OPERATOR_H
#define CURVE_CHUNKSERVER_FILENAME_OPERATOR_H

#include <string>
#include <vector>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/string_util.h"

namespace curve {
namespace chunkserver {

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

#endif  // CURVE_CHUNKSERVER_FILENAME_OPERATOR_H
