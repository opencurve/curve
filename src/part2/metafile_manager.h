/*
 * Project: nebd
 * Created Date: 2020-01-19
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_METAFILE_MANAGER_H_
#define SRC_PART2_METAFILE_MANAGER_H_

#include <json/json.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <thread>  // NOLINT
#include <mutex>   // NOLINT

#include "src/common/posix_wrapper.h"
#include "src/part2/define.h"

namespace nebd {
namespace server {

using nebd::common::PosixWrapper;
using FileRecordMap = std::unordered_map<int, NebdFileRecord>;

const char kVolumes[] = "volumes";
const char kFileName[] = "filename";
const char kFd[] = "fd";

class NebdMetaFileParser {
 public:
    int Parse(const Json::Value& root,
              FileRecordMap* fileRecords);
};

class NebdMetaFileManager {
 public:
    NebdMetaFileManager(const std::string& metaFilePath,
                        std::shared_ptr<PosixWrapper> wrapper =
                            std::make_shared<PosixWrapper>(),
                        std::shared_ptr<NebdMetaFileParser> parser =
                            std::make_shared<NebdMetaFileParser>());
    virtual ~NebdMetaFileManager();

    /**
     * @brief 列出文件记录
     *
     * @param fileRecord 持久化的文件记录
     *
     * @return 成功返回0，失败返回-1
     *
     */
    virtual int ListFileRecord(FileRecordMap* fileRecords);

    // 更新元数据文件
    virtual int UpdateMetaFile(const FileRecordMap& fileRecords);

 private:
    // 原子写文件
    int AtomicWriteFile(const Json::Value& root);

 private:
    // 元数据文件路径
    std::string metaFilePath_;
    // 文件系统操作封装
    std::shared_ptr<common::PosixWrapper> wrapper_;
    // 用于解析Json格式的元数据
    std::shared_ptr<NebdMetaFileParser> parser_;
};
using MetaFileManagerPtr = std::shared_ptr<NebdMetaFileManager>;

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_METAFILE_MANAGER_H_
