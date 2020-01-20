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
#include <memory>
#include <thread>  // NOLINT
#include <mutex>   // NOLINT

#include "src/common/posix_wrapper.h"
#include "src/part2/define.h"

namespace nebd {
namespace server {

using nebd::common::PosixWrapper;

const char kVolumes[] = "volumes";
const char kFileName[] = "filename";
const char kFd[] = "fd";

class NebdMetaFileParser {
 public:
    int Parse(const Json::Value& root,
              std::vector<NebdFileRecordPtr>* fileRecords);
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
     * @brief 删除文件记录
     *
     * @param fileName 要删除的文件名
     *
     * @return 成功返回0，失败返回-1
     *
     */
    virtual int RemoveFileRecord(const std::string& fileName);

    /**
     * @brief 更新文件记录
     *
     * @param fileRecord 要更新的文件记录
     *
     * @return 成功返回0，失败返回-1
     *
     */
    virtual int UpdateFileRecord(const NebdFileRecordPtr& fileRecord);

    /**
     * @brief 列出文件记录
     *
     * @param fileRecord 要更新的文件记录
     *
     * @return 成功返回0，失败返回-1
     *
     */
    virtual int ListFileRecord(std::vector<NebdFileRecordPtr>* fileRecords);

 private:
    // 更新元数据文件
    int UpdateMetaFile(
                  const std::vector<NebdFileRecordPtr>& fileRecords);

    // 原子写文件
    int AtomicWriteFile(const Json::Value& root);

    // 判断两条记录是否相等
    bool RecordsEqual(const NebdFileRecordPtr& record1,
                     const NebdFileRecordPtr& record2);

    // 内部List，无锁版本
    int ListFileRecordUnlocked(std::vector<NebdFileRecordPtr>* fileRecords);

    // 从文件list fileRecord
    int ListFileRecordFromFile(std::vector<NebdFileRecordPtr>* fileRecords);

 private:
    // 元数据文件路径
    std::string metaFilePath_;
    // 文件系统操作封装
    std::shared_ptr<common::PosixWrapper> wrapper_;
    // 用于解析Json格式的元数据
    std::shared_ptr<NebdMetaFileParser> parser_;
    // 读写锁，保护json和文件
    std::mutex  mtx_;
    // 元数据的json
    Json::Value metaDataJson_;
    // 是否从文件加载过
    bool loaded_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_METAFILE_MANAGER_H_
