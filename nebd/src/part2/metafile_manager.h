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

#include "src/common/concurrent/rw_lock.h"
#include "nebd/src/common/posix_wrapper.h"
#include "nebd/src/part2/define.h"
#include "nebd/src/part2/util.h"

#include "src/common/crc32.h"

namespace nebd {
namespace server {

using nebd::common::PosixWrapper;
using curve::common::RWLock;
using curve::common::WriteLockGuard;
using curve::common::ReadLockGuard;
using FileMetaMap = std::unordered_map<std::string, NebdFileMeta>;

const char kVolumes[] = "volumes";
const char kFileName[] = "filename";
const char kFd[] = "fd";
const char kCRC[] = "crc";

class NebdMetaFileParser {
 public:
    int Parse(Json::Value root,
              FileMetaMap* fileMetas);
    Json::Value ConvertFileMetasToJson(const FileMetaMap& fileMetas);
};

struct NebdMetaFileManagerOption {
    std::string metaFilePath = "";
    std::shared_ptr<PosixWrapper> wrapper
        = std::make_shared<PosixWrapper>();
    std::shared_ptr<NebdMetaFileParser> parser
        = std::make_shared<NebdMetaFileParser>();
};

class NebdMetaFileManager {
 public:
    NebdMetaFileManager();
    virtual ~NebdMetaFileManager();

    // 初始化，主要从文件读取元数据信息并加载到内存
    virtual int Init(const NebdMetaFileManagerOption& option);

    // 列出文件记录
    virtual int ListFileMeta(std::vector<NebdFileMeta>* fileMetas);

    // 更新文件元数据
    virtual int UpdateFileMeta(const std::string& fileName,
                               const NebdFileMeta& fileMeta);

    // 删除文件元数据
    virtual int RemoveFileMeta(const std::string& fileName);

 private:
    // 原子写文件
    int AtomicWriteFile(const Json::Value& root);
    // 更新元数据文件并更新内存缓存
    int UpdateMetaFile(const FileMetaMap& fileMetas);
    // 初始化从持久化文件读取到内存
    int LoadFileMeta();

 private:
    // 元数据文件路径
    std::string metaFilePath_;
    // 文件系统操作封装
    std::shared_ptr<common::PosixWrapper> wrapper_;
    // 用于解析Json格式的元数据
    std::shared_ptr<NebdMetaFileParser> parser_;
    // MetaFileManager 线程安全读写锁
    RWLock rwLock_;
    // meta文件内存缓存
    FileMetaMap metaCache_;
};
using MetaFileManagerPtr = std::shared_ptr<NebdMetaFileManager>;

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_METAFILE_MANAGER_H_
