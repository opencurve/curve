/*
 * Project: curve
 * Created Date: 18-12-20
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_CONF_EPOCH_FILE_H_
#define SRC_CHUNKSERVER_CONF_EPOCH_FILE_H_

#include <string>

#include "src/fs/local_filesystem.h"
#include "src/fs/fs_common.h"
#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

/**
 * 配置版本序列化和反序列化的工具类
 */
class ConfEpochFile {
 public:
    explicit ConfEpochFile(std::shared_ptr<LocalFileSystem> fs)
        : fs_(fs) {}

    /**
     * 加载快照文件中的配置版本
     * @param path:文件路径
     * @param logicPoolID:逻辑池id
     * @param copysetID:复制组id
     * @param epoch:配置版本，出参，返回读取的epoch值
     * @return 0，成功； -1失败
     */
    int Load(const std::string &path,
             LogicPoolID *logicPoolID,
             CopysetID *copysetID,
             uint64_t *epoch);

    /**
     * 保存配置版本信息到快照文件中序列化的格式如下，处理head表示长度，使用二
     * 进制，其它都是文本格式，便于必要的时候能够直接用查看，sync保证数据落盘
     * |              head           |          配置版本信息                |
     * | 8 bytes size_t   | uint32_t |              变 长文本              |
     * |     length       |   crc32  | logic pool id | copyset id | epoch |
     * 上面的持久化使用 ‘：’ 分隔
     * @param path:文件路径
     * @param logicPoolID:逻辑池id
     * @param copysetID:复制组id
     * @param epoch:配置版本
     * @return 0成功； -1失败
     */
    int Save(const std::string &path,
             const LogicPoolID logicPoolID,
             const CopysetID copysetID,
             const uint64_t epoch);

 private:
    std::shared_ptr<LocalFileSystem> fs_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CONF_EPOCH_FILE_H_
