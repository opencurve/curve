/*
 * Project: curve
 * Created Date: 2020-02-28
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_RAFT_LOG_TOOL_H_
#define SRC_TOOLS_RAFT_LOG_TOOL_H_

#include <gflags/gflags.h>
#include <braft/util.h>
#include <butil/raw_pack.h>
#include <fcntl.h>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include "src/fs/local_filesystem.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

namespace curve {
namespace tool {

using curve::fs::LocalFileSystem;

const size_t ENTRY_HEADER_SIZE = 24;

struct EntryHeader {
    int64_t term;
    int type;
    int checksum_type;
    uint32_t data_len;
    uint32_t data_checksum;

    bool operator== (const EntryHeader& rhs) const;
};

std::ostream& operator<<(std::ostream& os, const EntryHeader& h);

class SegmentParser {
 public:
    explicit SegmentParser(std::shared_ptr<LocalFileSystem> localFS) :
                localFS_(localFS) {}

    /**
     *  @brief 初始化
     *  @param fileName segmnet文件的文件名
     *  @return 获取成功返回0，失败返回-1
     */
    virtual int Init(const std::string& fileName);

    /**
     *  @brief 反初始化
     */
    virtual void UnInit();

    /**
     *  @brief 获取下一个EntryHeader
     *  @param[out] header log entry header
     *  @return 获取成功返回true，失败返回false
     */
    virtual bool GetNextEntryHeader(EntryHeader* header);

    /**
     *  @brief 判断读取是否成功完成
     */
    virtual bool SuccessfullyFinished() {
        return off_ >= fileLen_;
    }

 private:
    // 文件描述符
    int fd_;
    // 下一个Entry的偏移
    int64_t off_;
    // 文件长度
    int64_t fileLen_;

    std::shared_ptr<LocalFileSystem> localFS_;
};

class RaftLogTool : public CurveTool {
 public:
    explicit RaftLogTool(std::shared_ptr<SegmentParser> parser) :
                                    parser_(parser) {}

    /**
     *  @brief 执行命令
     *  @param command 要执行的命令
     *  @return 成功返回0，失败返回-1
    */
    int RunCommand(const std::string& command) override;

    /**
     *  @brief 打印帮助信息
    */
    void PrintHelp(const std::string& command) override;

    /**
     *  @brief 返回是否支持该命令
     *  @param command：执行的命令
     *  @return true / false
     */
    static bool SupportCommand(const std::string& command);

 private:
    /**
     *  @brief 打印文件中所有raft log的头部信息
     *  @param fileName raft log文件名
     *  @return 成功返回0，否则返回-1
     */
    int PrintHeaders(const std::string& fileName);

    /**
     *  @brief 从文件解析出entry header
     *  @param fd 文件描述符
     *  @param offset 文件中的偏移
     *  @param[out] head entry头部信息，返回值为0时有效
     *  @return 成功返回0，否则返回-1
     */
    int ParseEntryHeader(int fd, off_t offset, EntryHeader *head);

    /**
     *  @brief 从文件名解析first index
     *  @param fileName raft log文件名
     *  @param[out] firstIndex segment文件包含的log entry的第一个index
     *  @return 成功返回0，否则返回-1
     */
    int ParseFirstIndexFromFileName(const std::string& fileName,
                                    int64_t* firstIndex);

    std::shared_ptr<SegmentParser> parser_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_RAFT_LOG_TOOL_H_
