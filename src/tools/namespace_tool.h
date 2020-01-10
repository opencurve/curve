/*
 * Project: curve
 * Created Date: 2019-09-25
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_NAMESPACE_TOOL_H_
#define SRC_TOOLS_NAMESPACE_TOOL_H_

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
#include "src/tools/namespace_tool_core.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

using curve::mds::FileInfo;
using curve::mds::PageFileSegment;
using curve::mds::StatusCode;

namespace curve {
namespace tool {

class NameSpaceTool : public CurveTool {
 public:
    explicit NameSpaceTool(std::shared_ptr<NameSpaceToolCore> core) :
                              core_(core), inited_(false) {}

    /**
     *  @brief 打印用法
     *  @param command：查询的命令
     *  @return 无
     */
    void PrintHelp(const std::string &command) override;

    /**
     *  @brief 执行命令
     *  @param command：执行的命令
     *  @return 成功返回0，失败返回-1
     */
    int RunCommand(const std::string &command) override;

    /**
     *  @brief 返回是否支持该命令
     *  @param command：执行的命令
     *  @return true / false
     */
    static bool SupportCommand(const std::string& command);

 private:
    // 初始化
    int Init();
    // 打印fileInfo和文件占用的实际空间
    int PrintFileInfoAndActualSize(const std::string& fileName);

    // 打印fileInfo和文件占用的实际空间
    int PrintFileInfoAndActualSize(const std::string& fullName,
                                   const FileInfo& fileInfo);

    // 打印目录中的文件信息
    int PrintListDir(const std::string& dirName);

    // 打印出文件的segment信息
    int PrintSegmentInfo(const std::string &fileName);

    // 打印fileInfo，把时间转化为易读的格式输出
    void PrintFileInfo(const FileInfo& fileInfo);

    // 打印PageFileSegment，把同一个chunk的信息打在同一行
    void PrintSegment(const PageFileSegment& segment);

    // 打印chunk的位置信息
    int PrintChunkLocation(const std::string& fileName,
                                     uint64_t offset);

    // 打印文件的分配大小
    int GetAndPrintAllocSize(const std::string& fileName);

    // 打印目录的file size
    int GetAndPrintFileSize(const std::string& fileName);

    // 目前curve mds不支持/test/格式的文件名，需要把末尾的/去掉
    void TrimEndingSlash(std::string* fileName);

 private:
    // 核心逻辑
    std::shared_ptr<NameSpaceToolCore> core_;
    // 是否初始化成功过
    bool inited_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_NAMESPACE_TOOL_H_
