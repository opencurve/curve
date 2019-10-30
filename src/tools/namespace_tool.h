/*
 * Project: curve
 * Created Date: 2019-09-25
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_NAMESPACE_TOOL_H_
#define SRC_TOOLS_NAMESPACE_TOOL_H_

#include <gflags/gflags.h>
#include <brpc/channel.h>

#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include <cstdint>
#include <cstring>

#include "proto/nameserver2.pb.h"
#include "src/common/timeutility.h"
#include "src/common/authenticator.h"

using curve::mds::FileInfo;
using curve::mds::PageFileSegment;
using curve::mds::StatusCode;
using curve::common::Authenticator;

namespace curve {
namespace tool {
class NameSpaceTool {
 public:
    NameSpaceTool() {}
    ~NameSpaceTool() = default;

    /**
     *  @brief 初始化channel
     *  @param 无
     *  @return 无
     */
    int Init();

    /**
     *  @brief 打印用法
     *  @param cmd：查询的命令
     *  @return 无
     */
    void PrintHelp(const std::string &cmd);

    /**
     *  @brief 执行命令
     *  @param cmd：执行的命令
     *  @param filename: 要操作的文件名
     *  @return 成功返回0，失败返回-1
     */
    int RunCommand(const std::string &cmd);

 private:
    // 打印fileInfo和文件占用的实际空间
    int PrintFileInfoAndActualSize(const std::string &fileName);

    // 从绝对路径中解析出parentid和fileName然后获取fileInfo
    int GetFileInfo(const std::string &fileName, FileInfo* fileInfo);

    // 计算文件或目录实际占用的空间
    int64_t GetActualSize(const FileInfo& fileInfo);

    // 打印目录中的文件信息
    int PrintListDir(const std::string &dirName);

    // 列出目录中的文件信息并输出到files里面
    int ListDir(const std::string& dirName, std::vector<FileInfo>* files);

    // 打印出文件的segment信息
    int PrintSegmentInfo(const std::string &fileName);

    // 获取文件的segment信息并输出到segments里面
    int GetSegmentInfo(const FileInfo &fileInfo,
                            std::vector<PageFileSegment>* segments);

    // 删除文件
    int DeleteFile(const std::string& fileName, bool forcedelete);

    // 清空回收站
    int CleanRecycleBin();

    // 创建文件
    int CreateFile(const std::string& fileName);

    // 填充signature
    template <class T>
    void FillUserInfo(T* request);

    brpc::Channel channel_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_NAMESPACE_TOOL_H_
