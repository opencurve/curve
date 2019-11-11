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
#include <time.h>

#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include <cstdint>
#include <cstring>

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/common/timeutility.h"
#include "src/common/authenticator.h"
#include "src/common/string_util.h"
#include "src/mds/common/mds_define.h"

using curve::mds::FileInfo;
using curve::mds::PageFileSegment;
using curve::mds::StatusCode;
using curve::mds::PageFileChunkInfo;
using curve::mds::topology::kTopoErrCodeSuccess;
using curve::common::Authenticator;

namespace curve {
namespace tool {

enum class GetSegmentRes {
    kOK = 0,   // 获取segment成功
    kSegmentNotAllocated = -1,  // segment不存在
    kOtherError = -2  // 其他错误
};

class NameSpaceTool {
 public:
    NameSpaceTool();
    ~NameSpaceTool();

    /**
     *  @brief 初始化channel
     *  @param mdsAddr mds的地址，支持多地址，用","分隔
     *  @return 无
     */
    int Init(const std::string& mdsAddr);

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
    int PrintFileInfoAndActualSize(std::string fileName);

    // 从绝对路径中解析出parentid和fileName然后获取fileInfo
    int GetFileInfo(const std::string &fileName, FileInfo* fileInfo);

    // 计算文件或目录实际占用的空间
    int64_t GetActualSize(const FileInfo& fileInfo);

    // 打印目录中的文件信息
    int PrintListDir(std::string dirName);

    // 列出目录中的文件信息并输出到files里面
    int ListDir(const std::string& dirName, std::vector<FileInfo>* files);

    // 打印出文件的segment信息
    int PrintSegmentInfo(const std::string &fileName);

    // 获取文件的segment信息并输出到segments里面
    int GetFileSegments(const FileInfo &fileInfo,
                       std::vector<PageFileSegment>* segments);

    // 获取指定偏移的segment放到segment里面
    GetSegmentRes GetSegmentInfo(std::string fileName,
                                 uint64_t offset,
                                 PageFileSegment* segment);

    // 删除文件
    int DeleteFile(const std::string& fileName, bool forcedelete);

    // 清空回收站
    int CleanRecycleBin();

    // 创建文件
    int CreateFile(const std::string& fileName);

    // 根据文件名和offset查询chunk位置
    int QueryChunkLocation(const std::string& fileName, uint64_t offset);

    // 填充signature
    template <class T>
    void FillUserInfo(T* request);

    // 打印fileInfo，把时间转化为易读的格式输出
    void PrintFileInfo(const FileInfo& fileInfo);

    // 打印PageFileSegment，把同一个chunk的信息打在同一行
    void PrintSegment(const PageFileSegment& segment);

    // 打印copyset里面的chunkserver
    int PrintCopysetMembers(uint32_t logicalPoolId,
                            uint32_t copysetId);

    // 向mds发送RPC的channel
    brpc::Channel* channel_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_NAMESPACE_TOOL_H_
