/*
 * Project: curve
 * Created Date: 2019-09-25
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include "src/tools/namespace_tool.h"

DEFINE_string(fileName, "", "file name");
DEFINE_bool(forcedelete, false, "force delete file or not");
DEFINE_uint64(fileLength, 20*1024*1024*1024ull, "file length");
DEFINE_bool(isTest, false, "is unit test or not");
DEFINE_uint64(offset, 0, "offset to query chunk location");
DEFINE_uint64(rpc_timeout, 3000, "millisecond for rpc timeout");
DEFINE_bool(noSize, false, "If specified, the allocated size will not be computed");  // NOLINT

namespace curve {
namespace tool {

// 根据命令行参数选择对应的操作
int NameSpaceTool::RunCommand(const std::string &cmd) {
    if (cmd == "get") {
        return PrintFileInfoAndActualSize(FLAGS_fileName);
    } else if (cmd == "list") {
        return PrintListDir(FLAGS_fileName);
    } else if (cmd == "seginfo") {
        return PrintSegmentInfo(FLAGS_fileName);
    } else if (cmd == "delete") {
        // 单元测试不判断输入
        if (FLAGS_isTest) {
            return core_->DeleteFile(FLAGS_fileName, FLAGS_forcedelete);
        }
        std::cout << "Are you sure you want to delete "
                  << FLAGS_fileName << "?" << "(yes/no)" << std::endl;
        std::string str;
        std::cin >> str;
        if (str == "yes") {
            return core_->DeleteFile(FLAGS_fileName, FLAGS_forcedelete);
        } else {
            std::cout << "Delete cancled!" << std::endl;
            return 0;
        }
    } else if (cmd == "clean-recycle") {
        if (FLAGS_isTest) {
            return core_->CleanRecycleBin(FLAGS_fileName);
        }
        std::cout << "Are you sure you want to clean the RecycleBin?"
                  << "(yes/no)" << std::endl;
        std::string str;
        std::cin >> str;
        if (str == "yes") {
            return core_->CleanRecycleBin();;
        } else {
            std::cout << "Clean RecycleBin cancled!" << std::endl;
            return 0;
        }
    } else if (cmd == "create") {
        return core_->CreateFile(FLAGS_fileName, FLAGS_fileLength);
    } else if (cmd == "chunk-location") {
        return PrintChunkLocation(FLAGS_fileName, FLAGS_offset);
    } else {
        std::cout << "Command not support!" << std::endl;
        PrintHelp("get");
        PrintHelp("list");
        PrintHelp("seginfo");
        PrintHelp("delete");
        PrintHelp("clean-recycle");
        PrintHelp("create");
        PrintHelp("chunk-location");
        return -1;
    }
}

void NameSpaceTool::PrintHelp(const std::string &cmd) {
    std::cout << "Example: " << std::endl;
    if (cmd == "get" || cmd == "list") {
        std::cout << "curve_ops_tool " << cmd << " -mdsAddr=127.0.0.1:6666 -fileName=/test [-noSize]" << std::endl;  // NOLINT
    } else if (cmd == "seginfo") {
        std::cout << "curve_ops_tool " << cmd << " -mdsAddr=127.0.0.1:6666 -fileName=/test" << std::endl;  // NOLINT
    } else if (cmd == "clean-recycle") {
        std::cout << "curve_ops_tool " << cmd << " -mdsAddr=127.0.0.1:6666 [-fileName=/cinder]" << std::endl;  // NOLINT
        std::cout << "If -fileName is specified, delete the files in recyclebin that the original directory is fileName" << std::endl;  // NOLINT
    } else if (cmd == "create") {
        std::cout << "curve_ops_tool " << cmd << " -mdsAddr=127.0.0.1:6666 -fileName=/test -userName=test -password=123 -fileLength=21474836480‬" << std::endl;  // NOLINT
    } else if (cmd == "delete") {
        std::cout << "curve_ops_tool " << cmd << " -mdsAddr=127.0.0.1:6666 -fileName=/test -userName=test -password=123 -forcedelete=true" << std::endl;  // NOLINT
    } else if (cmd == "chunk-location") {
        std::cout << "curve_ops_tool " << cmd << " -mdsAddr=127.0.0.1:6666 -fileName=/test -offset=16777216" << std::endl;  // NOLINT
    } else {
        std::cout << "command not found!" << std::endl;
    }
}

int NameSpaceTool::PrintFileInfoAndActualSize(std::string fileName) {
    // 如果最后面有/，去掉
    if (fileName.size() > 1 && fileName.back() == '/') {
        fileName.pop_back();
    }
    FileInfo fileInfo;
    auto ret = core_->GetFileInfo(fileName, &fileInfo);
    if (ret != 0) {
        return -1;
    }
    std::cout << "File info:" << std::endl;
    PrintFileInfo(fileInfo);
    if (FLAGS_noSize) {
        return 0;
    }
    uint64_t size;
    if (core_->GetAllocatedSize(fileName, &size) != 0) {
        std::cout << "Get allocated size fail!" << std::endl;
        return -1;
    }
    double res = static_cast<double>(size) / (1024 * 1024 * 1024);
    std::cout << "allocated size: " << res << "GB" << std::endl;
    return 0;
}

void NameSpaceTool::PrintFileInfo(const FileInfo& fileInfo) {
    std::string fileInfoStr = fileInfo.DebugString();
    std::vector<std::string> items;
    curve::common::SplitString(fileInfoStr, "\n", &items);
    for (const auto& item : items) {
        if (item.compare(0, 5, "ctime") == 0) {
            // ctime是微妙，打印的时候只打印到秒
            time_t ctime = fileInfo.ctime() / 1000000;
            std::string standard;
            curve::common::TimeUtility::TimeStampToStandard(ctime, &standard);
            std::cout << "ctime: " << standard << std::endl;
            continue;
        }
        std::cout << item << std::endl;
    }
}

int NameSpaceTool::PrintListDir(std::string dirName) {
    // 如果最后面有/，去掉
    if (dirName.size() > 1 && dirName.back() == '/') {
        dirName.pop_back();
    }
    std::vector<FileInfo> files;
    if (core_->ListDir(dirName, &files) != 0) {
        std::cout << "List directory failed!" << std::endl;
        return -1;
    }
    for (uint64_t i = 0; i < files.size(); ++i) {
        if (i != 0) {
            std::cout << std::endl;
        }
        PrintFileInfo(files[i]);
        if (FLAGS_noSize) {
            continue;
        }
        std::string fullPathName;
        if (dirName == "/") {
            fullPathName = dirName + files[i].filename();
        } else {
            fullPathName = dirName + "/" + files[i].filename();
        }
        uint64_t size;
        int res = core_->GetAllocatedSize(fullPathName, &size);
        if (res != 0) {
            std::cout << "Get allocated size of " << fullPathName
                      <<" fail!" << std::endl;
            continue;
        }
        double allocSize = static_cast<double>(size) / mds::kGB;
        std::cout << "allocated size: " << allocSize << "GB" << std::endl;
    }
    if (!files.empty()) {
        std::cout << std::endl;
    }
    std::cout << "Total file number: " << files.size() << std::endl;
    return 0;
}

int NameSpaceTool::PrintSegmentInfo(const std::string &fileName) {
    std::vector<PageFileSegment> segments;
    if (core_->GetFileSegments(fileName, &segments) != 0) {
        std::cout << "GetFileSegments fail!" << std::endl;
        return -1;
    }
    for (auto& segment : segments) {
        PrintSegment(segment);
    }
    return 0;
}

void NameSpaceTool::PrintSegment(const PageFileSegment& segment) {
    if (segment.has_logicalpoolid()) {
        std::cout << "logicalPoolID: " << segment.logicalpoolid() << std::endl;
    }
    if (segment.has_startoffset()) {
        std::cout << "startOffset: " << segment.startoffset() << std::endl;
    }
    if (segment.has_segmentsize()) {
        std::cout << "segmentSize: " << segment.segmentsize() << std::endl;
    }
    if (segment.has_chunksize()) {
        std::cout << "chunkSize: " << segment.chunksize() << std::endl;
    }
    std::cout << "chunks: " << std::endl;
    for (int i = 0; i < segment.chunks_size(); ++i) {
        uint64_t chunkId = 0;
        uint32_t copysetId = 0;
        if (segment.chunks(i).has_chunkid()) {
            chunkId = segment.chunks(i).chunkid();
        }
        if (segment.chunks(i).has_copysetid()) {
            copysetId = segment.chunks(i).copysetid();
        }
        std::cout << "chunkID: " << chunkId << ", copysetID: "
                                 << copysetId << std::endl;
    }
}


int NameSpaceTool::PrintChunkLocation(const std::string& fileName,
                                     uint64_t offset) {
    uint64_t chunkId;
    std::pair<uint32_t, uint32_t> copyset;
    if (core_->QueryChunkCopyset(fileName, offset, &chunkId, &copyset) != 0) {
        std::cout << "QueryChunkCopyset fail!" << std::endl;
        return -1;
    }
    uint32_t logicPoolId = copyset.first;
    uint32_t copysetId = copyset.second;
    uint64_t groupId = (static_cast<uint64_t>(logicPoolId) << 32) | copysetId;
    std::cout << "chunkId: " << chunkId
              << ", logicalPoolId: " << logicPoolId
              << ", copysetId: " << copysetId
              << ", groupId: " << groupId << std::endl;
    std::vector<ChunkServerLocation> csLocs;
    int res = core_->GetChunkServerListInCopySets(logicPoolId,
                                    copysetId, &csLocs);
    if (res != 0) {
        std::cout << "GetChunkServerListInCopySet fail!" << std::endl;
        return -1;
    }
    std::cout << "location: {";
    for (uint64_t i = 0; i < csLocs.size(); ++i) {
        if (i != 0) {
            std::cout << ", ";
        }
        auto location = csLocs[i];
        std::cout << location.hostip() << ":"
                  << std::to_string(location.port());
    }
    std::cout << "}" << std::endl;
    return 0;
}
}  // namespace tool
}  // namespace curve
