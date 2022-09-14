/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2019-09-25
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include "src/tools/namespace_tool.h"

#include <algorithm>

#include "absl/strings/str_format.h"

DEFINE_string(fileName, "", "file name");
DEFINE_string(dirName, "", "directory name");
DEFINE_bool(forcedelete, false, "force delete file or not");
DEFINE_uint64(fileLength, 20, "file length (GB)");
DEFINE_uint64(newSize, 30, "the new size of expanded volume(GB)");
DEFINE_string(poolset, "", "specify the poolset name");
DEFINE_bool(isTest, false, "is unit test or not");
DEFINE_uint64(offset, 0, "offset to query chunk location");
DEFINE_uint64(rpc_timeout, 3000, "millisecond for rpc timeout");
DEFINE_bool(showAllocSize, true, "If specified, the allocated size will not be computed");  // NOLINT
DEFINE_bool(showFileSize, true, "If specified, the file size will not be computed");  // NOLINT
DECLARE_string(mdsAddr);
DEFINE_bool(showAllocMap, false, "If specified, the allocated size in each"
                                 " logical pool will be print");

DEFINE_uint64(stripeUnit, 0, "stripe unit size");
DEFINE_uint64(stripeCount, 0, "strip count");

namespace curve {
namespace tool {

int NameSpaceTool::Init() {
    if (!inited_) {
        int res = core_->Init(FLAGS_mdsAddr);
        if (res != 0) {
            std::cout << "Init nameSpaceToolCore fail!" << std::endl;
            return -1;
        }
        inited_ = true;
    }
    return 0;
}

bool NameSpaceTool::SupportCommand(const std::string& command) {
    return (command == kGetCmd || command == kListCmd
                               || command == kSegInfoCmd
                               || command == kDeleteCmd
                               || command == kCreateCmd
                               || command == kExtendCmd
                               || command == kCleanRecycleCmd
                               || command == kChunkLocatitonCmd
                               || command == kListPoolsets);
}

// 根据命令行参数选择对应的操作
int NameSpaceTool::RunCommand(const std::string &cmd) {
    if (Init() != 0) {
        std::cout << "Init NameSpaceTool failed" << std::endl;
        return -1;
    }
    std::string fileName = FLAGS_fileName;
    TrimEndingSlash(&fileName);
    if (cmd == kGetCmd) {
        return PrintFileInfoAndActualSize(fileName);
    } else if (cmd == kListCmd) {
        return PrintListDir(fileName);
    } else if (cmd == kSegInfoCmd) {
        return PrintSegmentInfo(fileName);
    } else if (cmd == kDeleteCmd) {
        // 单元测试不判断输入
        if (FLAGS_isTest) {
            return core_->DeleteFile(fileName, FLAGS_forcedelete);
        }
        std::cout << "Are you sure you want to delete "
                  << fileName << "?" << "(yes/no)" << std::endl;
        std::string str;
        std::cin >> str;
        if (str == "yes") {
            return core_->DeleteFile(fileName, FLAGS_forcedelete);
        } else {
            std::cout << "Delete cancled!" << std::endl;
            return 0;
        }
    } else if (cmd == kCleanRecycleCmd) {
        if (FLAGS_isTest) {
            return core_->CleanRecycleBin(fileName);
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
    } else if (cmd == kCreateCmd) {
        if (!FLAGS_dirName.empty() && !FLAGS_fileName.empty()) {
            std::cout << "Parameter error: dirName and fileName "
                      << "can't be set at the same time!" << std::endl;
            return -1;
        }
        bool normalFile = FLAGS_dirName.empty();
        CreateFileContext context;
        context.type = normalFile ? curve::mds::FileType::INODE_PAGEFILE
                                  : curve::mds::FileType::INODE_DIRECTORY;
        context.name = normalFile ? FLAGS_fileName : FLAGS_dirName;
        if (normalFile) {
            context.length = FLAGS_fileLength * mds::kGB;
            context.stripeUnit = FLAGS_stripeUnit;
            context.stripeCount = FLAGS_stripeCount;
            context.poolset = FLAGS_poolset;
        }

        return core_->CreateFile(context);
    } else if (cmd == kExtendCmd) {
        return core_->ExtendVolume(fileName, FLAGS_newSize * mds::kGB);
    } else if (cmd == kChunkLocatitonCmd) {
        return PrintChunkLocation(fileName, FLAGS_offset);
    } else if (cmd == kListPoolsets) {
        return PrintPoolsets();
    } else {
        std::cout << "Command not support!" << std::endl;
        return -1;
    }
}

void NameSpaceTool::PrintHelp(const std::string &cmd) {
    std::cout << "Example: " << std::endl;
    if (cmd == kGetCmd || cmd == kListCmd) {
        std::cout << "curve_ops_tool " << cmd << " -fileName=/test [-mdsAddr=127.0.0.1:6666]"  // NOLINT
                            " [-showAllocSize=false] [-showFileSize=false] [-confPath=/etc/curve/tools.conf]" << std::endl;  // NOLINT
    } else if (cmd == kSegInfoCmd) {
        std::cout << "curve_ops_tool " << cmd << " -fileName=/test [-mdsAddr=127.0.0.1:6666] [-confPath=/etc/curve/tools.conf]" << std::endl;  // NOLINT
    } else if (cmd == kCleanRecycleCmd) {
        std::cout << "curve_ops_tool " << cmd << " [-fileName=/cinder] [-mdsAddr=127.0.0.1:6666] [-confPath=/etc/curve/tools.conf]" << std::endl;  // NOLINT
        std::cout << "If -fileName is specified, delete the files in recyclebin that the original directory is fileName" << std::endl;  // NOLINT
    } else if (cmd == kCreateCmd) {
        std::cout << "curve_ops_tool " << cmd << " -fileName=/test -userName=test -password=123 -fileLength=20 [--poolset=default] [-stripeUnit=32768] [-stripeCount=32]  [-mdsAddr=127.0.0.1:6666] [-confPath=/etc/curve/tools.conf]" << std::endl;  // NOLINT
        std::cout << "curve_ops_tool " << cmd << " -dirName=/dir -userName=test -password=123 [-mdsAddr=127.0.0.1:6666] [-confPath=/etc/curve/tools.conf]" << std::endl;  // NOLINT
        std::cout << "The first example can create a volume and the second create a directory." << std::endl;  // NOLINT
    } else if (cmd == kExtendCmd) {
        std::cout << "curve_ops_tool " << cmd << " -fileName=/test -userName=test -password=123 -newSize=30  [-mdsAddr=127.0.0.1:6666] [-confPath=/etc/curve/tools.conf]" << std::endl;  // NOLINT
    } else if (cmd == kDeleteCmd) {
        std::cout << "curve_ops_tool " << cmd << " -fileName=/test -userName=test -password=123 -forcedelete=true  [-mdsAddr=127.0.0.1:6666] [-confPath=/etc/curve/tools.conf]" << std::endl;  // NOLINT
    } else if (cmd == kChunkLocatitonCmd) {
        std::cout << "curve_ops_tool " << cmd << " -fileName=/test -offset=16777216 [-mdsAddr=127.0.0.1:6666] [-confPath=/etc/curve/tools.conf]" << std::endl;  // NOLINT
    } else {
        std::cout << "command not found!" << std::endl;
    }
}

int NameSpaceTool::PrintFileInfoAndActualSize(const std::string& fileName) {
    FileInfo fileInfo;
    int ret = core_->GetFileInfo(fileName, &fileInfo);
    if (ret != 0) {
        return -1;
    }
    return PrintFileInfoAndActualSize(fileName, fileInfo);
}

int NameSpaceTool::PrintFileInfoAndActualSize(const std::string& fullName,
                                              const FileInfo& fileInfo) {
    PrintFileInfo(fileInfo);
    int ret = GetAndPrintAllocSize(fullName);
    // 如果是目录的话，计算目录中的文件大小(用户创建时指定的)
    if (fileInfo.filetype() == curve::mds::FileType::INODE_DIRECTORY) {
        ret = GetAndPrintFileSize(fullName);
    }
    return ret;
}

int NameSpaceTool::GetAndPrintFileSize(const std::string& fileName) {
    if (!FLAGS_showFileSize) {
        return 0;
    }
    uint64_t size;
    int res = core_->GetFileSize(fileName, &size);
    if (res != 0) {
        std::cout << "Get file size fail!" << std::endl;
        return -1;
    }
    double fileSize = static_cast<double>(size) / curve::mds::kGB;
    std::cout << "file size: " << fileSize << "GB" << std::endl;
    return 0;
}

int NameSpaceTool::GetAndPrintAllocSize(const std::string& fileName) {
    if (!FLAGS_showAllocSize) {
        return 0;
    }
    uint64_t size;
    AllocMap allocMap;
    int res = core_->GetAllocatedSize(fileName, &size, &allocMap);
    if (res != 0) {
        std::cout << "Get allocated size fail!" << std::endl;
        return -1;
    }
    double allocSize = static_cast<double>(size) / curve::mds::kGB;
    std::cout << "allocated size: " << allocSize << "GB" << std::endl;
    if (FLAGS_showAllocMap) {
        for (const auto& item : allocMap) {
            allocSize = static_cast<double>(item.second) / curve::mds::kGB;
            std::cout << "logical pool id: " << item.first
                      << ", allocated size: " << allocSize << "GB" << std::endl;
        }
    }
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
        // 把length转换成GB
        if (item.compare(0, 6, "length") == 0) {
            uint64_t length = fileInfo.length();
            double fileSize = static_cast<double>(length) / curve::mds::kGB;
            std::cout << "length: " << fileSize << "GB" << std::endl;
            continue;
        }
        std::cout << item << std::endl;
    }
}

int NameSpaceTool::PrintListDir(const std::string& dirName) {
    std::vector<FileInfo> files;
    int ret = core_->ListDir(dirName, &files);
    if (ret != 0) {
        std::cout << "List directory failed!" << std::endl;
        return -1;
    }
    for (uint64_t i = 0; i < files.size(); ++i) {
        if (i != 0) {
            std::cout << std::endl;
        }
        std::string fullPathName;
        if (dirName == "/") {
            fullPathName = dirName + files[i].filename();
        } else {
            fullPathName = dirName + "/" + files[i].filename();
        }
        if (PrintFileInfoAndActualSize(fullPathName, files[i]) != 0) {
            ret = -1;
        }
    }
    if (!files.empty()) {
        std::cout << std::endl;
    }
    std::cout << "Total file number: " << files.size() << std::endl;
    return ret;
}

int NameSpaceTool::PrintPoolsets() {
    std::vector<PoolsetInfo> poolsets;
    if (core_->ListPoolset(&poolsets) != 0) {
        std::cout << "List poolset fail!" << std::endl;
        return -1;
    }

    std::sort(poolsets.begin(), poolsets.end(),
              [](const PoolsetInfo& a, const PoolsetInfo& b) {
                  return a.poolsetid() < b.poolsetid();
              });

    for (const auto& poolset : poolsets) {
        const std::string str = absl::StrFormat(
                "id: %3d, name: %s, type: %s, desc: `%s`", poolset.poolsetid(),
                poolset.poolsetname(), poolset.type(), poolset.desc());
        std::cout << str << std::endl;
    }

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
    int res = core_->GetChunkServerListInCopySet(logicPoolId,
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

void NameSpaceTool::TrimEndingSlash(std::string* fileName) {
    // 如果最后面有/，去掉
    if (fileName->size() > 1 && fileName->back() == '/') {
        fileName->pop_back();
    }
}
}  // namespace tool
}  // namespace curve
