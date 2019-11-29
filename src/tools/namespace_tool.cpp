/*
 * Project: curve
 * Created Date: 2019-09-25
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include "src/tools/namespace_tool.h"

DEFINE_string(fileName, "", "file name");
DEFINE_string(userName, "root", "owner of the file");
DEFINE_string(password, "root_password", "password of administrator");
DEFINE_bool(forcedelete, false, "force delete file or not");
DEFINE_uint64(fileLength, 20*1024*1024*1024ull, "file length");
DEFINE_bool(isTest, false, "is unit test or not");
DEFINE_uint64(offset, 0, "offset to query chunk location");
DEFINE_uint64(rpc_timeout, 3000, "millisecond for rpc timeout");

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
            return DeleteFile(FLAGS_fileName, FLAGS_forcedelete);
        }
        std::cout << "Are you sure you want to delete "
                  << FLAGS_fileName << "?" << "(yes/no)" << std::endl;
        std::string str;
        std::cin >> str;
        if (str == "yes") {
            return DeleteFile(FLAGS_fileName, FLAGS_forcedelete);
        } else {
            std::cout << "Delete cancled!" << std::endl;
            return 0;
        }
    } else if (cmd == "clean-recycle") {
        if (FLAGS_isTest) {
            return CleanRecycleBin();
        }
        std::cout << "Are you sure you want to clean the RecycleBin?"
                  << "(yes/no)" << std::endl;
        std::string str;
        std::cin >> str;
        if (str == "yes") {
            return CleanRecycleBin();;
        } else {
            std::cout << "Clean RecycleBin cancled!" << std::endl;
            return 0;
        }
    } else if (cmd == "create") {
        return CreateFile(FLAGS_fileName);
    } else if (cmd == "chunk-location") {
        return QueryChunkLocation(FLAGS_fileName, FLAGS_offset);
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

int NameSpaceTool::Init(const std::string& mdsAddr) {
    // 初始化channel
    std::vector<std::string> mdsAddrVec;
    curve::common::SplitString(mdsAddr, ",", &mdsAddrVec);
    if (mdsAddrVec.empty()) {
        std::cout << "Split mds address fail!" << std::endl;
        return -1;
    }
    for (const auto& mdsAddr : mdsAddrVec) {
        if (channel_->Init(mdsAddr.c_str(), nullptr) != 0) {
            continue;
        }
        // 寻找哪个mds存活
        curve::mds::GetFileInfoRequest request;
        curve::mds::GetFileInfoResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpc_timeout);
        request.set_filename("/");
        FillUserInfo(&request);
        curve::mds::CurveFSService_Stub stub(channel_);
        stub.GetFileInfo(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            // 多mds，前两个mds失败不应该打印错误
            continue;
        }
        return 0;
    }
    std::cout << "Init channel to all mds fail!" << std::endl;
    return -1;
}

NameSpaceTool::NameSpaceTool() {
    channel_ = new (std::nothrow) brpc::Channel();
}

NameSpaceTool::~NameSpaceTool() {
    delete channel_;
    channel_ = nullptr;
}

void NameSpaceTool::PrintHelp(const std::string &cmd) {
    std::cout << "Example: " << std::endl;
    if (cmd == "get" || cmd == "list" || cmd == "seginfo") {
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
    auto ret = GetFileInfo(fileName, &fileInfo);
    if (ret != 0) {
        return -1;
    }
    std::cout << "File info:" << std::endl;
    PrintFileInfo(fileInfo);
    fileInfo.set_originalfullpathname(fileName);
    int64_t size = GetActualSize(fileInfo);
    if (size < 0) {
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

int NameSpaceTool::GetFileInfo(const std::string &fileName,
                               FileInfo* fileInfo) {
    curve::mds::GetFileInfoRequest request;
    curve::mds::GetFileInfoResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpc_timeout);
    request.set_filename(fileName);
    FillUserInfo(&request);

    curve::mds::CurveFSService_Stub stub(channel_);
    stub.GetFileInfo(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        std::cout<< "Get file fail, errCode = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
        return -1;
    }

    if (response.has_statuscode()) {
        if (response.statuscode() == StatusCode::kOK) {
            *fileInfo = response.fileinfo();
            return 0;
        } else {
            std::cout << "Get file fail, errCode: "
                      << response.statuscode() << std::endl;
        }
    }
    return -1;
}

int64_t NameSpaceTool::GetActualSize(const FileInfo& fileInfo) {
    int64_t size = 0;
    // 如果是文件的话，直接获取segment信息，然后计算空间即可
    if (fileInfo.filetype() != curve::mds::FileType::INODE_DIRECTORY) {
        std::vector<PageFileSegment> segments;
        if (GetFileSegments(fileInfo, &segments) != 0) {
            std::cout << "Get segment info fail, parent id: "
                      << fileInfo.parentid()
                      << " filename: " << fileInfo.filename()
                      << std::endl;
            return -1;
        }
        for (auto& segment : segments) {
            int64_t chunkSize = segment.chunksize();
            int64_t chunkNum = segment.chunks().size();
            size += chunkNum * chunkSize;
        }
        return size;
    } else {  // 如果是目录，则list dir，并递归计算每个文件的大小最后加起来
        std::vector<FileInfo> files;
        if (ListDir(fileInfo.originalfullpathname(), &files) != 0) {
            std::cout << "List directory failed!" << std::endl;
            return -1;
        }
        for (auto& file : files) {
            std::string fullPathName;
            if (fileInfo.filename() == "/") {
                fullPathName = fileInfo.originalfullpathname()
                                            + file.filename();
            } else {
                fullPathName = fileInfo.originalfullpathname()
                                            + "/" + file.filename();
            }
            file.set_originalfullpathname(fullPathName);
            int64_t tmp = GetActualSize(file);
            if (tmp < 0) {
                std::cout << "Get allocated size fail!" << std::endl;
                return -1;
            }
            size += tmp;
        }
        return size;
    }
}

int NameSpaceTool::PrintListDir(std::string dirName) {
    // 如果最后面有/，去掉
    if (dirName.size() > 1 && dirName.back() == '/') {
        dirName.pop_back();
    }
    std::vector<FileInfo> files;
    if (ListDir(dirName, &files) != 0) {
        std::cout << "List directory failed!" << std::endl;
        return -1;
    }
    for (int i = 0; i < files.size(); ++i) {
        if (i != 0) {
            std::cout << std::endl;
        }
        PrintFileInfo(files[i]);
        if (dirName == "/") {
            files[i].set_originalfullpathname(dirName + files[i].filename());
        } else {
            files[i].set_originalfullpathname(dirName +
                                            "/" + files[i].filename());
        }
        int64_t size = GetActualSize(files[i]);
        if (size < 0) {
            std::cout << "Get allocated size fail!" << std::endl;
            return -1;
        }
        double res = static_cast<double>(size) / (1024 * 1024 * 1024);
        std::cout << "allocated size: " << res << "GB" << std::endl;
    }
    return 0;
}

int NameSpaceTool::ListDir(const std::string& dirName,
                           std::vector<FileInfo>* files) {
    curve::mds::ListDirRequest request;
    curve::mds::ListDirResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpc_timeout);
    request.set_filename(dirName);
    FillUserInfo(&request);

    curve::mds::CurveFSService_Stub stub(channel_);
    stub.ListDir(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        std::cout<< "List directory fail, errCde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
        return -1;
    }

    if (response.has_statuscode()) {
        if (response.statuscode() == StatusCode::kOK) {
            int fileinfoNum = response.fileinfo_size();
            for (int i = 0; i < fileinfoNum; i++) {
                files->push_back(response.fileinfo(i));
            }
            return 0;
        } else {
            std::cout << "List directory fail, errCode: "
                      << response.statuscode() << std::endl;
        }
    }
    return -1;
}

int NameSpaceTool::PrintSegmentInfo(const std::string &fileName) {
    FileInfo fileInfo;
    int ret = GetFileInfo(fileName, &fileInfo);
    if (ret != 0) {
        std::cout << "Get file info failed!" << std::endl;
        return -1;
    }
    fileInfo.set_originalfullpathname(fileName);
    std::vector<PageFileSegment> segments;
    if (GetFileSegments(fileInfo, &segments) != 0) {
        std::cout << "Get segment info fail!" << std::endl;
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

int NameSpaceTool::GetFileSegments(const FileInfo &fileInfo,
                                  std::vector<PageFileSegment>* segments) {
    // 只能获取page file的segment
    if (fileInfo.filetype() != curve::mds::FileType::INODE_PAGEFILE) {
        std::cout << "It is not a page file!" << std::endl;
        return -1;
    }

    // 获取文件的segment数，并打印每个segment的详细信息
    int segmentNum = fileInfo.length() / fileInfo.segmentsize();
    uint64_t segmentSize = fileInfo.segmentsize();
    std::string fileName = fileInfo.originalfullpathname();
    for (uint64_t i = 0; i < segmentNum; i++) {
        // load  segment
        PageFileSegment segment;
        GetSegmentRes res = GetSegmentInfo(fileName,
                                    i * segmentSize, &segment);
        if (res == GetSegmentRes::kOK) {
            segments->emplace_back(segment);
        } else if (res == GetSegmentRes::kSegmentNotAllocated) {
            continue;
        } else {
            std::cout << "Get segment info from mds fail!" << std::endl;
            return -1;
        }
    }
    return 0;
}

GetSegmentRes NameSpaceTool::GetSegmentInfo(std::string fileName,
                                         uint64_t offset,
                                         PageFileSegment* segment) {
    curve::mds::GetOrAllocateSegmentRequest request;
    curve::mds::GetOrAllocateSegmentResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    request.set_filename(fileName);
    request.set_offset(offset);
    request.set_allocateifnotexist(false);
    FillUserInfo(&request);

    curve::mds::CurveFSService_Stub stub(channel_);
    stub.GetOrAllocateSegment(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        std::cout << "Get segment info fail, errCde = "
                  << response.statuscode()
                  << ", error content:"
                  << cntl.ErrorText() << std::endl;
        return GetSegmentRes::kOtherError;
    }

    if (response.has_statuscode()) {
        if (response.statuscode() == StatusCode::kOK) {
            *segment = response.pagefilesegment();
        } else if (response.statuscode() ==
                StatusCode::kSegmentNotAllocated) {
            return GetSegmentRes::kSegmentNotAllocated;
        } else {
            std::cout << "Get segment info fail, offset: "
                      << offset << " errCode:"
                      << response.statuscode() << std::endl;
            return GetSegmentRes::kOtherError;;
        }
    }
    return GetSegmentRes::kOK;
}

int NameSpaceTool::DeleteFile(const std::string& fileName, bool forcedelete) {
    curve::mds::DeleteFileRequest request;
    curve::mds::DeleteFileResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpc_timeout);
    request.set_filename(fileName);
    request.set_forcedelete(forcedelete);
    FillUserInfo(&request);

    curve::mds::CurveFSService_Stub stub(channel_);
    stub.DeleteFile(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        std::cout<< "Delete file fail, errCode = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
        return -1;
    }

    if (response.has_statuscode()) {
        if (response.statuscode() == StatusCode::kOK) {
            std::cout << "Delete file " << fileName << " success!" << std::endl;
            return 0;
        } else {
            std::cout << "Delete file fail, errCode: "
                      << response.statuscode() << std::endl;
        }
    }
    return -1;
}

int NameSpaceTool::CleanRecycleBin() {
    std::vector<FileInfo> files;
    if (ListDir("/RecycleBin", &files) != 0) {
        std::cout << "List RecycleBin fail!" << std::endl;
        return -1;
    }
    bool success = true;
    for (const auto& fileInfo : files) {
        std::string fileName = "/RecycleBin/" + fileInfo.filename();
        // 如果指定了-fileName，就只删除原来在这个目录下的文件
        if (!FLAGS_fileName.empty()) {
            std::string originPath = fileInfo.originalfullpathname();
            if (originPath.find(FLAGS_fileName) != 0) {
                continue;
            }
        }
        if (DeleteFile(fileName, true) != 0) {
            success = false;
        }
    }
    if (success) {
        std::cout << "Clean /RecycleBin success!" << std::endl;
    }
    return 0;
}

int NameSpaceTool::CreateFile(const std::string& fileName) {
    curve::mds:: CreateFileRequest request;
    curve::mds::CreateFileResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpc_timeout);
    request.set_filename(fileName);
    request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    request.set_filelength(FLAGS_fileLength);
    FillUserInfo(&request);

    curve::mds::CurveFSService_Stub stub(channel_);
    stub.CreateFile(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        std::cout<< "Create file fail, errCde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
        return -1;
    }

    if (response.has_statuscode()) {
        if (response.statuscode() == StatusCode::kOK) {
            std::cout << "Create file: " << fileName
                      << " success!" << std::endl;
            return 0;
        } else {
            std::cout << "Create file fail, errCode: "
                      << response.statuscode() << std::endl;
        }
    }
    return -1;
}

int NameSpaceTool::QueryChunkLocation(const std::string& fileName,
                                     uint64_t offset) {
    FileInfo fileInfo;
    int ret = GetFileInfo(fileName, &fileInfo);
    if (ret != 0) {
        std::cout << "Get file info failed!" << std::endl;
        return -1;
    }
    if (fileInfo.filetype() != curve::mds::FileType::INODE_PAGEFILE) {
        std::cout << "It is not a page file!" << std::endl;
        return -1;
    }
    uint64_t segmentSize = fileInfo.segmentsize();
    // segment对齐的offset
    uint64_t segOffset = (offset / segmentSize) * segmentSize;
    PageFileSegment segment;
    GetSegmentRes res = GetSegmentInfo(fileName, segOffset, &segment);
    if (res != GetSegmentRes::kOK) {
        if (res == GetSegmentRes::kSegmentNotAllocated) {
            std::cout << "Chunk has not been allocated!" << std::endl;
            return -1;
        } else {
            std::cout << "Get segment info from mds fail!" << std::endl;
            return -1;
        }
    }
    // 在segment里面的chunk的索引
    uint64_t chunkIndex = (offset - segOffset) / segment.chunksize();
    if (chunkIndex >= segment.chunks_size()) {
        std::cout << "ChunkIndex exceed chunks num in segment!" << std::endl;
        return -1;
    }
    PageFileChunkInfo chunk = segment.chunks(chunkIndex);
    uint64_t chunkId = chunk.chunkid();
    uint32_t logicPoolId = segment.logicalpoolid();
    uint32_t copysetId = chunk.copysetid();
    uint64_t groupId = (static_cast<uint64_t>(logicPoolId) << 32) | copysetId;
    std::cout << "chunkId: " << chunkId
              << ", logicalPoolId: " << logicPoolId
              << ", copysetId: " << copysetId
              << ", groupId: " << groupId << std::endl;
    return PrintCopysetMembers(segment.logicalpoolid(), copysetId);
}

int NameSpaceTool::PrintCopysetMembers(uint32_t logicalPoolId,
                                       uint32_t copysetId) {
    curve::mds::topology::GetChunkServerListInCopySetsRequest request;
    curve::mds::topology::GetChunkServerListInCopySetsResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    request.set_logicalpoolid(logicalPoolId);
    request.add_copysetid(copysetId);
    curve::mds::topology::TopologyService_Stub topo_stub(channel_);
    topo_stub.GetChunkServerListInCopySets(&cntl,
                            &request, &response, nullptr);
    if (cntl.Failed()) {
        std::cout << "GetChunkServerListInCopySets fail, errCode = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
        return -1;
    }
    if (response.has_statuscode()) {
        if (response.statuscode() != kTopoErrCodeSuccess) {
            std::cout << "GetChunkServerListInCopySets fail, errCode: "
                      << response.statuscode() << std::endl;
            return -1;
        }
    }
    std::cout << "location: {";
    for (int i = 0; i < response.csinfo(0).cslocs_size(); ++i) {
        if (i != 0) {
            std::cout << ", ";
        }
        auto location = response.csinfo(0).cslocs(i);
        std::cout << location.hostip() << ":"
                  << std::to_string(location.port());
    }
    std::cout << "}" << std::endl;
    return 0;
}

template <class T>
void NameSpaceTool::FillUserInfo(T* request) {
    uint64_t date = curve::common::TimeUtility::GetTimeofDayUs();
    request->set_owner(FLAGS_userName);
    request->set_date(date);

    if (!FLAGS_userName.compare("root") &&
        FLAGS_password.compare("")) {
        std::string str2sig = Authenticator::GetString2Signature(date,
                                                        FLAGS_userName);
        std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                         FLAGS_password);
        request->set_signature(sig);
    }
}
}  // namespace tool
}  // namespace curve
