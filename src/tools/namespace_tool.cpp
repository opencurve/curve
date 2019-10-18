/*
 * Project: curve
 * Created Date: 2019-09-25
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include <thread>
#include "src/tools/namespace_tool.h"

DEFINE_string(mds_addr, "127.0.0.1:6666", "mds addr");
DEFINE_string(fileName, "", "file name");
DEFINE_string(userName, "root", "owner of the file");
DEFINE_string(password, "root_password", "password of administrator");
DEFINE_bool(forcedelete, false, "force delete file or not");
DEFINE_uint64(fileLength, 20*1024*1024*1024ull, "file length");
DEFINE_bool(isTest, false, "is unit test or not");

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
    } else {
        std::cout << "Command not support!" << std::endl;
        PrintHelp("get");
        PrintHelp("list");
        PrintHelp("seginfo");
        PrintHelp("delete");
        PrintHelp("clean-recycle");
        PrintHelp("create");
        return -1;
    }
}

int NameSpaceTool::Init() {
    // 初始化channel
    if (channel_.Init(FLAGS_mds_addr.c_str(), nullptr) != 0) {
        std::cout << "Init channel failed!" << std::endl;
        return -1;
    }
    return 0;
}

void NameSpaceTool::PrintHelp(const std::string &cmd) {
    std::cout << "Example: " << std::endl;
    if (cmd == "get" || cmd == "list" || cmd == "seginfo") {
        std::cout << "curve_ops_tool " << cmd << " -mds_addr=127.0.0.1:6666 -fileName=/test" << std::endl;  // NOLINT
    } else if (cmd == "clean-recycle") {
        std::cout << "curve_ops_tool " << cmd << " -mds_addr=127.0.0.1:6666" << std::endl;  // NOLINT
    } else if (cmd == "create") {
        std::cout << "curve_ops_tool " << cmd << " -mds_addr=127.0.0.1:6666 -fileName=/test -userName=test -password=123 -fileLength=1024" << std::endl;  // NOLINT
    } else if (cmd == "delete") {
        std::cout << "curve_ops_tool " << cmd << " -mds_addr=127.0.0.1:6666 -fileName=/test -userName=test -password=123 -forcedelete=true" << std::endl;  // NOLINT
    } else {
        std::cout << "command not found!" << std::endl;
    }
}

int NameSpaceTool::PrintFileInfoAndActualSize(const std::string &fileName) {
    FileInfo fileInfo;
    auto ret = GetFileInfo(fileName, &fileInfo);
    if (ret != 0) {
        return -1;
    }
    std::cout << "File info:" << std::endl;
    std::cout << fileInfo.DebugString() << std::endl;
    fileInfo.set_originalfullpathname(fileName);
    int64_t size = GetActualSize(fileInfo);
    if (size < 0) {
        std::cout << "Get actual size fail!" << std::endl;
        return -1;
    }
    double res = static_cast<double>(size) / (1024 * 1024 * 1024);
    std::cout << "actual size: " << res << "GB" << std::endl;
    return 0;
}

int NameSpaceTool::GetFileInfo(const std::string &fileName,
                               FileInfo* fileInfo) {
    curve::mds::GetFileInfoRequest request;
    curve::mds::GetFileInfoResponse response;
    brpc::Controller cntl;
    request.set_filename(fileName);
    FillUserInfo(&request);

    curve::mds::CurveFSService_Stub stub(&channel_);
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
            std::cout << "GetFileInfo fail, errCode: "
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
        if (GetSegmentInfo(fileInfo, &segments) != 0) {
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
                std::cout << "Get actual size fail!" << std::endl;
                return -1;
            }
            size += tmp;
        }
        return size;
    }
}

int NameSpaceTool::PrintListDir(const std::string& dirName) {
    std::vector<FileInfo> files;
    if (ListDir(dirName, &files) != 0) {
        std::cout << "List directory failed!" << std::endl;
        return -1;
    }
    for (auto& file : files) {
        std::cout << file.DebugString() << std::endl;
    }
    return 0;
}

int NameSpaceTool::ListDir(const std::string& dirName,
                           std::vector<FileInfo>* files) {
    curve::mds::ListDirRequest request;
    curve::mds::ListDirResponse response;
    brpc::Controller cntl;
    request.set_filename(dirName);
    FillUserInfo(&request);

    curve::mds::CurveFSService_Stub stub(&channel_);
    stub.ListDir(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        std::cout<< "ListDir fail, errCde = "
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
            std::cout << "ListDir fail, errCode: "
                      << response.statuscode() << std::endl;
        }
    }
    return -1;
}

int NameSpaceTool::PrintSegmentInfo(const std::string &fileName) {
    FileInfo fileInfo;
    int ret = GetFileInfo(fileName, &fileInfo);
    if (ret != 0) {
        std::cout << "PrintSegmentInfo: get file info failed!" << std::endl;
        return -1;
    }
    fileInfo.set_originalfullpathname(fileName);
    std::vector<PageFileSegment> segments;
    if (GetSegmentInfo(fileInfo, &segments) != 0) {
        std::cout << "Get segment info fail!" << std::endl;
        return -1;
    }
    for (auto& segment : segments) {
        std::cout << segment.DebugString() << std::endl;
    }
    return 0;
}

int NameSpaceTool::GetSegmentInfo(const FileInfo &fileInfo,
                                  std::vector<PageFileSegment>* segments) {
    // 如果是目录，不能计算segmentsize
    if (fileInfo.filetype() == curve::mds::FileType::INODE_DIRECTORY) {
        std::cout << "It is a directory!" << std::endl;
        return -1;
    }

    // 获取文件的segment数，并打印每个segment的详细信息
    int segmentNum = fileInfo.length() / fileInfo.segmentsize();
    uint64_t segmentSize = fileInfo.segmentsize();
    std::string fileName = fileInfo.originalfullpathname();
    for (int i = 0; i != segmentNum; i++) {
        // load  segment
        curve::mds::GetOrAllocateSegmentRequest request;
        curve::mds::GetOrAllocateSegmentResponse response;
        brpc::Controller cntl;
        request.set_filename(fileName);
        request.set_offset(i*segmentSize);
        request.set_allocateifnotexist(false);
        FillUserInfo(&request);

        curve::mds::CurveFSService_Stub stub(&channel_);
        stub.GetOrAllocateSegment(&cntl, &request, &response, NULL);

        if (cntl.Failed()) {
            std::cout<< "Get segment info fail, errCde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
            return -1;
        }

        if (response.has_statuscode()) {
            if (response.statuscode() == StatusCode::kOK) {
                segments->push_back(response.pagefilesegment());
            } else if (response.statuscode() ==
                            StatusCode::kSegmentNotAllocated) {
                continue;
            } else {
                std::cout << "GetSegmentInfo fail, offset: "
                      << i*segmentSize << " errCode:"
                      << response.statuscode() << std::endl;
                return -1;
            }
        }
    }
    return 0;
}

int NameSpaceTool::DeleteFile(const std::string& fileName, bool forcedelete) {
    curve::mds::DeleteFileRequest request;
    curve::mds::DeleteFileResponse response;
    brpc::Controller cntl;
    request.set_filename(fileName);
    request.set_forcedelete(forcedelete);
    FillUserInfo(&request);

    curve::mds::CurveFSService_Stub stub(&channel_);
    stub.DeleteFile(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        std::cout<< "DeleteFile fail, errCde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
        return -1;
    }

    if (response.has_statuscode()) {
        if (response.statuscode() == StatusCode::kOK) {
            std::cout << "DeleteFile " << fileName << " success!" << std::endl;
            return 0;
        } else {
            std::cout << "DeleteFile fail, errCode: "
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
    for (const auto& fileInfo : files) {
        std::string fileName = "/RecycleBin/" + fileInfo.filename();
        if (DeleteFile(fileName, true) != 0) {
            std::cout << "delete file: " << fileName << " fail!" << std::endl;
        }
    }
    return 0;
}

int NameSpaceTool::CreateFile(const std::string& fileName) {
    curve::mds:: CreateFileRequest request;
    curve::mds::CreateFileResponse response;
    brpc::Controller cntl;
    request.set_filename(fileName);
    request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    request.set_filelength(FLAGS_fileLength);
    FillUserInfo(&request);

    curve::mds::CurveFSService_Stub stub(&channel_);
    stub.CreateFile(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        std::cout<< "CreateFile fail, errCde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
        return -1;
    }

    if (response.has_statuscode()) {
        if (response.statuscode() == StatusCode::kOK) {
            return 0;
        } else {
            std::cout << "CreateFile fail, errCode: "
                      << response.statuscode() << std::endl;
        }
    }
    return -1;
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
