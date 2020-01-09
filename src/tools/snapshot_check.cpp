/*
 * Project: curve
 * Created Date: 2019-10-10
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include "src/tools/snapshot_check.h"

DEFINE_string(clientConfPath, "conf/client.conf", "client config path");
DEFINE_string(file_name, "", "file name to read snapshot");
DEFINE_string(user_name, "test", "user name");
DEFINE_string(snapshotId, "", "UUID of snapshot");
DEFINE_string(snapshot_clone_config, "./conf/snapshot_clone_server.conf", "config file of snapshot clone server");  // NOLINT

namespace curve {
namespace tool {

SnapshotCheck::~SnapshotCheck() {
    if (inited_) {
        snapshot_->UnInit();
        client_->UnInit();
    }
}

int SnapshotCheck::Init() {
    if (inited_) {
        return 0;
    }
    if (snapshot_->Init(FLAGS_snapshotId,
                        FLAGS_snapshot_clone_config) != 0) {
        std::cout << "Init snapshotRead fail!" << std::endl;
        return -1;
    }
    if (client_->Init(FLAGS_clientConfPath.c_str()) != 0) {
        std::cout << "Init file client fail!" << std::endl;
        return -1;
    }
    inited_ = true;
    return 0;
}

bool SnapshotCheck::SupportCommand(const std::string& command) {
    return (command == kSnapshotCheckCmd);
}


void SnapshotCheck::PrintHelp(const std::string &cmd) {
    if (!SupportCommand(cmd)) {
        std::cout << "Command not supported!" << std::endl;
        return;
    }
    std::cout << "Example: " << std::endl;
    std::cout << "curve_ops_tool snapshot-check -snapshotId=12345678 -file_name=/test -clientConfPath=conf/client.conf "  // NOLINT
                 "-snapshot_clone_config=conf/snapshot_clone_server.conf -user_name=test" << std::endl;  // NOLINT
}

int SnapshotCheck::RunCommand(const std::string &cmd) {
    if (Init() != 0) {
        std::cout << "Init ConsistencyCheck failed" << std::endl;
        return -1;
    }
    if (cmd == kSnapshotCheckCmd) {
        return Check();
    } else {
        std::cout << "Command not supported!" << std::endl;
        return -1;
    }
}

int SnapshotCheck::Check() {
    curve::client::UserInfo_t userinfo;
    userinfo.owner = FLAGS_user_name;
    int fd = client_->Open4ReadOnly(FLAGS_file_name, userinfo);
    if (fd < 0) {
        std::cout << "Open file fail! errCode: " << fd << std::endl;
        return -1;
    }
    // 获取文件fileInfo
    FileStatInfo fileInfo;
    int ret = client_->StatFile(FLAGS_file_name, userinfo, &fileInfo);
    if (ret < 0) {
        std::cout << "StatFile fail! errCode: " << ret << std::endl;
        return -1;
    }
    // 对比元数据
    SnapshotRepoItem snapshotInfo;
    snapshot_->GetSnapshotInfo(&snapshotInfo);
    if (snapshotInfo.fileLength != fileInfo.length) {
        std::cout << "File length not match!" << std::endl;
        return -1;
    }
    uint64_t fileLength = fileInfo.length;
    // 开始比较,用chunksize作为每次比较的大小，避免重复加载
    uint64_t size = snapshotInfo.chunkSize;
    char* fileBuf = new char[size];
    char* snapBuf = new char[size];
    for (uint64_t off = 0; off < fileLength; off += size) {
        // 读出文件
        ret = client_->Read(fd, fileBuf, off, size);
        if (ret < 0) {
            std::cout << "Read file fail with errCode: " << ret << std::endl;
            delete[] fileBuf;
            delete[] snapBuf;
            return -1;
        }  else if (ret != size) {
            std::cout << "Read size not match!" << std::endl;
            delete[] fileBuf;
            delete[] snapBuf;
            return -1;
        }
        // 读出快照
        ret = snapshot_->Read(snapBuf, off, size);
        if (ret != 0) {
            std::cout << "Read snapshot fail!" << std::endl;
            delete[] fileBuf;
            delete[] snapBuf;
            return -1;
        }
        // 计算hash
        uint32_t fileHash = 0;
        uint32_t snapHash = 0;
        fileHash = curve::common::CRC32(fileHash, fileBuf, size);
        snapHash = curve::common::CRC32(snapHash, snapBuf, size);
        if (fileHash != snapHash) {
            std::cout << "snapshot not match file! offset: " << off
                      << " len: " << size << std::endl;
            delete[] fileBuf;
            delete[] snapBuf;
            return -1;
        }
    }
    std::cout << "snapshot check ok!" << std::endl;
    delete[] fileBuf;
    delete[] snapBuf;
    return 0;
}
}  // namespace tool
}  // namespace curve
