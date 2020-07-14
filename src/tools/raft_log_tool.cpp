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
 * Created Date: 2020-02-28
 * Author: charisu
 */

#include "src/tools/raft_log_tool.h"

DECLARE_string(fileName);

namespace curve {
namespace tool {

#define BRAFT_SEGMENT_OPEN_PATTERN "log_inprogress_%020" PRId64
#define BRAFT_SEGMENT_CLOSED_PATTERN "log_%020" PRId64 "_%020" PRId64

enum class CheckSumType {
    CHECKSUM_MURMURHASH32 = 0,
    CHECKSUM_CRC32 = 1,
};

inline bool VerifyCheckSum(int type,
                            const char* data, size_t len, uint32_t value) {
    CheckSumType checkSunType = static_cast<CheckSumType>(type);
    switch (checkSunType) {
    case CheckSumType::CHECKSUM_MURMURHASH32:
        return (value == braft::murmurhash32(data, len));
    case CheckSumType::CHECKSUM_CRC32:
        return (value == braft::crc32(data, len));
    default:
        std::cout << "Unknown checksum_type=" << type <<std::endl;
        return false;
    }
}

std::ostream& operator<<(std::ostream& os, const EntryHeader& h) {
    os << "{term=" << h.term << ", type=" << h.type << ", data_len="
       << h.data_len << ", checksum_type=" << h.checksum_type
       << ", data_checksum=" << h.data_checksum << '}';
    return os;
}

bool EntryHeader::operator== (const EntryHeader& rhs) const {
    return term == rhs.term
        && type == rhs.type
        && checksum_type == rhs.checksum_type
        && data_len == rhs.data_len
        && data_checksum == rhs.data_checksum;
}

void RaftLogTool::PrintHelp(const std::string& cmd) {
    if (!SupportCommand(cmd)) {
        std::cout << "command not supported!" << std::endl;
        return;
    }
    std::cout << "curve_chunkserver_tool " << cmd
              << " -fileName=log_inprogress_01" << std::endl;
}

int RaftLogTool::RunCommand(const std::string& cmd) {
    if (cmd == kRaftLogMeta) {
        return PrintHeaders(FLAGS_fileName);
    } else {
        std::cout << "command not supported!" << std::endl;
        return -1;
    }
}

bool RaftLogTool::SupportCommand(const std::string& cmd) {
    return cmd == kRaftLogMeta;
}

int RaftLogTool::PrintHeaders(const std::string& fileName) {
    // 从文件名获取firstIndex
    int64_t firstIndex;
    int res = ParseFirstIndexFromFileName(fileName, &firstIndex);
    if (res != 0) {
        std::cout << "ParseFirstIndexFromFileName fail" << std::endl;
        return -1;
    }

    res = parser_->Init(fileName);
    if (res != 0) {
        std::cout << "init segmentparser fail!" << std::endl;
        return -1;
    }
    EntryHeader header;
    int64_t curIndex = firstIndex;
    while (parser_->GetNextEntryHeader(&header)) {
        std::cout << "index = " << curIndex++
                  << ", Entry Header = " << header << std::endl;
    }
    parser_->UnInit();
    if (!parser_->SuccessfullyFinished()) {
        std::cout << "Parse file " << fileName << " fail" << std::endl;
        return -1;
    }
    return 0;
}

int SegmentParser::Init(const std::string& fileName) {
    fd_ = localFS_->Open(fileName.c_str(), O_RDONLY);
    if (fd_ < 0) {
        std::cout << "Fail to open " << fileName << ", "
                  << berror() << std::endl;
        return -1;
    }

    // get file size
    struct stat stBuf;
    if (localFS_->Fstat(fd_, &stBuf) != 0) {
        std::cout << "Fail to get the stat of " << fileName
                  << ", " << berror() << std::endl;
        localFS_->Close(fd_);
        return -1;
    }
    fileLen_ = stBuf.st_size;
    off_ = 0;
    return 0;
}

void SegmentParser::UnInit() {
    localFS_->Close(fd_);
}

bool SegmentParser::GetNextEntryHeader(EntryHeader* head) {
    if (off_ >= fileLen_) {
        return false;
    }
    char buf[ENTRY_HEADER_SIZE];
    const ssize_t n = localFS_->Read(fd_, buf, off_, ENTRY_HEADER_SIZE);
    if (n != (ssize_t)ENTRY_HEADER_SIZE) {
        if (n < 0) {
            std::cout << "read header from file, fd: " << fd_ << ", offset: "
                      << off_ << ", " << berror() << std::endl;
        } else {
            std::cout << "Read size not match, header size: "
                      << ENTRY_HEADER_SIZE << ", read size: "
                      << n << std::endl;
        }
        return false;
    }

    int64_t term = 0;
    uint32_t meta_field;
    uint32_t data_len = 0;
    uint32_t data_checksum = 0;
    uint32_t header_checksum = 0;
    butil::RawUnpacker(buf).unpack64((uint64_t&)term)
                  .unpack32(meta_field)
                  .unpack32(data_len)
                  .unpack32(data_checksum)
                  .unpack32(header_checksum);
    EntryHeader tmp;
    tmp.term = term;
    tmp.type = meta_field >> 24;
    tmp.checksum_type = (meta_field << 8) >> 24;
    tmp.data_len = data_len;
    tmp.data_checksum = data_checksum;
    if (!VerifyCheckSum(tmp.checksum_type,
                        buf, ENTRY_HEADER_SIZE - 4, header_checksum)) {
        std::cout << "Found corrupted header at offset=" << off_
                  << ", header=" << tmp;
        return false;
    }
    if (head != NULL) {
        *head = tmp;
    }
    off_ += (ENTRY_HEADER_SIZE + tmp.data_len);
    return true;
}

int RaftLogTool::ParseFirstIndexFromFileName(const std::string& fileName,
                                             int64_t* firstIndex) {
    int match = 0;
    int64_t lastIndex  = 0;
    std::string name;
    auto pos =  fileName.find_last_of("/");
    if (pos == std::string::npos) {
        name = fileName;
    } else {
        name = fileName.substr(pos + 1);
    }
    match = sscanf(name.c_str(), BRAFT_SEGMENT_CLOSED_PATTERN,
        firstIndex, &lastIndex);
    if (match == 2) {
        std::cout << "it is a closed segment, path: " << fileName
                  << " first index: " << *firstIndex
                  << " last index: " << lastIndex << std::endl;
    } else {
        match = sscanf(name.c_str(), BRAFT_SEGMENT_OPEN_PATTERN,
            firstIndex);
        if (match == 1) {
            std::cout << "it is a opening segment, path: "
                      << fileName
                      << " first index: " << *firstIndex << std::endl;
        } else {
            std::cout << "filename = " << fileName <<
                         " is not a raft segment pattern!" << std::endl;
            return -1;
        }
    }
    return 0;
}
}  // namespace tool
}  // namespace curve
