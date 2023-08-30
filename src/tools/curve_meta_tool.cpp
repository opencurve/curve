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

#include "src/tools/curve_meta_tool.h"

DEFINE_uint32(pageSize, 4096, "meta page size of chunkfile");
DECLARE_string(fileName);

namespace curve {
namespace tool {

void CurveMetaTool::PrintHelp(const std::string& cmd) {
    std::cout << "curve_chunkserver_tool " << cmd << " -fileName=chunk_2542065"
              << std::endl;
}

std::ostream& operator<<(std::ostream& os, const vector<BitRange>& ranges) {
    for (uint32_t i = 0; i < ranges.size(); ++i) {
        if (i != 0) {
            os << ", ";
        }
        uint64_t startOff = ranges[i].beginIndex * FLAGS_pageSize;
        uint64_t endOff = (ranges[i].endIndex + 1) * FLAGS_pageSize;
        os << "[" <<  startOff << ","
                  << endOff << ")";
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const ChunkFileMetaPage& metaPage) {
    os << "meta page info:" << std::endl;
    os << "version: " << static_cast<int>(metaPage.version) << std::endl;
    os << "sn: " << metaPage.sn << std::endl;
    os << "correctedSn: " << metaPage.correctedSn << std::endl;
    if (!metaPage.location.empty()) {
        os << "location: " << metaPage.location << std::endl;
    }
    if (metaPage.bitmap) {
        auto bitmap = metaPage.bitmap;
        uint32_t startIndex = 0;
        uint32_t endIndex = bitmap->Size();
        vector<BitRange> clearRanges;
        vector<BitRange> setRanges;
        bitmap->Divide(startIndex, endIndex, &clearRanges, &setRanges);
        os << "writed bytes ragne: " << setRanges << std::endl;
        os << "clear bytes range: " << clearRanges << std::endl;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const SnapshotMetaPage& metaPage) {
    os << "meta page info:" << std::endl;
    os << "version: " << static_cast<int>(metaPage.version) << std::endl;
    os << "dameged: ";
    if (metaPage.damaged) {
        std::cout << "true" << std::endl;
    } else {
        std::cout << "false" << std::endl;
    }
    os << "sn: " << metaPage.sn << std::endl;
    if (metaPage.bitmap) {
        auto bitmap = metaPage.bitmap;
        uint32_t startIndex = 0;
        uint32_t endIndex = bitmap->Size();
        vector<BitRange> clearRanges;
        vector<BitRange> setRanges;
        bitmap->Divide(startIndex, endIndex, &clearRanges, &setRanges);
        os << "writed bytes ragne: " << setRanges << std::endl;
        os << "clear bytes range: " << clearRanges << std::endl;
    }
    return os;
}

bool CurveMetaTool::SupportCommand(const std::string& cmd) {
    return cmd == kChunkMeta || cmd == kSnapshotMeta;
}

int CurveMetaTool::RunCommand(const std::string& cmd) {
    if (cmd == kChunkMeta) {
        return PrintChunkMeta(FLAGS_fileName);
    } else if (cmd == kSnapshotMeta) {
        return PrintSnapshotMeta(FLAGS_fileName);
    } else {
        std::cout << "command not supported!" << std::endl;
        return -1;
    }
}



int CurveMetaTool::PrintChunkMeta(const std::string& chunkFileName) {
    //Open chunk file
    int fd = localFS_->Open(chunkFileName.c_str(), O_RDONLY|O_NOATIME);
    if (fd < 0) {
        std::cout << "Fail to open " << chunkFileName << ", "
                  << berror() << std::endl;
        return -1;
    }

    //Read chunk header
    std::unique_ptr<char[]> buf(new char[FLAGS_pageSize]);
    memset(buf.get(), 0, FLAGS_pageSize);
    int rc = localFS_->Read(fd, buf.get(), 0, FLAGS_pageSize);
    localFS_->Close(fd);
    if (rc != static_cast<int64_t>(FLAGS_pageSize)) {
        if (rc < 0) {
            std::cout << "Fail to read metaPage from "
                  << chunkFileName << ", " << berror() << std::endl;
        } else {
            std::cout << "Read size not match, page size: " << FLAGS_pageSize
                      << ", read size: " << rc << ", " << berror() << std::endl;
        }
        return -1;
    }
    ChunkFileMetaPage metaPage;
    CSErrorCode ret = metaPage.decode(buf.get());
    if (ret != CSErrorCode::Success) {
        std::cout << "Failed to decode meta page" << std::endl;
        return -1;
    }

    //Print MetaPage
    std::cout << metaPage;
    return 0;
}

int CurveMetaTool::PrintSnapshotMeta(const std::string& snapFileName) {
    //Open snapshot file
    int fd = localFS_->Open(snapFileName.c_str(), O_RDONLY|O_NOATIME);
    if (fd < 0) {
        std::cout << "Fail to open " << snapFileName << ", "
                  << berror() << std::endl;
        return -1;
    }

    //Read snapshot file header
    std::unique_ptr<char[]> buf(new char[FLAGS_pageSize]);
    memset(buf.get(), 0, FLAGS_pageSize);
    int rc = localFS_->Read(fd, buf.get(), 0, FLAGS_pageSize);
    localFS_->Close(fd);
    if (rc != static_cast<int64_t>(FLAGS_pageSize)) {
        if (rc < 0) {
            std::cout << "Fail to read metaPage from "
                  << snapFileName << ", " << berror() << std::endl;
        } else {
            std::cout << "Read size not match, page size: " << FLAGS_pageSize
                      << ", read size: " << rc << std::endl;
        }
        return -1;
    }
    SnapshotMetaPage metaPage;
    CSErrorCode ret = metaPage.decode(buf.get());
    if (ret != CSErrorCode::Success) {
        std::cout << "Failed to decode meta page" << std::endl;
        return -1;
    }

    //Print MetaPage
    std::cout << metaPage;
    return 0;
}

}  // namespace tool
}  // namespace curve
