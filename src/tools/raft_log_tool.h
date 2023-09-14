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

#ifndef SRC_TOOLS_RAFT_LOG_TOOL_H_
#define SRC_TOOLS_RAFT_LOG_TOOL_H_

#include <gflags/gflags.h>
#include <braft/util.h>
#include <butil/raw_pack.h>
#include <fcntl.h>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include "src/fs/local_filesystem.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

namespace curve {
namespace tool {

using curve::fs::LocalFileSystem;

const size_t ENTRY_HEADER_SIZE = 24;

struct EntryHeader {
    int64_t term;
    int type;
    int checksum_type;
    uint32_t data_len;
    uint32_t data_checksum;

    bool operator== (const EntryHeader& rhs) const;
};

std::ostream& operator<<(std::ostream& os, const EntryHeader& h);

class SegmentParser {
 public:
    explicit SegmentParser(std::shared_ptr<LocalFileSystem> localFS) :
                localFS_(localFS) {}

    /**
     * @brief initialization
     * @param fileName The file name of the segmnet file
     * @return returns 0 if successful, -1 if unsuccessful
     */
    virtual int Init(const std::string& fileName);

    /**
     * @brief deinitialization
     */
    virtual void UnInit();

    /**
     * @brief Get the next EntryHeader
     * @param[out] header log entry header
     * @return returns true for success, false for failure
     */
    virtual bool GetNextEntryHeader(EntryHeader* header);

    /**
     * @brief Determine if the read was successfully completed
     */
    virtual bool SuccessfullyFinished() {
        return off_ >= fileLen_;
    }

 private:
    // File Descriptor
    int fd_;
    // Offset for the next Entry
    int64_t off_;
    // File length
    int64_t fileLen_;

    std::shared_ptr<LocalFileSystem> localFS_;
};

class RaftLogTool : public CurveTool {
 public:
    explicit RaftLogTool(std::shared_ptr<SegmentParser> parser) :
                                    parser_(parser) {}

    /**
     * @brief Execute command
     * @param command The command to be executed
     * @return returns 0 for success, -1 for failure
    */
    int RunCommand(const std::string& command) override;

    /**
     * @brief Print help information
    */
    void PrintHelp(const std::string& command) override;

    /**
     * @brief returns whether the command is supported
     * @param command: The command executed
     * @return true/false
     */
    static bool SupportCommand(const std::string& command);

 private:
    /**
     * @brief Print the header information of all raft logs in the file
     * @param fileName raft log file name
     * @return successfully returns 0, otherwise returns -1
     */
    int PrintHeaders(const std::string& fileName);

    /**
     * @brief Parse the entry header from the file
     * @param fd file descriptor
     * @param offset Offset in file
     * @param[out] head entry header information, valid when the return value is 0
     * @return successfully returns 0, otherwise returns -1
     */
    int ParseEntryHeader(int fd, off_t offset, EntryHeader *head);

    /**
     * @brief Parsing first index from file name
     * @param fileName raft log file name
     * @param[out] firstIndex The first index of the log entry contained in the segment file
     * @return successfully returns 0, otherwise returns -1
     */
    int ParseFirstIndexFromFileName(const std::string& fileName,
                                    int64_t* firstIndex);

    std::shared_ptr<SegmentParser> parser_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_RAFT_LOG_TOOL_H_
