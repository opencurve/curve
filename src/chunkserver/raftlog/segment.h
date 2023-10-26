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
 * Created Date: 2020-09-24
 * Author: charisu
 */

#ifndef  SRC_CHUNKSERVER_RAFTLOG_SEGMENT_H_
#define  SRC_CHUNKSERVER_RAFTLOG_SEGMENT_H_

#include <braft/log_entry.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <string>

namespace curve {
namespace chunkserver {

class Segment : public butil::RefCountedThreadSafe<Segment> {
 public:
    virtual ~Segment() = default;
    // create open segment
    virtual int create() = 0;

    // load open or closed segment
    // open fd, load index, truncate uncompleted entry
    virtual int load(braft::ConfigurationManager* configuration_manager) = 0;

    // serialize entry, and append to open segment
    virtual int append(const braft::LogEntry* entry) = 0;

    // get entry by index
    virtual braft::LogEntry* get(const int64_t index) const = 0;

    // get entry's term by index
    virtual int64_t get_term(const int64_t index) const = 0;

    // close open segment
    virtual int close(bool will_sync = true) = 0;

    // sync open segment
    virtual int sync(bool will_sync) = 0;

    // unlink segment
    virtual int unlink() = 0;

    // truncate segment to last_index_kept
    virtual int truncate(const int64_t last_index_kept) = 0;

    virtual bool is_open() const = 0;

    virtual int64_t bytes() const = 0;

    virtual int64_t first_index() const = 0;

    virtual int64_t last_index() const = 0;

    virtual std::string file_name() = 0;

    virtual int currut_fd() = 0;
    virtual bool get_meta_info(const int64_t index, off_t* offset, size_t* length, int64_t* term) = 0;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTLOG_SEGMENT_H_
