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

#ifndef  SRC_CHUNKSERVER_RAFTLOG_BRAFT_SEGMENT_H_
#define  SRC_CHUNKSERVER_RAFTLOG_BRAFT_SEGMENT_H_

#include <braft/log.h>
#include <string>
#include "src/chunkserver/raftlog/segment.h"

namespace curve {
namespace chunkserver {

class BraftSegment : public Segment {
 public:
    BraftSegment(const std::string& path, const int64_t first_index,
                 int checksum_type)
        : _segment(new braft::Segment(path, first_index, checksum_type))
    {}
    BraftSegment(const std::string& path, const int64_t first_index,
            const int64_t last_index, int checksum_type)
        : _segment(new braft::Segment(path, first_index,
                                      last_index, checksum_type))
    {}

    int create() override {
        return _segment->create();
    }

    int load(braft::ConfigurationManager* configuration_manager) override {
        return _segment->load(configuration_manager);
    }

    int append(const braft::LogEntry* entry) override {
        return _segment->append(entry);
    }

    braft::LogEntry* get(const int64_t index) const override {
        return _segment->get(index);
    }

    int64_t get_term(const int64_t index) const override {
        return _segment->get_term(index);
    }

    int close(bool will_sync) override {
        return _segment->close(will_sync);
    }

    int sync(bool will_sync) override {
        return _segment->sync(will_sync);
    }

    int unlink() override {
        return _segment->unlink();
    }

    int truncate(const int64_t last_index_kept) override {
        return _segment->truncate(last_index_kept);
    }

    bool is_open() const override {
        return _segment->is_open();
    }

    int64_t bytes() const override {
        return _segment->bytes();
    }

    int64_t first_index() const override {
        return _segment->first_index();
    }

    int64_t last_index() const override {
        return _segment->last_index();
    }

    std::string file_name() override {
        return _segment->file_name();
    }

    int currut_fd() override {
        return 0;
    }
    bool get_meta_info(const int64_t index, off_t* offset, size_t* length, int64_t* term) override {
        return false;
    };

 private:
    scoped_refptr<braft::Segment> _segment;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTLOG_BRAFT_SEGMENT_H_
