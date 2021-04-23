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
 * Created Date: 2020-09-16
 * Author: charisu
 */

// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/10/08 17:00:05

#include <fcntl.h>
#include <gtest/gtest.h>
#include <braft/log.h>
#include <memory>
#include "src/chunkserver/raftlog/curve_segment.h"
#include "src/chunkserver/raftlog/define.h"
#include "test/fs/mock_local_filesystem.h"
#include "test/chunkserver/datastore/mock_file_pool.h"
#include "test/chunkserver/raftlog/common.h"

namespace curve {
namespace chunkserver {

using curve::fs::MockLocalFileSystem;
using ::testing::Return;
using ::testing::_;

class CurveSegmentTest : public testing::Test {
 protected:
    CurveSegmentTest() {
        fp_option.metaPageSize = kPageSize;
        fp_option.fileSize = kSegmentSize;
    }
    void SetUp() {
        lfs = std::make_shared<MockLocalFileSystem>();
        file_pool = std::make_shared<MockFilePool>(lfs);
        std::string cmd = std::string("mkdir ") + kRaftLogDataDir;
        ::system(cmd.c_str());
    }
    void TearDown() {
        std::string cmd = std::string("rm -rf ") + kRaftLogDataDir;
        ::system(cmd.c_str());
    }
    void append_entries_curve_segment(CurveSegment* segment,
                                const char* data_pattern = "hello, world: %d",
                                int start_index = 0,
                                int end_index = 10) {
        for (int i = start_index; i < end_index; i++) {
            braft::LogEntry* entry = new braft::LogEntry();
            entry->AddRef();
            entry->type = braft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = i + 1;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), data_pattern, i + 1);
            entry->data.append(data_buf);
            ASSERT_EQ(0, segment->append(entry));
            entry->Release();
        }
    }
    void append_entries_braft_segment(braft::Segment* segment,
                                const char* data_pattern = "hello, world: %d",
                                int start_index = 0,
                                int end_index = 10) {
        for (int i = start_index; i < end_index; i++) {
            braft::LogEntry* entry = new braft::LogEntry();
            entry->AddRef();
            entry->type = braft::ENTRY_TYPE_DATA;
            entry->id.term = 1;
            entry->id.index = i + 1;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), data_pattern, i + 1);
            entry->data.append(data_buf);
            ASSERT_EQ(0, segment->append(entry));
            entry->Release();
        }
    }
    void read_entries_curve_segment(CurveSegment* segment,
                                const char* data_pattern = "hello, world: %d",
                                int start_index = 0,
                                int end_index = 10) {
        for (int i = start_index; i < end_index; i++) {
            int64_t term = segment->get_term(i+1);
            ASSERT_EQ(term, 1);

            braft::LogEntry* entry = segment->get(i+1);
            ASSERT_EQ(entry->id.term, 1);
            ASSERT_EQ(entry->type, braft::ENTRY_TYPE_DATA);
            ASSERT_EQ(entry->id.index, i+1);

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), data_pattern, i + 1);
            ASSERT_EQ(data_buf, entry->data.to_string());
            entry->Release();
        }
    }
    std::shared_ptr<MockLocalFileSystem> lfs;
    std::shared_ptr<MockFilePool> file_pool;
    FilePoolOptions fp_option;
};

TEST_F(CurveSegmentTest, open_segment) {
    EXPECT_CALL(*file_pool, GetFilePoolOpt())
        .WillRepeatedly(Return(fp_option));
    EXPECT_CALL(*file_pool, GetFile(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*file_pool, RecycleFile(_))
        .WillOnce(Return(0));
    scoped_refptr<CurveSegment> seg1 =
                new CurveSegment(kRaftLogDataDir, 1, 0, file_pool);

    // not open
    braft::LogEntry* entry = seg1->get(1);
    ASSERT_TRUE(entry == NULL);

    // create and open
    std::string path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 1);
    ASSERT_EQ(0, prepare_segment(path));
    ASSERT_EQ(0, seg1->create());
    ASSERT_TRUE(seg1->is_open());

    // append entry
    append_entries_curve_segment(seg1);

    // read entry
    read_entries_curve_segment(seg1);
    {
        braft::LogEntry* entry = seg1->get(0);
        ASSERT_TRUE(entry == NULL);
        entry = seg1->get(11);
        ASSERT_TRUE(entry == NULL);
    }

    braft::ConfigurationManager* configuration_manager =
                                new braft::ConfigurationManager;
    // load open segment
    scoped_refptr<CurveSegment> seg2 =
                        new CurveSegment(kRaftLogDataDir, 1, 0, file_pool);
    ASSERT_EQ(0, seg2->load(configuration_manager));

    read_entries_curve_segment(seg2);
    {
        braft::LogEntry* entry = seg1->get(0);
        ASSERT_TRUE(entry == NULL);
        entry = seg1->get(11);
        ASSERT_TRUE(entry == NULL);
    }

    // truncate and read
    ASSERT_EQ(0, seg1->truncate(5));
    append_entries_curve_segment(seg1, "HELLO, WORLD: %d", 5, 10);
    read_entries_curve_segment(seg1, "hello, world: %d", 0, 5);
    read_entries_curve_segment(seg1, "HELLO, WORLD: %d", 5, 10);
    ASSERT_EQ(0, seg1->close());
    ASSERT_FALSE(seg1->is_open());
    ASSERT_EQ(0, seg1->unlink());

    delete configuration_manager;
}

TEST_F(CurveSegmentTest, closed_segment) {
    EXPECT_CALL(*file_pool, GetFilePoolOpt())
        .WillRepeatedly(Return(fp_option));
    EXPECT_CALL(*file_pool, GetFile(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*file_pool, RecycleFile(_))
        .WillOnce(Return(0));
    scoped_refptr<CurveSegment> seg1 =
                new CurveSegment(kRaftLogDataDir, 1, 0, file_pool);

    // create and open
    std::string path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 1);
    ASSERT_EQ(0, prepare_segment(path));
    ASSERT_EQ(0, seg1->create());
    ASSERT_TRUE(seg1->is_open());

    // append entry
    append_entries_curve_segment(seg1);
    seg1->close();

    // read entry
    read_entries_curve_segment(seg1);
    {
        braft::LogEntry* entry = seg1->get(0);
        ASSERT_TRUE(entry == NULL);
        entry = seg1->get(11);
        ASSERT_TRUE(entry == NULL);
    }

    braft::ConfigurationManager* configuration_manager =
                                new braft::ConfigurationManager;
    // load closed segment
    scoped_refptr<CurveSegment> seg2 =
                        new CurveSegment(kRaftLogDataDir, 1, 10, 0, file_pool);
    ASSERT_EQ(0, seg2->load(configuration_manager));

    read_entries_curve_segment(seg2);
    {
        braft::LogEntry* entry = seg1->get(0);
        ASSERT_TRUE(entry == NULL);
        entry = seg1->get(11);
        ASSERT_TRUE(entry == NULL);
    }

    // truncate and read
    ASSERT_EQ(0, seg1->truncate(5));
    append_entries_curve_segment(seg1, "HELLO, WORLD: %d", 5, 10);
    read_entries_curve_segment(seg1, "hello, world: %d", 0, 5);
    read_entries_curve_segment(seg1, "HELLO, WORLD: %d", 5, 10);
    ASSERT_EQ(0, seg1->unlink());

    delete configuration_manager;
}

}  // namespace chunkserver
}  // namespace curve
