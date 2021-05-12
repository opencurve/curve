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

#include <gtest/gtest.h>
#include <braft/log.h>
#include <fcntl.h>

#include <cstdio>
#include <memory>
#include <string>
#include <array>

#include "src/chunkserver/raftlog/curve_segment_log_storage.h"
#include "src/chunkserver/raftlog/define.h"
#include "test/fs/mock_local_filesystem.h"
#include "test/chunkserver/datastore/mock_file_pool.h"
#include "test/chunkserver/raftlog/common.h"

namespace curve {
namespace chunkserver {

using curve::fs::MockLocalFileSystem;
using ::testing::Return;
using ::testing::Invoke;
using ::testing::_;

class CurveSegmentLogStorageTest : public testing::Test {
 protected:
    CurveSegmentLogStorageTest() {
        fp_option.metaPageSize = kPageSize;
        fp_option.fileSize = kSegmentSize;
    }
    void SetUp() {
        lfs = std::make_shared<MockLocalFileSystem>();
        file_pool = std::make_shared<MockFilePool>(lfs);
        std::string cmd = std::string("mkdir ") + kRaftLogDataDir;
        ::system(cmd.c_str());

        auto recycleFile = [this](const std::string& chunkpath)->int {
            std::string cmd = "rm -rf " + chunkpath;
            execShell(cmd);
        };
        EXPECT_CALL(*file_pool, GetFilePoolOpt())
            .WillRepeatedly(Return(fp_option));
        EXPECT_CALL(*file_pool, GetFileImpl(_, _))
            .WillRepeatedly(Return(0));
        EXPECT_CALL(*file_pool, RecycleFile(_))
            .WillRepeatedly(Invoke(recycleFile));
    }
    void TearDown() {
        std::string cmd = std::string("rm -rf ") + kRaftLogDataDir;
        ::system(cmd.c_str());
    }
    void append_entries(std::shared_ptr<braft::LogStorage> storage,
                        int m, int n) {
        for (int i = 0; i < m; i++) {
            std::vector<braft::LogEntry*> entries;
            for (int j = 0; j < n; j++) {
                int64_t index = n*i + j + 1;
                braft::LogEntry* entry = new braft::LogEntry();
                entry->type = braft::ENTRY_TYPE_DATA;
                entry->id.term = 1;
                entry->id.index = index;

                char data_buf[128];
                snprintf(data_buf, sizeof(data_buf),
                            "hello, world: %" PRId64, index);
                entry->data.append(data_buf);
                entries.push_back(entry);
            }

            ASSERT_EQ(n, storage->append_entries(entries));
        }
    }
    void read_entries(std::shared_ptr<CurveSegmentLogStorage> storage,
                      int start, int end) {
        for (int i = start; i < end; i++) {
            int64_t index = i + 1;
            braft::LogEntry* entry = storage->get_entry(index);
            ASSERT_EQ(entry->id.term, 1);
            ASSERT_EQ(entry->type, braft::ENTRY_TYPE_DATA);
            ASSERT_EQ(entry->id.index, index);

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf),
                        "hello, world: %" PRId64, index);
            ASSERT_EQ(data_buf, entry->data.to_string());
        }
    }

    std::string execShell(const string& cmd) {
        std::array<char, 128> buffer;
        std::string result;
        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                      pclose);
        if (!pipe) {
            throw std::runtime_error("popen() failed!");
        }
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            result += buffer.data();
        }
        return result;
    }

    int countWalSegmentFile() {
        char cmd[128];
        snprintf(cmd, sizeof(cmd),
            "ls %s | grep -E '^(curve_)?log_(inprogress|[0-9]+)_[0-9]+$' -c",
            kRaftLogDataDir);
        std::string output = execShell(string(cmd));
        return "" != output ? std::stoi(output) : 0;
    }

    std::shared_ptr<MockLocalFileSystem> lfs;
    std::shared_ptr<MockFilePool> file_pool;
    FilePoolOptions fp_option;
};

TEST_F(CurveSegmentLogStorageTest, basic_test) {
    auto storage = std::make_shared<CurveSegmentLogStorage>(kRaftLogDataDir,
            true, file_pool);

    // init
    ASSERT_EQ(0, storage->init(new braft::ConfigurationManager()));
    ASSERT_EQ(1, storage->first_log_index());
    ASSERT_EQ(0, storage->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // append entry
    std::string path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 1);
    ASSERT_EQ(0,  prepare_segment(path));
    path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 2049);
    ASSERT_EQ(0,  prepare_segment(path));
    path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 4097);
    ASSERT_EQ(0,  prepare_segment(path));
    append_entries(storage, 1000, 5);

    // read entry
    read_entries(storage, 0, 5000);
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 5000);
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // truncate prefix
    ASSERT_EQ(0, storage->truncate_prefix(1001));
    ASSERT_EQ(storage->first_log_index(), 1001);
    ASSERT_EQ(storage->last_log_index(), 5000);
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // boundary truncate prefix
    {
        auto& segments1 = storage->segments();
        size_t old_segment_num = segments1.size();
        auto first_seg = segments1.begin()->second.get();

        ASSERT_EQ(0, storage->truncate_prefix(first_seg->last_index()));
        auto& segments2 = storage->segments();
        ASSERT_EQ(old_segment_num, segments2.size());

        ASSERT_EQ(0, storage->truncate_prefix(first_seg->last_index() + 1));
        auto& segments3 = storage->segments();
        ASSERT_EQ(old_segment_num - 1, segments3.size());
    }

    ASSERT_EQ(0, storage->truncate_prefix(2100));
    ASSERT_EQ(storage->first_log_index(), 2100);
    ASSERT_EQ(storage->last_log_index(), 5000);
    read_entries(storage, 2100, 5000);
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // append
    path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 6145);
    ASSERT_EQ(0,  prepare_segment(path));
    for (int i = 5001; i <= 7000; i++) {
        int64_t index = i;
        braft::LogEntry* entry = new braft::LogEntry();
        entry->type = braft::ENTRY_TYPE_DATA;
        entry->id.term = 1;
        entry->id.index = index;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %" PRId64, index);
        entry->data.append(data_buf);
        ASSERT_EQ(0, storage->append_entry(entry));
    }

    // truncate suffix
    ASSERT_EQ(2100, storage->first_log_index());
    ASSERT_EQ(7000, storage->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);
    ASSERT_EQ(0, storage->truncate_suffix(6200));
    ASSERT_EQ(2100, storage->first_log_index());
    ASSERT_EQ(6200, storage->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // boundary truncate suffix
    {
        auto& segments1 = storage->segments();
        LOG(INFO) << "segments num: " << segments1.size();
        auto first_seg = segments1.begin()->second.get();
        if (segments1.size() > 1) {
            storage->truncate_suffix(first_seg->last_index() + 1);
        }
        auto& segments2 = storage->segments();
        ASSERT_EQ(2, segments2.size());
        ASSERT_EQ(storage->last_log_index(), first_seg->last_index() + 1);
        storage->truncate_suffix(first_seg->last_index());
        auto segments3 = storage->segments();
        ASSERT_EQ(1, segments3.size());
        ASSERT_EQ(storage->last_log_index(), first_seg->last_index());
    }

    // read
    read_entries(storage, 2100, storage->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // reload from the existing log data
    auto storage2 = std::make_shared<CurveSegmentLogStorage>(kRaftLogDataDir,
            true, file_pool);
    ASSERT_EQ(0, storage2->init(new braft::ConfigurationManager()));
    ASSERT_EQ(storage->first_log_index(), storage2->first_log_index());
    ASSERT_EQ(storage->last_log_index(), storage2->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage2->GetStatus().walSegmentFileCount);

    // reload from empty log data
    std::string cmd = std::string("rm -rf ") + kRaftLogDataDir + "/log_meta";
    execShell(cmd);
    auto storage3 = std::make_shared<CurveSegmentLogStorage>(kRaftLogDataDir,
            true, file_pool);
    ASSERT_EQ(0, storage3->init(new braft::ConfigurationManager()));
    ASSERT_EQ(1, storage3->first_log_index());
    ASSERT_EQ(0, storage3->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage3->GetStatus().walSegmentFileCount);
}

TEST_F(CurveSegmentLogStorageTest, append_close_load_append) {
    auto storage = std::make_shared<CurveSegmentLogStorage>(kRaftLogDataDir,
            true, file_pool);
    braft::ConfigurationManager* configuration_manager =
                                new braft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // append entry
    std::string path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 1);
    ASSERT_EQ(0,  prepare_segment(path));
    path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 2049);
    ASSERT_EQ(0,  prepare_segment(path));
    append_entries(storage, 600, 5);
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    storage = nullptr;
    delete configuration_manager;

    // reinit
    storage = std::make_shared<CurveSegmentLogStorage>(kRaftLogDataDir,
            true, file_pool);
    configuration_manager = new braft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // append entry
    path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 4097);
    ASSERT_EQ(0,  prepare_segment(path));
    for (int i = 600; i < 1000; i++) {
        std::vector<braft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            braft::LogEntry* entry = new braft::LogEntry();
            entry->type = braft::ENTRY_TYPE_DATA;
            entry->id.term = 2;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf),
                    "hello, world: %" PRId64, index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        ASSERT_EQ(5, storage->append_entries(entries));
    }

    // check and read
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 5000);
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    for (int i = 0; i < 5000; i++) {
        int64_t index = i + 1;
        braft::LogEntry* entry = storage->get_entry(index);
        if (i < 3000) {
            ASSERT_EQ(entry->id.term, 1);
        } else {
            ASSERT_EQ(entry->id.term, 2);
        }
        ASSERT_EQ(entry->type, braft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %" PRId64, index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }
    delete configuration_manager;
}

TEST_F(CurveSegmentLogStorageTest, data_lost) {
    auto storage = std::make_shared<CurveSegmentLogStorage>(kRaftLogDataDir,
            true, file_pool);
    braft::ConfigurationManager* configuration_manager =
                            new braft::ConfigurationManager;
    ASSERT_EQ(0, storage->init(configuration_manager));
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // append entry
    std::string path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 1);
    ASSERT_EQ(0,  prepare_segment(path));
    append_entries(storage, 100, 5);
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    delete configuration_manager;

    // corrupt data
    int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    char data[4096];
    memset(data, 0, 4096);
    ASSERT_EQ(4096, ::pwrite(fd, data, 4096, 8192));
    storage = std::make_shared<CurveSegmentLogStorage>(kRaftLogDataDir,
            true, file_pool);
    configuration_manager = new braft::ConfigurationManager;
    ASSERT_NE(0, storage->init(configuration_manager));
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    delete configuration_manager;
}

TEST_F(CurveSegmentLogStorageTest, compatibility) {
    auto storage1 = std::make_shared<braft::SegmentLogStorage>(kRaftLogDataDir);
    // init
    braft::ConfigurationManager* configuration_manager =
                            new braft::ConfigurationManager;
    ASSERT_EQ(0, storage1->init(configuration_manager));
    ASSERT_EQ(1, storage1->first_log_index());
    ASSERT_EQ(0, storage1->last_log_index());

    // append entry
    append_entries(storage1, 600, 5);
    delete configuration_manager;

    // reinit
    auto storage2 = std::make_shared<CurveSegmentLogStorage>(kRaftLogDataDir,
            true, file_pool);
    configuration_manager = new braft::ConfigurationManager;
    ASSERT_EQ(0, storage2->init(configuration_manager));
    ASSERT_EQ(countWalSegmentFile(), storage2->GetStatus().walSegmentFileCount);

    // append entry
    std::string path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 3001);
    ASSERT_EQ(0,  prepare_segment(path));
    for (int i = 600; i < 1000; i++) {
        std::vector<braft::LogEntry*> entries;
        for (int j = 0; j < 5; j++) {
            int64_t index = 5*i + j + 1;
            braft::LogEntry* entry = new braft::LogEntry();
            entry->type = braft::ENTRY_TYPE_DATA;
            entry->id.term = 2;
            entry->id.index = index;

            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf),
                    "hello, world: %" PRId64, index);
            entry->data.append(data_buf);
            entries.push_back(entry);
        }

        ASSERT_EQ(5, storage2->append_entries(entries));
    }
    // check and read
    ASSERT_EQ(storage2->first_log_index(), 1);
    ASSERT_EQ(storage2->last_log_index(), 5000);
    ASSERT_EQ(countWalSegmentFile(), storage2->GetStatus().walSegmentFileCount);

    for (int i = 0; i < 5000; i++) {
        int64_t index = i + 1;
        braft::LogEntry* entry = storage2->get_entry(index);
        if (i < 3000) {
            ASSERT_EQ(entry->id.term, 1);
        } else {
            ASSERT_EQ(entry->id.term, 2);
        }
        ASSERT_EQ(entry->type, braft::ENTRY_TYPE_DATA);
        ASSERT_EQ(entry->id.index, index);

        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %" PRId64, index);
        ASSERT_EQ(data_buf, entry->data.to_string());
        entry->Release();
    }
    delete configuration_manager;
}

TEST_F(CurveSegmentLogStorageTest, basic_test_without_direct) {
    FLAGS_enableWalDirectWrite = false;
    auto storage = std::make_shared<CurveSegmentLogStorage>(kRaftLogDataDir,
            true, file_pool);

    // init
    ASSERT_EQ(0, storage->init(new braft::ConfigurationManager()));
    ASSERT_EQ(1, storage->first_log_index());
    ASSERT_EQ(0, storage->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // append entry
    std::string path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 1);
    ASSERT_EQ(0,  prepare_segment(path));
    path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 2049);
    ASSERT_EQ(0,  prepare_segment(path));
    path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 4097);
    ASSERT_EQ(0,  prepare_segment(path));
    append_entries(storage, 1000, 5);

    // read entry
    read_entries(storage, 0, 5000);
    ASSERT_EQ(storage->first_log_index(), 1);
    ASSERT_EQ(storage->last_log_index(), 5000);
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // truncate prefix
    ASSERT_EQ(0, storage->truncate_prefix(1001));
    ASSERT_EQ(storage->first_log_index(), 1001);
    ASSERT_EQ(storage->last_log_index(), 5000);
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // boundary truncate prefix
    {
        auto& segments1 = storage->segments();
        size_t old_segment_num = segments1.size();
        auto first_seg = segments1.begin()->second.get();

        ASSERT_EQ(0, storage->truncate_prefix(first_seg->last_index()));
        auto& segments2 = storage->segments();
        ASSERT_EQ(old_segment_num, segments2.size());

        ASSERT_EQ(0, storage->truncate_prefix(first_seg->last_index() + 1));
        auto& segments3 = storage->segments();
        ASSERT_EQ(old_segment_num - 1, segments3.size());
    }

    ASSERT_EQ(0, storage->truncate_prefix(2100));
    ASSERT_EQ(storage->first_log_index(), 2100);
    ASSERT_EQ(storage->last_log_index(), 5000);
    read_entries(storage, 2100, 5000);
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // append
    path = kRaftLogDataDir;
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, 6145);
    ASSERT_EQ(0,  prepare_segment(path));
    for (int i = 5001; i <= 7000; i++) {
        int64_t index = i;
        braft::LogEntry* entry = new braft::LogEntry();
        entry->type = braft::ENTRY_TYPE_DATA;
        entry->id.term = 1;
        entry->id.index = index;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello, world: %" PRId64, index);
        entry->data.append(data_buf);
        ASSERT_EQ(0, storage->append_entry(entry));
    }

    // truncate suffix
    ASSERT_EQ(2100, storage->first_log_index());
    ASSERT_EQ(7000, storage->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);
    ASSERT_EQ(0, storage->truncate_suffix(6200));
    ASSERT_EQ(2100, storage->first_log_index());
    ASSERT_EQ(6200, storage->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // boundary truncate suffix
    {
        auto& segments1 = storage->segments();
        LOG(INFO) << "segments num: " << segments1.size();
        auto first_seg = segments1.begin()->second.get();
        if (segments1.size() > 1) {
            storage->truncate_suffix(first_seg->last_index() + 1);
        }
        auto& segments2 = storage->segments();
        ASSERT_EQ(2, segments2.size());
        ASSERT_EQ(storage->last_log_index(), first_seg->last_index() + 1);
        storage->truncate_suffix(first_seg->last_index());
        auto segments3 = storage->segments();
        ASSERT_EQ(1, segments3.size());
        ASSERT_EQ(storage->last_log_index(), first_seg->last_index());
    }

    // read
    read_entries(storage, 2100, storage->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);

    // re load
    std::string cmd = std::string("rm -rf ") + kRaftLogDataDir + "/log_meta";
    ::system(cmd.c_str());
    auto storage2 = std::make_shared<CurveSegmentLogStorage>(kRaftLogDataDir,
            true, file_pool);
    ASSERT_EQ(0, storage2->init(new braft::ConfigurationManager()));
    ASSERT_EQ(1, storage2->first_log_index());
    ASSERT_EQ(0, storage2->last_log_index());
    ASSERT_EQ(countWalSegmentFile(), storage->GetStatus().walSegmentFileCount);
}


}  // namespace chunkserver
}  // namespace curve
