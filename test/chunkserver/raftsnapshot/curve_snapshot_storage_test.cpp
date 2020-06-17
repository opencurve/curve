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
 * Created Date: 2020-06-18
 * Author: charisu
 */

// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/10/08 17:00:05

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <brpc/server.h>
#include "src/chunkserver/raftsnapshot/curve_snapshot_storage.h"
#include "src/chunkserver/raftsnapshot/curve_file_service.h"

namespace braft {
DECLARE_int64(raft_minimal_throttle_threshold_mb);
}

namespace curve {
namespace chunkserver {

const char serverAddr[] = "127.0.0.1:9502";

class CurveSnapshotStorageTest : public testing::Test {
 protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(CurveSnapshotStorageTest, writer_and_reader) {
    scoped_refptr<braft::PosixFileSystemAdaptor> fs(
                new braft::PosixFileSystemAdaptor());
    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }
    braft::SnapshotStorage* storage = new CurveSnapshotStorage("./data");
    ASSERT_EQ(storage->set_file_system_adaptor(fs), 0);
    ASSERT_TRUE(storage);
    ASSERT_EQ(0, storage->init());

    // empty snapshot
    braft::SnapshotReader* reader = storage->open();
    ASSERT_TRUE(reader == NULL);

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);

    // normal create writer
    braft::SnapshotWriter* writer = storage->create();
    ASSERT_TRUE(writer != NULL);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage->close(writer));

    // normal create writer again
    meta.set_last_included_index(2000);
    meta.set_last_included_term(2);
    writer = storage->create();
    ASSERT_TRUE(writer != NULL);

    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage->close(writer));

    // normal open reader
    reader = storage->open();
    ASSERT_TRUE(reader != NULL);
    braft::SnapshotMeta new_meta;
    ASSERT_EQ(0, reader->load_meta(&new_meta));
    ASSERT_EQ(meta.last_included_index(), new_meta.last_included_index());
    ASSERT_EQ(meta.last_included_term(), new_meta.last_included_term());
    reader->set_error(EIO, "read failed");
    storage->close(reader);

    delete storage;

    // reinit
    storage = new CurveSnapshotStorage("./data");
    ASSERT_EQ(storage->set_file_system_adaptor(fs), 0);
    ASSERT_TRUE(storage);
    ASSERT_EQ(0, storage->init());

    // normal create writer after reinit
    meta.set_last_included_index(3000);
    meta.set_last_included_term(3);
    writer = storage->create();
    ASSERT_TRUE(writer != NULL);
    ASSERT_TRUE(dynamic_cast<CurveSnapshotWriter*>(writer));
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ("./data/temp", writer->get_path());
    ASSERT_EQ(0, storage->close(writer));

    // normal open reader after reinit
    reader = storage->open();
    ASSERT_TRUE(reader != NULL);
    ASSERT_TRUE(dynamic_cast<CurveSnapshotReader*>(reader));
    braft::SnapshotMeta new_meta2;
    ASSERT_EQ(0, reader->load_meta(&new_meta2));
    ASSERT_EQ(meta.last_included_index(), new_meta2.last_included_index());
    ASSERT_EQ(meta.last_included_term(), new_meta2.last_included_term());
    storage->close(reader);

    // normal create writer after reinit
    meta.Clear();
    meta.set_last_included_index(5000);
    meta.set_last_included_term(4);
    for (int i = 1; i <= 3; ++i) {
        meta.add_peers("127.0.0.1:" + std::to_string(i));
    }
    for (int i = 4; i <= 6; ++i) {
        meta.add_old_peers("127.0.0.1:" + std::to_string(i));
    }
    writer = storage->create();
    ASSERT_TRUE(writer != NULL);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ("./data/temp", writer->get_path());
    ASSERT_EQ(0, storage->close(writer));

    reader = storage->open();
    ASSERT_TRUE(reader != NULL);
    braft::SnapshotMeta new_meta3;
    ASSERT_EQ(0, reader->load_meta(&new_meta3));
    ASSERT_EQ(meta.last_included_index(), new_meta3.last_included_index());
    ASSERT_EQ(meta.last_included_term(), new_meta3.last_included_term());
    storage->close(reader);

    ASSERT_EQ(new_meta3.peers_size(), meta.peers_size());
    ASSERT_EQ(new_meta3.old_peers_size(), meta.old_peers_size());

    for (int i = 0; i < new_meta3.peers_size(); ++i) {
        ASSERT_EQ(new_meta3.peers(i), meta.peers(i));
    }

    for (int i = 0; i < new_meta3.old_peers_size(); ++i) {
        ASSERT_EQ(new_meta3.old_peers(i), meta.old_peers(i));
    }
    delete storage;
}

TEST_F(CurveSnapshotStorageTest, copy) {
    scoped_refptr<braft::PosixFileSystemAdaptor> fs(
                new braft::PosixFileSystemAdaptor());
    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&kCurveFileService,
                                   brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(serverAddr, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage
    CurveSnapshotStorage* storage1 = new CurveSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    butil::EndPoint ep;
    ASSERT_EQ(0, butil::str2endpoint(serverAddr, &ep));
    storage1->set_server_addr(ep);
    // normal create writer
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    CurveSnapshotStorage* storage2 = new CurveSnapshotStorage("./data2");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage2->init());
    braft::SnapshotReader* reader2 = storage2->copy_from(uri);
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));

    delete storage2;
    delete storage1;
}

TEST_F(CurveSnapshotStorageTest, file_escapes_directory) {
    scoped_refptr<braft::PosixFileSystemAdaptor> fs(
                new braft::PosixFileSystemAdaptor());

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&kCurveFileService,
                                   brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(serverAddr, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    CurveSnapshotStorage* storage1
            = new CurveSnapshotStorage("./data/snapshot1/data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    if (!fs) {
        ASSERT_EQ(0, system("mkdir -p ./data/snapshot1/dir1/ &&"
                                " touch ./data/snapshot1/dir1/file"));
    } else {
        ASSERT_TRUE(fs->create_directory("./data/snapshot1/dir1/", NULL, true));
        braft::FileAdaptor* file = fs->open("./data/snapshot1/dir1/file",
                O_CREAT | O_TRUNC | O_RDWR, NULL, NULL);
        CHECK(file != NULL);
        delete file;
    }
    butil::EndPoint ep;
    ASSERT_EQ(0, butil::str2endpoint(serverAddr, &ep));
    storage1->set_server_addr(ep);
    // normal create writer
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    ASSERT_EQ(0, writer1->add_file("../../dir1/file"));
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    CurveSnapshotStorage* storage2
            = new CurveSnapshotStorage("./data/snapshot2/data");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage2->init());
    braft::SnapshotReader* reader2 = storage2->copy_from(uri);
    if (!fs) {
        ASSERT_TRUE(butil::PathExists(
            butil::FilePath("./data/snapshot2/dir1/file")));
    } else {
        ASSERT_TRUE(fs->path_exists(reader2->get_path() + "/dir1/file"));
    }
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));
    delete storage2;
    delete storage1;
}

void write_file(braft::FileSystemAdaptor* fs, const std::string& path,
                const std::string& data) {
    if (!fs) {
        fs = braft::default_file_system();
    }
    braft::FileAdaptor* file = fs->open(
                    path, O_CREAT | O_TRUNC | O_RDWR, NULL, NULL);
    CHECK(file != NULL);
    butil::IOBuf io_buf;
    io_buf.append(data);
    CHECK_EQ(data.size(), file->write(io_buf, 0));
    delete file;
}

void add_file_meta(braft::FileSystemAdaptor* fs, braft::SnapshotWriter* writer,
            int index, const std::string* checksum, const std::string& data) {
    std::stringstream path;
    path << "file" << index;
    braft::LocalFileMeta file_meta;
    if (checksum) {
        file_meta.set_checksum(*checksum);
    }
    write_file(fs, writer->get_path() + "/" + path.str(),
                                    path.str() + ": " + data);
    ASSERT_EQ(0, writer->add_file(path.str(), &file_meta));
}

void add_file_without_meta(braft::FileSystemAdaptor* fs,
                           braft::SnapshotWriter* writer, int index,
                           const std::string& data) {
    std::stringstream path;
    path << "file" << index;
    write_file(fs, writer->get_path() + "/" + path.str(),
                            path.str() + ": " + data);
}

bool check_file_exist(braft::FileSystemAdaptor* fs, const std::string& path,
                      int index) {
    if (fs == NULL) {
        fs = braft::default_file_system();
    }
    std::stringstream ss;
    ss << path << "/file" << index;
    return fs->path_exists(ss.str());
}

std::string read_from_file(braft::FileSystemAdaptor* fs,
                           const std::string& path, int index) {
    if (fs == NULL) {
        fs = braft::default_file_system();
    }
    std::stringstream ss;
    ss << path << "/file" << index;
    braft::FileAdaptor* file = fs->open(ss.str(), O_RDONLY, NULL, NULL);
    ssize_t size = file->size();
    butil::IOPortal buf;
    file->read(&buf, 0, size_t(size));
    delete file;
    return buf.to_string();
}

TEST_F(CurveSnapshotStorageTest, filter_before_copy) {
    scoped_refptr<braft::PosixFileSystemAdaptor> fs(
                new braft::PosixFileSystemAdaptor());

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&kCurveFileService,
                                   brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(serverAddr, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage
    CurveSnapshotStorage* storage1 = new CurveSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    butil::EndPoint ep;
    ASSERT_EQ(0, butil::str2endpoint(serverAddr, &ep));
    storage1->set_server_addr(ep);
    // normal create writer
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);

    const std::string data1("aaa");
    const std::string checksum1("1");
    add_file_meta(fs, writer1, 1, &checksum1, data1);
    add_file_meta(fs, writer1, 2, NULL, data1);
    add_file_meta(fs, writer1, 3, &checksum1, data1);
    add_file_meta(fs, writer1, 4, &checksum1, data1);
    add_file_meta(fs, writer1, 5, &checksum1, data1);
    add_file_meta(fs, writer1, 6, &checksum1, data1);
    add_file_meta(fs, writer1, 7, NULL, data1);
    add_file_meta(fs, writer1, 8, &checksum1, data1);
    add_file_meta(fs, writer1, 9, &checksum1, data1);

    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
        ::system("rm -rf snapshot_temp");
    } else {
        fs->delete_file("data2", true);
        fs->delete_file("snapshot_temp", true);
    }

    CurveSnapshotStorage* storage2 = new CurveSnapshotStorage("./data2");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    storage2->set_filter_before_copy_remote();
    ASSERT_EQ(0, storage2->init());

    braft::SnapshotWriter* writer2 = storage2->create();
    ASSERT_TRUE(writer2 != NULL);

    meta.set_last_included_index(900);
    meta.set_last_included_term(1);
    const std::string& data2("bbb");
    const std::string& checksum2("2");
    // same checksum, will not copy
    add_file_meta(fs, writer2, 1, &checksum1, data2);
    // remote checksum not set, local set, will copy
    add_file_meta(fs, writer2, 2, &checksum1, data2);
    // remote checksum set, local not set, will copy
    add_file_meta(fs, writer2, 3, NULL, data2);
    // different checksum, will copy
    add_file_meta(fs, writer2, 4, &checksum2, data2);
    // file not exist in remote, will delete
    add_file_meta(fs, writer2, 100, &checksum2, data2);
    // file exit but meta not exit, will delete
    add_file_without_meta(fs, writer2, 102, data2);

    ASSERT_EQ(0, writer2->save_meta(meta));
    ASSERT_EQ(0, storage2->close(writer2));
    if (fs == NULL) {
        ::system("mv data2/snapshot_00000000000000000900 snapshot_temp");
    } else {
        fs->rename("data2/snapshot_00000000000000000900", "snapshot_temp");
    }

    writer2 = storage2->create();
    ASSERT_TRUE(writer2 != NULL);

    meta.set_last_included_index(901);
    const std::string data3("ccc");
    const std::string checksum3("3");
    // same checksum, will copy from last_snapshot with index=901
    add_file_meta(fs, writer2, 6, &checksum1, data3);
    // remote checksum not set, local last_snapshot set, will copy
    add_file_meta(fs, writer2, 7, &checksum1, data3);
    // remote checksum set, local last_snapshot not set, will copy
    add_file_meta(fs, writer2, 8, NULL, data3);
    // remote and local last_snapshot different checksum, will copy
    add_file_meta(fs, writer2, 9, &checksum3, data3);
    // file not exist in remote, will not copy
    add_file_meta(fs, writer2, 101, &checksum3, data3);
    ASSERT_EQ(0, writer2->save_meta(meta));
    ASSERT_EQ(0, storage2->close(writer2));

    if (fs == NULL) {
        ::system("mv snapshot_temp data2/temp");
    } else {
        fs->rename("snapshot_temp", "data2/temp");
    }

    ASSERT_EQ(0, storage2->init());
    braft::SnapshotReader* reader2 = storage2->copy_from(uri);
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));

    const std::string snapshot_path("data2/snapshot_00000000000000001000");
    for (int i = 1; i <= 9; ++i) {
        ASSERT_TRUE(check_file_exist(fs, snapshot_path, i));
        std::stringstream content;
        content << "file" << i << ": ";
        if (i == 1) {
            content << data2;
        } else if (i == 6) {
            content << data3;
        } else {
            content << data1;
        }
        ASSERT_EQ(content.str(), read_from_file(fs, snapshot_path, i));
    }
    ASSERT_TRUE(!check_file_exist(fs, snapshot_path, 100));
    ASSERT_TRUE(!check_file_exist(fs, snapshot_path, 101));
    ASSERT_TRUE(!check_file_exist(fs, snapshot_path, 102));

    delete storage2;
    delete storage1;
}

TEST_F(CurveSnapshotStorageTest, snapshot_throttle_for_reading) {
    scoped_refptr<braft::PosixFileSystemAdaptor> fs(
                new braft::PosixFileSystemAdaptor());

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&kCurveFileService,
                                   brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(serverAddr, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    CurveSnapshotStorage* storage1 = new CurveSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    // create and set snapshot throttle for storage1
    braft::ThroughputSnapshotThrottle* throttle =
            new braft::ThroughputSnapshotThrottle(60, 10);
    ASSERT_TRUE(throttle);
    ASSERT_EQ(storage1->set_snapshot_throttle(throttle), 0);
    ASSERT_EQ(0, storage1->init());
    butil::EndPoint ep;
    ASSERT_EQ(0, butil::str2endpoint(serverAddr, &ep));
    storage1->set_server_addr(ep);
    // normal create writer
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    // add file meta for storage1
    const std::string data1("aaa");
    const std::string checksum1("1");
    add_file_meta(fs, writer1, 1, &checksum1, data1);

    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    CurveSnapshotStorage* storage2 = new CurveSnapshotStorage("./data2");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage2->init());
    // copy
    braft::SnapshotReader* reader2 = storage2->copy_from(uri);
    LOG(INFO) << "Copy finish.";
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));
    delete storage2;
    delete storage1;
}

TEST_F(CurveSnapshotStorageTest, snapshot_throttle_for_writing) {
    scoped_refptr<braft::PosixFileSystemAdaptor> fs(
                new braft::PosixFileSystemAdaptor());

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&kCurveFileService,
                                   brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(serverAddr, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    CurveSnapshotStorage* storage1 = new CurveSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    butil::EndPoint ep;
    ASSERT_EQ(0, butil::str2endpoint(serverAddr, &ep));
    storage1->set_server_addr(ep);
    // create writer1
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    // add nomal file for storage1
    LOG(INFO) << "add nomal file";
    const std::string data1("aaa");
    const std::string checksum1("1000");
    add_file_meta(fs, writer1, 1, &checksum1, data1);
    // save snapshot meta for storage1
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));
    // get uri of storage1
    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    CurveSnapshotStorage* storage2 = new CurveSnapshotStorage("./data2");
    // create and set snapshot throttle for storage2
    braft::ThroughputSnapshotThrottle* throttle2 = new
        braft::ThroughputSnapshotThrottle(3 * 1000 * 1000, 10);
    ASSERT_TRUE(throttle2);
    ASSERT_EQ(storage2->set_snapshot_throttle(throttle2), 0);
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    // create and set snapshot throttle for storage2
    braft::SnapshotThrottle* throttle =
        new braft::ThroughputSnapshotThrottle(20, 10);
    ASSERT_TRUE(throttle);
    ASSERT_EQ(storage2->set_snapshot_throttle(throttle), 0);
    ASSERT_EQ(0, storage2->init());

    // copy from storage1 to storage2
    LOG(INFO) << "Copy start.";
    braft::SnapshotCopier* copier = storage2->start_to_copy_from(uri);
    ASSERT_TRUE(copier != NULL);
    copier->join();
    LOG(INFO) << "Copy finish.";
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(copier));
    delete storage2;
    delete storage1;
}

TEST_F(CurveSnapshotStorageTest,
                snapshot_throttle_for_reading_without_enable_throttle) {
    braft::FLAGS_raft_enable_throttle_when_install_snapshot = false;
    scoped_refptr<braft::PosixFileSystemAdaptor> fs(
                new braft::PosixFileSystemAdaptor());

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&kCurveFileService,
                                   brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(serverAddr, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    CurveSnapshotStorage* storage1 = new CurveSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    // create and set snapshot throttle for storage1
    braft::ThroughputSnapshotThrottle* throttle = new
        braft::ThroughputSnapshotThrottle(30, 10);
    ASSERT_TRUE(throttle);
    ASSERT_EQ(storage1->set_snapshot_throttle(throttle), 0);
    ASSERT_EQ(0, storage1->init());
    butil::EndPoint ep;
    ASSERT_EQ(0, butil::str2endpoint(serverAddr, &ep));
    storage1->set_server_addr(ep);
    // normal create writer
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    // add file meta for storage1
    const std::string data1("aaa");
    const std::string checksum1("1");
    add_file_meta(fs, writer1, 1, &checksum1, data1);

    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));

    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    CurveSnapshotStorage* storage2 = new CurveSnapshotStorage("./data2");
    // create and set snapshot throttle for storage2
    braft::ThroughputSnapshotThrottle* throttle2 = new
        braft::ThroughputSnapshotThrottle(3 * 1000 * 1000, 10);
    ASSERT_TRUE(throttle2);
    ASSERT_EQ(storage2->set_snapshot_throttle(throttle2), 0);
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage2->init());
    // copy
    braft::SnapshotReader* reader2 = storage2->copy_from(uri);
    LOG(INFO) << "Copy finish.";
    ASSERT_TRUE(reader2 != NULL);
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(reader2));
    delete storage2;
    delete storage1;

    braft::FLAGS_raft_enable_throttle_when_install_snapshot = true;
}

TEST_F(CurveSnapshotStorageTest,
                snapshot_throttle_for_writing_without_enable_throttle) {
    braft::FLAGS_raft_enable_throttle_when_install_snapshot = false;
    scoped_refptr<braft::PosixFileSystemAdaptor> fs(
                new braft::PosixFileSystemAdaptor());

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&kCurveFileService,
                                   brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(serverAddr, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    CurveSnapshotStorage* storage1 = new CurveSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    butil::EndPoint ep;
    ASSERT_EQ(0, butil::str2endpoint(serverAddr, &ep));
    storage1->set_server_addr(ep);
    // create writer1
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    // add nomal file for storage1
    LOG(INFO) << "add nomal file";
    const std::string data1("aaa");
    const std::string checksum1("1000");
    add_file_meta(fs, writer1, 1, &checksum1, data1);
    // save snapshot meta for storage1
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));
    // get uri of storage1
    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    CurveSnapshotStorage* storage2 = new CurveSnapshotStorage("./data2");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    // create and set snapshot throttle for storage2
    braft::SnapshotThrottle* throttle =
        new braft::ThroughputSnapshotThrottle(20, 10);
    ASSERT_TRUE(throttle);
    ASSERT_EQ(storage2->set_snapshot_throttle(throttle), 0);
    ASSERT_EQ(0, storage2->init());

    // copy from storage1 to storage2
    LOG(INFO) << "Copy start.";
    braft::SnapshotCopier* copier = storage2->start_to_copy_from(uri);
    ASSERT_TRUE(copier != NULL);
    copier->join();
    LOG(INFO) << "Copy finish.";
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(copier));
    delete storage2;
    delete storage1;

    braft::FLAGS_raft_enable_throttle_when_install_snapshot = true;
}

TEST_F(CurveSnapshotStorageTest, dynamically_change_throttle_threshold) {
    braft::FLAGS_raft_minimal_throttle_threshold_mb = 1;
    scoped_refptr<braft::PosixFileSystemAdaptor> fs(
                new braft::PosixFileSystemAdaptor());

    if (fs == NULL) {
        ::system("rm -rf data");
    } else {
        fs->delete_file("data", true);
    }

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&kCurveFileService,
                                   brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(serverAddr, NULL));

    std::vector<braft::PeerId> peers;
    peers.push_back(braft::PeerId("1.2.3.4:1000"));
    peers.push_back(braft::PeerId("1.2.3.4:2000"));
    peers.push_back(braft::PeerId("1.2.3.4:3000"));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(1000);
    meta.set_last_included_term(2);
    for (size_t i = 0; i < peers.size(); ++i) {
        *meta.add_peers() = peers[i].to_string();
    }

    // storage1
    CurveSnapshotStorage* storage1 = new CurveSnapshotStorage("./data");
    ASSERT_TRUE(storage1);
    if (fs) {
        ASSERT_EQ(storage1->set_file_system_adaptor(fs), 0);
    }
    ASSERT_EQ(0, storage1->init());
    butil::EndPoint ep;
    ASSERT_EQ(0, butil::str2endpoint(serverAddr, &ep));
    storage1->set_server_addr(ep);
    // create writer1
    braft::SnapshotWriter* writer1 = storage1->create();
    ASSERT_TRUE(writer1 != NULL);
    // add nomal file for storage1
    LOG(INFO) << "add nomal file";
    const std::string data1("aaa");
    const std::string checksum1("1000");
    add_file_meta(fs, writer1, 1, &checksum1, data1);
    // save snapshot meta for storage1
    ASSERT_EQ(0, writer1->save_meta(meta));
    ASSERT_EQ(0, storage1->close(writer1));
    // get uri of storage1
    braft::SnapshotReader* reader1 = storage1->open();
    ASSERT_TRUE(reader1 != NULL);
    std::string uri = reader1->generate_uri_for_copy();

    // storage2
    if (fs == NULL) {
        ::system("rm -rf data2");
    } else {
        fs->delete_file("data2", true);
    }
    CurveSnapshotStorage* storage2 = new CurveSnapshotStorage("./data2");
    if (fs) {
        ASSERT_EQ(storage2->set_file_system_adaptor(fs), 0);
    }
    // create and set snapshot throttle for storage2
    braft::SnapshotThrottle* throttle =
        new braft::ThroughputSnapshotThrottle(10, 10);
    ASSERT_TRUE(throttle);
    ASSERT_EQ(storage2->set_snapshot_throttle(throttle), 0);
    ASSERT_EQ(0, storage2->init());

    // copy from storage1 to storage2
    LOG(INFO) << "Copy start.";
    braft::SnapshotCopier* copier = storage2->start_to_copy_from(uri);
    ASSERT_TRUE(copier != NULL);
    copier->join();
    LOG(INFO) << "Copy finish.";
    ASSERT_EQ(0, storage1->close(reader1));
    ASSERT_EQ(0, storage2->close(copier));
    delete storage2;
    delete storage1;

    braft::FLAGS_raft_minimal_throttle_threshold_mb = 0;
}

}  // namespace chunkserver
}  // namespace curve
