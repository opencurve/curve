/*
 *  Copyright (c) 2021 NetEase Inc.
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

/**
 * Project: Curve
 * Created Date: 2021-07-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_DUMPFILE_H_
#define CURVEFS_SRC_METASERVER_STORAGE_DUMPFILE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <condition_variable>

#include "absl/strings/string_view.h"
#include "curvefs/src/metaserver/storage/iterator.h"

namespace curve {
namespace fs {

class LocalFileSystem;
class Ext4FileSystemImpl;

}  // namespace fs
}  // namespace curve

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::fs::LocalFileSystem;
using ::curve::fs::Ext4FileSystemImpl;

enum class DUMPFILE_ERROR {
    OK,
    FAILED,
    BAD_FD,
    READ_FAILED,
    WRITE_FAILED,
    CLOSE_FAILED,
    LIST_FAILED,
    FSTAT_FAILED,
    EXCEED_MAX_STRING_LENGTH,
    SYNC_FAILED,
    FORK_FAILED,
    WAITPID_FAILED,
    UNEXPECTED_SIGNAL,
    ENCOUNTER_EOF,
};

enum class DUMPFILE_LOAD_STATUS {
    COMPLETE,
    INCOMPLETE,
    INVALID_FILE,
    INVALID_MAGIC,
    INVALID_VERSION,
    INVALID_SIZE,
    INVALID_PAIRS,
    INVALID_CHECKSUM,
};

enum DumpFileVersion : uint8_t {
    // Version 1 dumps all metadata into a single file
    // including partitions/inodes/dentries/pending tx/s3 chunk info and
    // volume extent
    kDumpFileV1 = 1,
    // See below dumpfile format diagram, it shows the difference between
    // kDumpFileV1 and kDumpFileV2
    kDumpFileV2 = 2,
    // Version 3 only dumps partitions and pending tx into file based on
    // kDumpFileV2 (because they're not inserted into rocksdb), other metadata
    // is saved by rocksdb
    kDumpFileV3 = 3,
};

std::ostream& operator<<(std::ostream& os, DUMPFILE_ERROR code);

std::ostream& operator<<(std::ostream& os, DUMPFILE_LOAD_STATUS code);

class DumpFileClosure {
 public:
    DumpFileClosure(): runned_(false) {}

    void Runned() {
        std::lock_guard<std::mutex> lk(mtx_);
        runned_ = true;
        cond_.notify_one();
    }

    void WaitRunned() {
        std::unique_lock<std::mutex> lk(mtx_);
        cond_.wait(lk, [this]() { return runned_; });
    }

 private:
    bool runned_;
    std::mutex mtx_;
    std::condition_variable cond_;
};

class DumpFileIterator;

/*
 * dumpfile format:
 *
 * v1:
 *   +---------+---------+------+-----------------+-----------+
 *   | CURVEFS | version | size | key_value_pairs | check_sum |
 *   +---------+---------+------+-----------------+-----------+
 *      CURVEFS:  "CURVEFS" (7-bytes)
 *      version:  uint8_t   (1-byte)
 *      size:     uint64_t  (8-bytes)
 *      checksum: uint32_t  (4-bytes)
 *
 * v2:
 *   +---------+---------+-----------------+-----+-----------+
 *   | CURVEFS | version | key_value_pairs | EOF | check_sum |
 *   +---------+---------+-----------------+-----+-----------+
 *      CURVEFS:  "CURVEFS" (7-bytes)
 *      version:  uint8_t   (1-byte)
 *      EOF:      uint32_t  (4-bytes)
 *      checksum: uint32_t  (4-bytes)
 *
 * key_value_pairs format:
 *   +--------------+-------+----------------+---------+-----+--------------+-------+----------------+---------+
 *   | key_1_length | key_1 | value_1_length | value_1 | ... | key_n_length | key_n | value_n_length | value_n |
 *   +--------------+-------+----------------+---------+-----+--------------+-------+----------------+---------+
 *      *length: uint32_t (4-bytes)
 */
class DumpFile {
 public:
    explicit DumpFile(const std::string& pathname);

    DumpFile(const std::string& pathname, uint8_t version);

    DUMPFILE_ERROR Open();

    DUMPFILE_ERROR Close();

    DUMPFILE_ERROR Save(std::shared_ptr<Iterator> iter);

    DUMPFILE_ERROR SaveBackground(std::shared_ptr<Iterator> iter,
                                  DumpFileClosure* done = nullptr);

    std::shared_ptr<DumpFileIterator> Load();

    DUMPFILE_LOAD_STATUS GetLoadStatus();

    void SetLoadStatus(DUMPFILE_LOAD_STATUS status);

    uint8_t GetVersion();

 private:
    DUMPFILE_ERROR Write(const char* buffer, off_t offset, size_t length);

    DUMPFILE_ERROR Read(char* buffer, off_t offset, size_t length);

    template <typename Int>
    DUMPFILE_ERROR SaveInt(Int num, off_t* offset, uint32_t* checkSum);

    DUMPFILE_ERROR SaveString(absl::string_view str,
                              off_t* offset,
                              uint32_t* checkSum);

    DUMPFILE_ERROR SaveEntry(absl::string_view entry,
                             off_t* offset,
                             uint32_t* checkSum);

    template <typename Int>
    DUMPFILE_ERROR LoadInt(Int* num, off_t* offset, uint32_t* checkSum);

    DUMPFILE_ERROR LoadString(std::string* str,
                              off_t* offset,
                              size_t length,
                              uint32_t* checkSum);

    DUMPFILE_ERROR LoadEntry(std::string* entry,
                             off_t* offset,
                             uint32_t* checkSum);

    static void SignalHandler(int signo, siginfo_t* siginfo, void* ucontext);

    DUMPFILE_ERROR InitSignals();

    DUMPFILE_ERROR CloseSockets();

    void SaveWorker(std::shared_ptr<Iterator> iter);

    DUMPFILE_ERROR WaitSaveDone(pid_t childpid);

 private:
    friend class DumpFileIterator;
    friend class DumpFileTest;

    // Check whether a version is valid
    static bool CheckDumpFileVersion(uint8_t ver);

 private:
    std::string pathname_;

    int64_t fd_;

    std::shared_ptr<LocalFileSystem> fs_;

    DUMPFILE_LOAD_STATUS loadStatus_;

    uint8_t version_;

    static const std::string kCurvefs_;

    static const uint8_t kVersion_;

    static const uint32_t kEOF_;

    static const uint32_t kMaxStringLength_;
};

class DumpFileIterator : public Iterator {
 public:
    using Iter = std::pair<std::string, std::string>;

 public:
    explicit DumpFileIterator(DumpFile* dumpFile);

    uint64_t Size() override;

    bool Valid() override;

    void SeekToFirst() override;

    void Next() override;

    absl::string_view Key() override;

    absl::string_view Value() override;

    bool ParseFromValue(ValueType* value) override;

    int Status() override;

    uint8_t Version() const {
        return version_;
    }

 private:
    void End();

 private:
    off_t offset_;

    uint32_t checkSum_;

    uint64_t nPairs_;

    uint64_t size_;

    bool isValid_;

    Iter iter_;

    uint64_t startTime_;

    DumpFile* dumpfile_;

    uint8_t version_;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_DUMPFILE_H_
