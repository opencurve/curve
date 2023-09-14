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
 * Created Date: Wed Dec 04 2019
 * Author: xuchaojie
 */

#ifndef TEST_INTEGRATION_SNAPSHOTCLONESERVER_FAKE_CURVEFS_CLIENT_H_
#define TEST_INTEGRATION_SNAPSHOTCLONESERVER_FAKE_CURVEFS_CLIENT_H_

#include <string>
#include <map>

#include "src/snapshotcloneserver/common/curvefs_client.h"


using ::curve::client::UserInfo_t;


namespace curve {
namespace snapshotcloneserver {

extern const uint64_t chunkSize;
extern const uint64_t segmentSize;
extern const uint64_t fileLength;
extern const char* testUser1;
extern const char* testFile1;

class FakeCurveFsClient : public CurveFsClient {
 public:
    FakeCurveFsClient() :
        fileId_(101) {}
    virtual ~FakeCurveFsClient() {}

    int Init(const CurveClientOptions &options) override;

    int UnInit() override;

    int CreateSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t *seq) override;

    int DeleteSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t seq) override;

    int GetSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t seq,
        FInfo* snapInfo) override;

    int GetSnapshotSegmentInfo(const std::string &filename,
        const std::string &user,
        uint64_t seq,
        uint64_t offset,
        SegmentInfo *segInfo) override;

    int ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        char *buf,
                        SnapCloneClosure *scc) override;

    int CheckSnapShotStatus(std::string filename,
                            std::string user,
                            uint64_t seq,
                            FileStatus* filestatus) override;

    int GetChunkInfo(const ChunkIDInfo &cidinfo,
        ChunkInfoDetail *chunkInfo) override;

    int CreateCloneFile(
        const std::string &source,
        const std::string &filename,
        const std::string &user,
        uint64_t size,
        uint64_t sn,
        uint32_t chunkSize,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string& poolset,
        FInfo* fileInfo) override;

    int CreateCloneChunk(
        const std::string &location,
        const ChunkIDInfo &chunkidinfo,
        uint64_t sn,
        uint64_t csn,
        uint64_t chunkSize,
        SnapCloneClosure *scc) override;

    int RecoverChunk(
        const ChunkIDInfo &chunkidinfo,
        uint64_t offset,
        uint64_t len,
        SnapCloneClosure *scc) override;

    int CompleteCloneMeta(
        const std::string &filename,
        const std::string &user) override;

    int CompleteCloneFile(
        const std::string &filename,
        const std::string &user) override;

    int SetCloneFileStatus(
        const std::string &filename,
        const FileStatus& filestatus,
        const std::string &user) override;

    int GetFileInfo(
        const std::string &filename,
        const std::string &user,
        FInfo* fileInfo) override;

    int GetOrAllocateSegmentInfo(
        bool allocate,
        uint64_t offset,
        FInfo* fileInfo,
        const std::string &user,
        SegmentInfo *segInfo) override;

    int RenameCloneFile(
        const std::string &user,
        uint64_t originId,
        uint64_t destinationId,
        const std::string &origin,
        const std::string &destination) override;

    int DeleteFile(
        const std::string &fileName,
        const std::string &user,
        uint64_t fileId) override;

    int Mkdir(const std::string& dirpath,
        const std::string &user) override;

    int ChangeOwner(const std::string& filename,
                    const std::string& newOwner) override;

    /**
     * @brief Check if there are temporary files under the /clone directory.
     *
     * @retval true If they exist.
     * @retval false If they do not exist.
     */
    bool JudgeCloneDirHasFile();

 private:
    // fileName -> fileInfo
    std::map<std::string, FInfo> fileMap_;

    // fileName -> snapshot fileInfo
    std::map<std::string, FInfo> fileSnapInfoMap_;

    // Inode IDs start from 101, with numbers under 100 reserved.
    // Snapshot file IDs are always 100, with a parentid = 99.
    // The ID for the "/" directory is 1.
    // The ID for the "/clone" directory is 2.
    // The ID for the "/user1" directory is 3.
    std::atomic<uint64_t> fileId_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // TEST_INTEGRATION_SNAPSHOTCLONESERVER_FAKE_CURVEFS_CLIENT_H_
