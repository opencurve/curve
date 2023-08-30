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

/*************************************************************************
	> File Name: curvefs_client.h
	> Author:
	> Created Time: Wed Nov 21 11:33:46 2018
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_CURVEFS_CLIENT_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_CURVEFS_CLIENT_H_


#include<string>
#include <vector>
#include <memory>
#include <chrono>  //NOLINT
#include <thread>  //NOLINT
#include "proto/nameserver2.pb.h"
#include "proto/chunk.pb.h"

#include "src/client/client_common.h"
#include "src/client/libcurve_snapshot.h"
#include "src/client/libcurve_file.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/common/timeutility.h"

using ::curve::client::SegmentInfo;
using ::curve::client::LogicPoolID;
using ::curve::client::CopysetID;
using ::curve::client::ChunkID;
using ::curve::client::ChunkInfoDetail;
using ::curve::client::ChunkIDInfo;
using ::curve::client::FInfo;
using ::curve::client::FileStatus;
using ::curve::client::SnapCloneClosure;
using ::curve::client::UserInfo;
using ::curve::client::SnapshotClient;
using ::curve::client::FileClient;

namespace curve {
namespace snapshotcloneserver {

using RetryMethod = std::function<int()>;
using RetryCondition = std::function<bool(int)>;

class RetryHelper {
 public:
    RetryHelper(const RetryMethod &retryMethod,
        const RetryCondition &condition) {
        retryMethod_ = retryMethod;
        condition_ = condition;
    }

    int RetryTimeSecAndReturn(
        uint64_t retryTimeSec,
        uint64_t retryIntervalMs) {
        int ret = -LIBCURVE_ERROR::FAILED;
        uint64_t startTime = TimeUtility::GetTimeofDaySec();
        uint64_t nowTime = startTime;
        do {
            ret = retryMethod_();
            if (!condition_(ret)) {
                return ret;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(retryIntervalMs));
            nowTime = TimeUtility::GetTimeofDaySec();
        } while (nowTime - startTime < retryTimeSec);
        return ret;
    }

 private:
    RetryMethod  retryMethod_;
    RetryCondition condition_;
};

class CurveFsClient {
 public:
    CurveFsClient() {}
    virtual ~CurveFsClient() {}

    /**
     * @brief client initialization
     *
     * @return error code
     */

    virtual int Init(const CurveClientOptions &options) = 0;

    /**
     * @brief client resource recycling
     *
     * @return error code
     */

    virtual int UnInit() = 0;

    /**
     * @brief Create a snapshot
     *
     * @param filename File name
     * @param user user information
     * @param[out] seq snapshot version number
     *
     * @return error code
     */

    virtual int CreateSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t *seq) = 0;

    /**
     * @brief Delete snapshot
     *
     * @param filename File name
     * @param user user information
     * @param seq snapshot version number
     *
     * @return error code
     */

    virtual int DeleteSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t seq) = 0;

    /**
     * @brief Get snapshot file information
     *
     * @param filename File name
     * @param user username
     * @param seq snapshot version number
     * @param[out] snapInfo snapshot file information
     *
     * @return error code
     */

    virtual int GetSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t seq, FInfo* snapInfo) = 0;

    /**
     * @brief Query snapshot file segment information
     *
     * @param filename File name
     * @param user user information
     * @param seq snapshot version number
     * @param offset offset value
     * @param segInfo segment information
     *
     * @return error code
     */

    virtual int GetSnapshotSegmentInfo(const std::string &filename,
        const std::string &user,
        uint64_t seq,
        uint64_t offset,
        SegmentInfo *segInfo) = 0;

    /**
     * @brief Read snapshot chunk data
     *
     * @param cinfo chunk ID information
     * @param seq snapshot version number
     * @param offset offset value
     * @param len length
     * @param[out] buffer buffer pointer
     * @param: scc is an asynchronous callback
     *
     * @return error code
     */

    virtual int ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        char *buf,
                        SnapCloneClosure* scc) = 0;

    /**
     *Get snapshot status
     * @param: userinfo is the user information
     * @param: filenam file name
     * @param: seq is the file version number information
     * @param: filestatus Snapshot file status
     */

    virtual int CheckSnapShotStatus(std::string filename,
                                std::string user,
                                uint64_t seq,
                                FileStatus* filestatus) = 0;

    /**
     * @brief to obtain the version number information of the chunk
     *
     * @param cinfo chunk ID information
     * @param chunkInfo chunk Details
     *
     * @return error code
     */

    virtual int GetChunkInfo(const ChunkIDInfo &cidinfo,
        ChunkInfoDetail *chunkInfo) = 0;

    /**
     * @brief Create clone file
     *@ detail
     *- If clone, reset sn to initial value
     *- If recover, sn remains unchanged
     *
     * @param source clone Source file name
     * @param filename clone Target filename
     * @param user user information
     * @param size File size
     * @param sn version number
     * @param chunkSize chunk size
     * @param stripeUnit stripe size
     * @param stripeCount stripe count
     * @param[out] fileInfo file information
     *
     * @return error code
     */

    virtual int CreateCloneFile(
        const std::string &source,
        const std::string &filename,
        const std::string &user,
        uint64_t size,
        uint64_t sn,
        uint32_t chunkSize,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string& poolset,
        FInfo* fileInfo) = 0;

    /**
     * @brief lazy Create clone chunk
     *@ detail
     *The format definition of a location is A@B The form of.
     *- If the source data is on s3, the location format is uri@s3 Uri is the address of the actual chunk object;
     *- If the source data is on curves, the location format is/filename/ chunkindex@cs
     *
     *URL of @ param location data source
     * @param chunkidinfo Target chunk
     *The serial number of @ param sn chunk
     * @param csn correct sn
     * @param chunkSize Chunk size
     * @param: scc is an asynchronous callback
     *
     * @return error code
     */

    virtual int CreateCloneChunk(
        const std::string &location,
        const ChunkIDInfo &chunkidinfo,
        uint64_t sn,
        uint64_t csn,
        uint64_t chunkSize,
        SnapCloneClosure* scc) = 0;


    /**
     * @brief Actual recovery chunk data
     *
     * @param chunkidinfo chunkidinfo
     * @param offset offset
     * @param len length
     * @param: scc is an asynchronous callback
     *
     * @return error code
     */

    virtual int RecoverChunk(
        const ChunkIDInfo &chunkidinfo,
        uint64_t offset,
        uint64_t len,
        SnapCloneClosure* scc) = 0;

    /**
     * @brief Notify mds to complete Clone Meta
     *
     * @param filename Target file name
     * @param user username
     *
     * @return error code
     */

    virtual int CompleteCloneMeta(
        const std::string &filename,
        const std::string &user) = 0;

    /**
     * @brief Notify mds to complete Clone Chunk
     *
     * @param filename Target file name
     * @param user username
     *
     * @return error code
     */

    virtual int CompleteCloneFile(
        const std::string &filename,
        const std::string &user) = 0;

    /**
     * @brief Set clone file status
     *
     * @param filename File name
     * @param filestatus The target state to be set
     * @param user username
     *
     * @return error code
     */

    virtual int SetCloneFileStatus(
        const std::string &filename,
        const FileStatus& filestatus,
        const std::string &user) = 0;

    /**
     * @brief Get file information
     *
     * @param filename File name
     * @param user username
     * @param[out] fileInfo file information
     *
     * @return error code
     */

    virtual int GetFileInfo(
        const std::string &filename,
        const std::string &user,
        FInfo* fileInfo) = 0;

    /**
     * @brief Query or allocate file segment information
     *
     * @param allocate whether to allocate
     * @param offset offset value
     * @param fileInfo file information
     * @param user username
     * @param segInfo segment information
     *
     * @return error code
     */

    virtual int GetOrAllocateSegmentInfo(
        bool allocate,
        uint64_t offset,
        FInfo* fileInfo,
        const std::string &user,
        SegmentInfo *segInfo) = 0;

    /**
     * @brief is the file copied for recover rename
     *
     * @param user user information
     * @param originId The original file ID that was restored
     * @param destinationId The cloned target file ID
     * @param origin The original file name of the recovered file
     *The target file cloned from @ param destination
     *
     * @return error code
     */

    virtual int RenameCloneFile(
            const std::string &user,
            uint64_t originId,
            uint64_t destinationId,
            const std::string &origin,
            const std::string &destination) = 0;


    /**
     * @brief Delete file
     *
     * @param fileName File name
     * @param user username
     * @param fileId Delete the inodeId of the file
     *
     * @return error code
     */

    virtual int DeleteFile(
            const std::string &fileName,
            const std::string &user,
            uint64_t fileId) = 0;

    /**
     * @brief Create directory
     *
     * @param dirpath directory name
     * @param user username
     *
     * @return error code
     */

    virtual int Mkdir(const std::string& dirpath,
        const std::string &user) = 0;

    /**
     * @brief Change the owner of the file
     *
     * @param filename File name
     * @param newOwner New owner
     *
     * @return error code
     */

    virtual int ChangeOwner(const std::string& filename,
        const std::string& newOwner) = 0;
};

class CurveFsClientImpl : public CurveFsClient {
 public:
    CurveFsClientImpl(std::shared_ptr<SnapshotClient> snapClient,
        std::shared_ptr<FileClient> fileClient) :
        snapClient_(snapClient), fileClient_(fileClient) {}
    virtual ~CurveFsClientImpl() {}

    //The following interface definitions can be found in the CurveFsClient interface annotations

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
                        SnapCloneClosure* scc) override;

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
        SnapCloneClosure* scc) override;

    int RecoverChunk(
        const ChunkIDInfo &chunkidinfo,
        uint64_t offset,
        uint64_t len,
        SnapCloneClosure* scc) override;

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

 private:
    UserInfo GetUserInfo(const std::string &user) {
        if (user == mdsRootUser_) {
            return UserInfo(mdsRootUser_, mdsRootPassword_);
        } else {
            return UserInfo(user, "");
        }
    }

 private:
    std::shared_ptr<SnapshotClient> snapClient_;
    std::shared_ptr<FileClient> fileClient_;

    std::string mdsRootUser_;
    std::string mdsRootPassword_;

    uint64_t clientMethodRetryTimeSec_;
    uint64_t clientMethodRetryIntervalMs_;
};

}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_CURVEFS_CLIENT_H_
