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
 * Created Date: 2021-8-13
 * Author: chengyi
 */

#ifndef CURVEFS_SRC_METASERVER_S3_METASERVER_S3_ADAPTOR_H_
#define CURVEFS_SRC_METASERVER_S3_METASERVER_S3_ADAPTOR_H_

#include <string>
#include <list>
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/s3/metaserver_s3.h"

namespace curvefs {
namespace metaserver {

struct S3ClientAdaptorOption {
    uint64_t blockSize;
    uint64_t chunkSize;
    uint64_t batchSize;
    uint32_t objectPrefix;
    bool enableDeleteObjects;
};

class S3ClientAdaptor {
 public:
    S3ClientAdaptor() {}
    virtual ~S3ClientAdaptor() {}

    /**
     * @brief Initialize s3 client
     * @param[in] options the options for s3 client
     */
    virtual void Init(const S3ClientAdaptorOption& option,
                      S3Client* client) = 0;
        /**
     * @brief Reinitialize s3 client
     */
    virtual void Reinit(const S3ClientAdaptorOption& option,
        const std::string& ak, const std::string& sk,
        const std::string& endpoint, const std::string& bucketName) = 0;

    /**
     * @brief delete inode from s3
     * @param inode
     * @return int
     *  0   : delete sucess
     *  -1  : delete fail
     * @details
     * Step.1 get indoe' ChunkInfoList
     * Step.2 delete chunk from s3 client
     */
    virtual int Delete(const Inode& inode) = 0;

    /**
     * @brief get S3ClientAdaptorOption
     * 
     * @param option return value
     * @details
     */
    virtual void GetS3ClientAdaptorOption(S3ClientAdaptorOption *option) = 0;
};

class S3ClientAdaptorImpl : public S3ClientAdaptor {
 public:
    S3ClientAdaptorImpl() {}
    ~S3ClientAdaptorImpl() {
        if (client_ != nullptr) {
            delete client_;
            client_ = nullptr;
        }
    }

    /**
     * @brief Initialize s3 client
     * @param[in] options the options for s3 client
     */
    void Init(const S3ClientAdaptorOption& option, S3Client* client) override;

    /**
     * @brief Reinitialize s3 client
     */
    void Reinit(const S3ClientAdaptorOption& option,
        const std::string& ak, const std::string& sk,
        const std::string& endpoint, const std::string& bucketName) override;

    /**
     * @brief delete inode from s3
     * @param inode
     * @return int
     * @details
     * Step.1 get indoe' ChunkInfoList
     * Step.2 delete chunk from s3 client
     */
    int Delete(const Inode& inode) override;

    /**
     * @brief get S3ClientAdaptorOption
     * 
     * @param option return value
     * @details
     */
    void GetS3ClientAdaptorOption(S3ClientAdaptorOption *option) override;

 private:
    /**
     * @brief  delete chunk from client
     * @return int
     *  0   : delete sucess or some objects are not exist
     *  -1  : some objects delete fail
     * @param[in] options the options for s3 client
     */
    int DeleteChunk(uint64_t fsId, uint64_t inodeId, uint64_t chunkId,
                    uint64_t compaction, uint64_t chunkPos, uint64_t length);

    int DeleteInodeByDeleteSingleChunk(const Inode& inode);

    int DeleteInodeByDeleteBatchChunk(const Inode& inode);

    int DeleteChunkInfoList(uint32_t fsId, uint64_t inodeId,
                              const ChunkInfoList& ChunkInfolist);

    void GenObjNameListForChunkInfoList(uint32_t fsId, uint64_t inodeId,
                                        const ChunkInfoList& ChunkInfolist,
                                        std::list<std::string>* objList);

    void GenObjNameListForChunkInfo(uint32_t fsId, uint64_t inodeId,
                                    const ChunkInfo& chunkInfo,
                                    std::list<std::string>* objList);

    S3Client* client_;
    uint64_t blockSize_;
    uint64_t chunkSize_;
    uint64_t batchSize_;
    uint32_t objectPrefix_;
    bool enableDeleteObjects_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_S3_METASERVER_S3_ADAPTOR_H_
