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
> File Name: snapshot_data_store.h
> Author:
> Created Time: Fri Dec 14 18:28:10 2018
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_DATA_STORE_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_DATA_STORE_H_

#include <glog/logging.h>

#include <functional>
#include <map>
#include <vector>
#include <list>
#include <string>
#include <memory>

#include "src/common/concurrent/concurrent.h"

using ::curve::common::SpinLock;
using ::curve::common::LockGuard;

namespace curve {
namespace snapshotcloneserver {

using ChunkIndexType = uint32_t;
using SnapshotSeqType = uint64_t;

const char kChunkDataNameSeprator[] = "-";

class ChunkDataName {
 public:
    ChunkDataName()
        : chunkSeqNum_(0),
          chunkIndex_(0) {}
    ChunkDataName(const std::string &fileName,
                  SnapshotSeqType seq,
                  ChunkIndexType chunkIndex)
        : fileName_(fileName),
          chunkSeqNum_(seq),
          chunkIndex_(chunkIndex) {}
    /**
     *Build the name of the datachunk object File name Chunk index Version number
     * @return: Object name string
     */
    std::string ToDataChunkKey() const {
        return fileName_
            + kChunkDataNameSeprator
            + std::to_string(this->chunkIndex_)
            + kChunkDataNameSeprator
            + std::to_string(this->chunkSeqNum_);
    }

    std::string fileName_;
    SnapshotSeqType chunkSeqNum_;
    ChunkIndexType chunkIndex_;
};

inline bool operator==(const ChunkDataName &lhs, const ChunkDataName &rhs) {
    return (lhs.fileName_ == rhs.fileName_) &&
           (lhs.chunkSeqNum_ == rhs.chunkSeqNum_) &&
           (lhs.chunkIndex_ == rhs.chunkIndex_);
}

/**
 * @brief Generate chunkdataname object based on object name parsing
 *
 * @param name Object name
 * @param[out] cName chunkDataName object
 *
 * @retVal true succeeded
 * @retVal false failed
 */
bool ToChunkDataName(const std::string &name, ChunkDataName *cName);

class ChunkIndexDataName {
 public:
    ChunkIndexDataName()
        : fileSeqNum_(0) {}
    ChunkIndexDataName(std::string filename,
                       SnapshotSeqType seq) {
        fileName_ = filename;
        fileSeqNum_ = seq;
    }
    /**
     *Build the name of the index chunk file name+file version number
     * @return: The name string of the index chunk
     */
    std::string ToIndexDataChunkKey() const {
        return this->fileName_
            + "-"
            + std::to_string(this->fileSeqNum_);
    }

    //File name
    std::string fileName_;
    //File version number
    SnapshotSeqType fileSeqNum_;
};

class ChunkIndexData {
 public:
    ChunkIndexData() {}
    /**
     *Index chunk data serialization (implemented using protobuf)
     * @param saves a pointer to serialized data
     * @return: true Serialization succeeded/false Serialization failed
     */
    bool Serialize(std::string *data) const;

    /**
     *Deserialize the data of the index chunk into the map
     * @param The data stored in the index chunk
     * @return: true Deserialization succeeded/false Deserialization failed
     */
    bool Unserialize(const std::string &data);

    void PutChunkDataName(const ChunkDataName &name) {
        chunkMap_.emplace(name.chunkIndex_, name.chunkSeqNum_);
    }

    bool GetChunkDataName(ChunkIndexType index, ChunkDataName* nameOut) const;

    bool IsExistChunkDataName(const ChunkDataName &name) const;

    std::vector<ChunkIndexType> GetAllChunkIndex() const;

    void SetFileName(const std::string &fileName) {
        fileName_ = fileName;
    }

    std::string GetFileName() {
        return fileName_;
    }

 private:
    //File name
    std::string fileName_;
    //Snapshot file index information map
    std::map<ChunkIndexType, SnapshotSeqType> chunkMap_;
};


class ChunkData{
 public:
    ChunkData() {}
    std::string data_;
};

class TransferTask {
 public:
     TransferTask() {}
     std::string uploadId_;

     void AddPartInfo(int partNum, std::string etag) {
         m_.Lock();
         partInfo_.emplace(partNum, etag);
         m_.UnLock();
     }

     std::map<int, std::string> GetPartInfo() {
         return partInfo_;
     }

 private:
     mutable SpinLock m_;
     // partnumber <=> etag
     std::map<int, std::string> partInfo_;
};

class SnapshotDataStore {
 public:
     SnapshotDataStore() {}
    virtual ~SnapshotDataStore() {}
    /**
     *The datastore initialization of snapshots can be implemented differently depending on the type of storage
     * @param s3 configuration file path
     * @return 0 initialization successful/-1 initialization failed
     */
    virtual int Init(const std::string &confpath) = 0;
    /**
     *Store the metadata information of the snapshot file in the datastore
     * @param metadata object name
     *The data content of the @ param metadata object
     * @return 0 saved successfully/-1 failed to save
     */
    virtual int PutChunkIndexData(const ChunkIndexDataName &name,
                              const ChunkIndexData &meta) = 0;
    /**
     *Obtain metadata information for snapshot files
     * @param metadata object name
     * @param Pointer to save metadata data content
     * @return:0 successfully obtained/-1 failed to obtain
     */
    virtual int GetChunkIndexData(const ChunkIndexDataName &name,
                                  ChunkIndexData *meta) = 0;
    /**
     *Delete metadata for snapshot files
     * @param metadata object name
     * @return: 0 successfully deleted/-1 failed to delete
     */
    virtual int DeleteChunkIndexData(const ChunkIndexDataName &name) = 0;
    //Does the snapshot metadata chunk exist
    /**
     *Determine whether snapshot metadata exists
     * @param metadata object name
     * @return: true exists/false does not exist
     */
    virtual bool ChunkIndexDataExist(const ChunkIndexDataName &name) = 0;
/*
    //Store the data information of the snapshot file in the datastore
    virtual int PutChunkData(const ChunkDataName &name,
                             const ChunkData &data) = 0;

    //Reading data information from snapshot files
    virtual int GetChunkData(const ChunkDataName &name,
                             ChunkData *data) = 0;
*/
    /**
     *Delete the data chunk of the snapshot
     * @param data chunk name
     * @return: 0 successfully deleted/-1 failed to delete
     */
    virtual int DeleteChunkData(const ChunkDataName &name) = 0;
    /**
     *Determine whether the data chunk of the snapshot exists
     * @param data chunk name
     * @return: true exists/false does not exist
     */
    virtual bool ChunkDataExist(const ChunkDataName &name) = 0;
    //Set snapshot dump completion flag
/*
    virtual int SetSnapshotFlag(const ChunkIndexDataName &name, int flag) = 0;
    //Get snapshot dump completion flag
    virtual int GetSnapshotFlag(const ChunkIndexDataName &name) = 0;
*/
    /**
     *Initialize the sharded dump task of the database chunk
     * @param data chunk name
     * @param Pointer to management dump task
     * @return 0 Task initialization successful/-1 Task initialization failed
     */
    virtual int DataChunkTranferInit(const ChunkDataName &name,
                                    std::shared_ptr<TransferTask> task) = 0;
    /**
     *Add a shard of the data chunk to the dump task
     * @param data chunk name
     *@ Dump Task
     *@ Which shard is it
     *@ Slice size
     *@ Fragmented data content
     * @return: 0 successfully added/-1 failed to add
     */
    virtual int DataChunkTranferAddPart(const ChunkDataName &name,
                                        std::shared_ptr<TransferTask> task,
                                       int partNum,
                                       int partSize,
                                       const char* buf) = 0;
    /**
     *Complete the dump task of data chunks
     * @param data chunk name
     * @param Dump Task Management Structure
     * @return: 0 Dump task completed/Dump task failed -1
     */
    virtual int DataChunkTranferComplete(const ChunkDataName &name,
                                        std::shared_ptr<TransferTask> task) = 0;
    /**
     *Terminate the sharded dump task of data chunks
     * @param data chunk name
     * @param Dump Task Management Structure
     * @return: 0 mission terminated successfully/-1 mission terminated failed
     */
    virtual int DataChunkTranferAbort(const ChunkDataName &name,
                                      std::shared_ptr<TransferTask> task) = 0;
};

}   // namespace snapshotcloneserver
}   // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_DATA_STORE_H_
