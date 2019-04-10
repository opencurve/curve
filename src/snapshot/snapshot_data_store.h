/*************************************************************************
> File Name: snapshot_data_store.h
> Author:
> Created Time: Fri Dec 14 18:28:10 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#ifndef SRC_SNAPSHOT_SNAPSHOT_DATA_STORE_H_
#define SRC_SNAPSHOT_SNAPSHOT_DATA_STORE_H_

#include <glog/logging.h>

#include <map>
#include <vector>
#include <list>
#include <string>
#include <memory>
#include "proto/snapshotserver.pb.h"

namespace curve {
namespace snapshotserver {

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
     * 构建datachunk对象的名称 文件名-chunk索引-版本号
     * @return: 对象名称字符串
     */
    std::string ToDataChunkKey() const {
        std::string name = this->fileName_
                            + kChunkDataNameSeprator
                            + std::to_string(this->chunkIndex_)
                            + kChunkDataNameSeprator
                            + std::to_string(this->chunkSeqNum_);
        return name;
    }
    /**
     * 根据对象名称解析生成chunkdataname对象
     * @param 对象的名称
     * @return: ChunkDataName对象
     */
    ChunkDataName ToChunkDataName(const std::string &name) const {
        ChunkDataName cdName;
        // 逆向解析string，以支持文件名具有分隔字符的情况
        std::string::size_type pos =
            name.find_last_of(kChunkDataNameSeprator);
        std::string::size_type lastPos = std::string::npos;
        if (std::string::npos == pos) {
            LOG(ERROR) << "ToChunkDataName error, namestr = " << name;
            return cdName;
        }
        std::string seqNumStr = name.substr(pos + 1, lastPos);
        cdName.chunkSeqNum_ = std::stoll(seqNumStr);
        lastPos = pos;

        pos =
            name.find_last_of(kChunkDataNameSeprator, lastPos);
        if (std::string::npos == pos) {
            LOG(ERROR) << "ToChunkDataName error, namestr = " << name;
            return cdName;
        }
        std::string chunkIndexStr = name.substr(pos + 1, lastPos);
        cdName.chunkIndex_ = std::stoll(chunkIndexStr);
        lastPos = pos;

        cdName.fileName_ = name.substr(0, lastPos);
        return cdName;
    }

    std::string fileName_;
    SnapshotSeqType chunkSeqNum_;
    ChunkIndexType chunkIndex_;
};


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
     * 构建索引chunk的名称 文件名+文件版本号
     * @return: 索引chunk的名称字符串
     */
    std::string ToIndexDataChunkKey() const {
        // filename+fileseqnum
        std::string name = this->fileName_
            + "-"
            + std::to_string(this->fileSeqNum_);
        return name;
    }
    // 文件名
    std::string fileName_;
    // 文件版本号
    SnapshotSeqType fileSeqNum_;
};

class ChunkIndexData {
 public:
    ChunkIndexData() {}
    /**
     * 索引chunk数据序列化（使用protobuf实现）
     * @param 保存序列化后数据的指针
     * @return: true 序列化成功/ false 序列化失败
     */
    bool Serialize(std::string *data) const {
        ChunkMap map;
        for (const auto &m : this->chunkMap_) {
            map.mutable_indexmap()->
                insert({m.first,
                    ChunkDataName(fileName_, m.second, m.first).
                    ToDataChunkKey()});
        }
        // Todo：可以转化为stream给adpater接口使用SerializeToOstream
        return map.SerializeToString(data);
    }
    /**
     * 反序列化索引chunk的数据到map中
     * @param 索引chunk存储的数据
     * @return: true 反序列化成功/ false 反序列化失败
     */
    bool Unserialize(const std::string &data) {
         ChunkMap map;
        if (map.ParseFromString(data)) {
            for (const auto &m : map.indexmap()) {
                ChunkDataName chunkDataName;
                this->chunkMap_.emplace(m.first,
                    chunkDataName.ToChunkDataName(m.second).chunkSeqNum_);
            }
            return true;
        } else {
            return false;
        }
    }

    void PutChunkDataName(const ChunkDataName &name) {
        chunkMap_.emplace(name.chunkIndex_, name.chunkSeqNum_);
    }

    bool GetChunkDataName(ChunkIndexType index, ChunkDataName* nameOut) const {
        auto it = chunkMap_.find(index);
        if (it != chunkMap_.end()) {
            *nameOut = ChunkDataName(fileName_, it->second, index);
            return true;
        } else {
            return false;
        }
    }

    bool IsExistChunkDataName(const ChunkDataName &name) const {
        if (fileName_ != name.fileName_) {
            return false;
        }
        auto it = chunkMap_.find(name.chunkIndex_);
        if (it != chunkMap_.end()) {
            if (it->second == name.chunkSeqNum_) {
                return true;
            }
        }
        return false;
    }

    std::vector<ChunkDataName> GetAllChunkDataName() const {
        std::vector<ChunkDataName> ret;
        for (auto it : chunkMap_) {
            ret.emplace_back(ChunkDataName(fileName_, it.second, it.first));
        }
        return ret;
    }

 private:
    // 文件名
    std::string fileName_;
    // 快照文件索引信息map
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
     // partnumber <=> etag
     std::map<int, std::string> partInfo_;
};

class SnapshotDataStore {
 public:
     SnapshotDataStore() {}
    virtual ~SnapshotDataStore() {}
    /**
     * 快照的datastore初始化，根据存储的类型有不同的实现
     * @return 0 初始化成功/ -1 初始化失败
     */
    virtual int Init() = 0;
    /**
     * 存储快照文件的元数据信息到datastore中
     * @param 元数据对象名
     * @param 元数据对象的数据内容
     * @return 0 保存成功/ -1 保存失败
     */
    virtual int PutChunkIndexData(const ChunkIndexDataName &name,
                              const ChunkIndexData &meta) = 0;
    /**
     * 获取快照文件的元数据信息
     * @param 元数据对象名
     * @param 保存元数据数据内容的指针
     * return: 0 获取成功/ -1 获取失败
     */
    virtual int GetChunkIndexData(const ChunkIndexDataName &name,
                                  ChunkIndexData *meta) = 0;
    /**
     * 删除快照文件的元数据
     * @param 元数据对象名
     * @return: 0 删除成功/ -1 删除失败
     */
    virtual int DeleteChunkIndexData(const ChunkIndexDataName &name) = 0;
    // 快照元数据chunk是否存在
    /**
     * 判断快照元数据是否存在
     * @param 元数据对象名
     * @return: true 存在/ false 不存在
     */
    virtual bool ChunkIndexDataExist(const ChunkIndexDataName &name) = 0;
/*
    // 存储快照文件的数据信息到datastore
    virtual int PutChunkData(const ChunkDataName &name,
                             const ChunkData &data) = 0;

    // 读取快照文件的数据信息
    virtual int GetChunkData(const ChunkDataName &name,
                             ChunkData *data) = 0;
*/
    /**
     * 删除快照的数据chunk
     * @param 数据chunk名
     * @return: 0 删除成功/ -1 删除失败
     */
    virtual int DeleteChunkData(const ChunkDataName &name) = 0;
    /**
     * 判断快照的数据chunk是否存在
     * @param 数据chunk名称
     * @return: true 存在/ false 不存在
     */
    virtual bool ChunkDataExist(const ChunkDataName &name) = 0;
    // 设置快照转储完成标志
/*
    virtual int SetSnapshotFlag(const ChunkIndexDataName &name, int flag) = 0;
    // 获取快照转储完成标志
    virtual int GetSnapshotFlag(const ChunkIndexDataName &name) = 0;
*/
    /**
     * 初始化数据库chunk的分片转储任务
     * @param 数据chunk名称
     * @param 管理转储任务的指针
     * @return 0 任务初始化成功/ -1 任务初始化失败
     */
    virtual int DataChunkTranferInit(const ChunkDataName &name,
                                    std::shared_ptr<TransferTask> task) = 0;
    /**
     * 添加数据chunk的一个分片到转储任务中
     * @param 数据chunk名
     * @转储任务
     * @第几个分片
     * @分片大小
     * @分片的数据内容
     * @return: 0 添加成功/ -1 添加失败
     */
    virtual int DataChunkTranferAddPart(const ChunkDataName &name,
                                        std::shared_ptr<TransferTask> task,
                                       int partNum,
                                       int partSize,
                                       const char* buf) = 0;
    /**
     * 完成数据chunk的转储任务
     * @param 数据chunk名
     * @param 转储任务管理结构
     * @return: 0 转储任务完成/ 转储任务失败 -1
     */
    virtual int DataChunkTranferComplete(const ChunkDataName &name,
                                        std::shared_ptr<TransferTask> task) = 0;
    /**
     * 终止数据chunk的分片转储任务
     * @param 数据chunk名
     * @param 转储任务管理结构
     * @return: 0 任务终止成功/ -1 任务终止失败
     */
    virtual int DataChunkTranferAbort(const ChunkDataName &name,
                                      std::shared_ptr<TransferTask> task) = 0;
};

}   // namespace snapshotserver
}   // namespace curve

#endif  // SRC_SNAPSHOT_SNAPSHOT_DATA_STORE_H_
