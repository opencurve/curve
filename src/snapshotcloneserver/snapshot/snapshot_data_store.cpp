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
 * Created Date: Fri Apr 19 2019
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"

#include "proto/snapshotcloneserver.pb.h"

namespace curve {
namespace snapshotcloneserver {

bool ToChunkDataName(const std::string &name, ChunkDataName *cName) {
    //Reverse parsing of strings to support cases where file names have separator characters
    std::string::size_type pos =
        name.find_last_of(kChunkDataNameSeprator);
    std::string::size_type lastPos = std::string::npos;
    if (std::string::npos == pos) {
        LOG(ERROR) << "ToChunkDataName error, namestr = " << name;
        return false;
    }
    std::string seqNumStr = name.substr(pos + 1, lastPos);
    cName->chunkSeqNum_ = std::stoll(seqNumStr);
    lastPos = pos - 1;

    pos =
        name.find_last_of(kChunkDataNameSeprator, lastPos);
    if (std::string::npos == pos) {
        LOG(ERROR) << "ToChunkDataName error, namestr = " << name;
        return false;
    }
    std::string chunkIndexStr = name.substr(pos + 1, lastPos);
    cName->chunkIndex_ = std::stoll(chunkIndexStr);
    lastPos = pos;

    cName->fileName_ = name.substr(0, lastPos);
    if (cName->fileName_.empty()) {
        return false;
    }
    return true;
}

bool ChunkIndexData::Serialize(std::string *data) const {
    ChunkMap map;
    for (const auto &m : this->chunkMap_) {
        map.mutable_indexmap()->
            insert({m.first,
                ChunkDataName(fileName_, m.second, m.first).
                ToDataChunkKey()});
    }
    //Todo: Can be converted into a stream for the adpater interface to use SerializeToOstream
    return map.SerializeToString(data);
}

bool ChunkIndexData::Unserialize(const std::string &data) {
     ChunkMap map;
    if (map.ParseFromString(data)) {
        for (const auto &m : map.indexmap()) {
            ChunkDataName chunkDataName;
            if (ToChunkDataName(m.second, &chunkDataName)) {
                this->fileName_ = chunkDataName.fileName_;
                this->chunkMap_.emplace(m.first,
                    chunkDataName.chunkSeqNum_);
            } else {
                return false;
            }
        }
        return true;
    } else {
        return false;
    }
}

bool ChunkIndexData::GetChunkDataName(ChunkIndexType index,
    ChunkDataName* nameOut) const {
    auto it = chunkMap_.find(index);
    if (it != chunkMap_.end()) {
        *nameOut = ChunkDataName(fileName_, it->second, index);
        return true;
    } else {
        return false;
    }
}

bool ChunkIndexData::IsExistChunkDataName(const ChunkDataName &name) const {
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

std::vector<ChunkIndexType> ChunkIndexData::GetAllChunkIndex() const {
    std::vector<ChunkIndexType> ret;
    for (auto it : chunkMap_) {
        ret.emplace_back(it.first);
    }
    return ret;
}

}   // namespace snapshotcloneserver
}   // namespace curve
