/*
 * Project: curve
 * File Created: Wednesday, 5th September 2018 7:55:03 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_CHUNKSERVER_CHUNKSERVER_STORAGE_H
#define CURVE_CHUNKSERVER_CHUNKSERVER_STORAGE_H

#include <string>
#include <memory>
#include <vector>

#include "include/curve_compiler_specific.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor.h"

namespace curve {
namespace chunkserver {
class CURVE_CACHELINE_ALIGNMENT ChunkserverStorage {
 public:
    static void Init() {
        csSfsAdaptorPtrVect_.clear();
    }

    static std::shared_ptr<CSSfsAdaptor> CreateFsAdaptor(std::string deviceID,
                                                    std::string storageuri) {
        std::shared_ptr<CSSfsAdaptor> csSfsAdaptorPtr_ =
         std::shared_ptr<CSSfsAdaptor>(new CSSfsAdaptor());
        if (csSfsAdaptorPtr_ != nullptr) {
            if (!csSfsAdaptorPtr_->Initialize(deviceID, storageuri)) {
                csSfsAdaptorPtr_.reset();
            } else {
                csSfsAdaptorPtrVect_.push_back(csSfsAdaptorPtr_);
            }
        }
        return csSfsAdaptorPtr_;
    }

    static void UnInit() {
        for (auto iter : csSfsAdaptorPtrVect_) {
            if (iter) {
                iter->UnInitialize();
                iter.reset();
            }
        }
        csSfsAdaptorPtrVect_.clear();
    }

    ~ChunkserverStorage() {}

 private:
    ChunkserverStorage() {}
    static std::vector<std::shared_ptr<CSSfsAdaptor>>  csSfsAdaptorPtrVect_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // !CURVE_CHUNKSERVER_CHUNKSERVER_STORAGE_H
