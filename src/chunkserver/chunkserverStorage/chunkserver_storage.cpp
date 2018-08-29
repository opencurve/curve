/*
 * Project: curve
 * File Created: Thursday, 27th September 2018 4:38:31 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include "src/chunkserver/chunkserverStorage/chunkserver_storage.h"

namespace curve {
namespace chunkserver {

std::vector<std::shared_ptr<CSSfsAdaptor>> ChunkserverStorage::csSfsAdaptorPtrVect_;

}  // namespace chunkserver
}  // namespace curve
