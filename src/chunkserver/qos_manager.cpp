/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/chunkserver/qos_manager.h"

namespace curve {
namespace chunkserver {

int QosManager::Init(const QosOptions &options) {
    options_ = options;
    copysetNodeManager_ = options.copysetNodeManager;

    return 0;
}

int QosManager::Run() {
    return 0;
}

int QosManager::Fini() {
    return 0;
}

}  // namespace chunkserver
}  // namespace curve

