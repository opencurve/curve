/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/chunkserver/integrity.h"

namespace curve {
namespace chunkserver {

int Integrity::Init(const IntegrityOptions &options) {
    options_ = options;
    return 0;
}

int Integrity::Run() {
    return 0;
}

int Integrity::Fini() {
    return 0;
}

}  // namespace chunkserver
}  // namespace curve

