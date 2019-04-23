/*
 * Project: curve
 * Created Date: Thur Apr 23th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <string>
#include "src/mds/nameserver2/chunk_id_generator.h"
#include "src/common/string_util.h"
#include "src/mds/nameserver2/namespace_helper.h"

namespace curve {
namespace mds {
bool ChunkIDGeneratorImp::GenChunkID(ChunkID *id) {
    return generator_->GenID(id);
}
}  // namespace mds
}  // namespace curve
