/*
 * Project: curve
 * Created Date: Thur March 28th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <string>
#include "src/mds/nameserver2/idgenerator/inode_id_generator.h"
#include "src/common/string_util.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

namespace curve {
namespace mds {

bool InodeIdGeneratorImp::GenInodeID(InodeID *id) {
    return generator_->GenID(id);
}

}  // namespace mds
}  // namespace curve
