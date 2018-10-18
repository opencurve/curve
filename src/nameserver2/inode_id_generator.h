/*
 * Project: curve
 * Created Date: Monday September 10th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_NAMESERVER2_INODE_ID_GENERATOR_H_
#define SRC_NAMESERVER2_INODE_ID_GENERATOR_H_

#include "src/nameserver2/define.h"

namespace curve {
namespace mds {

class InodeIDGenerator {
 public:
    virtual ~InodeIDGenerator() {}
    virtual bool GenInodeID(InodeID * id) = 0;
};
}  // namespace mds
}  // namespace curve
#endif   // SRC_NAMESERVER2_INODE_ID_GENERATOR_H_
