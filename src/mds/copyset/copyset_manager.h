/*
 * Project:
 * Created Date: Wed Oct 10 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_COPYSET_COPYSET_MANAGER_H_
#define CURVE_SRC_MDS_COPYSET_COPYSET_MANAGER_H_


#include <memory>

#include "src/mds/copyset/copyset_policy.h"

namespace curve {
namespace mds {
namespace copyset {



class CopysetManager {
public:
    CopysetManager() {}
    ~CopysetManager() {}

std::shared_ptr<CopysetPolicy> GetCopysetPolicy(uint32_t zoneNum,
    uint32_t zoneChoseNum,
    uint32_t replicaNum) const;

};


}  // namespace copyset
}  // namespace mds
}  // namespace curve

#endif  // CURVE_SRC_MDS_COPYSET_COPYSET_MANAGER_H_
