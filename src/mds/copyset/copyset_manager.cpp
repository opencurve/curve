/*
 * Project:
 * Created Date: Wed Oct 10 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/mds/copyset/copyset_manager.h"

namespace curve {
namespace mds {
namespace copyset {


std::shared_ptr<CopysetPolicy> CopysetManager::GetCopysetPolicy(
    uint32_t zoneNum,
    uint32_t zoneChoseNum,
    uint32_t replicaNum) const {
    if ((3 == zoneNum) &&
        (3 == zoneChoseNum) &&
        (3 == replicaNum)) {
        return std::make_shared<CopysetZoneShufflePolicy>(
               std::make_shared<CopysetPermutationPolicy333>());
    } else {
        return nullptr;
    }
}


}  // namespace copyset
}  // namespace mds
}  // namespace curve
