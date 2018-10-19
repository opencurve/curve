/*
 * Project: curve
 * Created Date: Monday October 15th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef TEST_MDS_NAMESERVER2_MOCK_TOPOLOGY_ADMIN_H_
#define TEST_MDS_NAMESERVER2_MOCK_TOPOLOGY_ADMIN_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <vector>
#include "src/mds/topology/topology_admin.h"

using ::curve::mds::topology::TopologyAdmin;

namespace curve {
namespace mds {

class  MOCKTopologyAdmin1: public TopologyAdmin {
 public:
     using CopysetIdInfo = ::curve::mds::topology::CopysetIdInfo;

    ~MOCKTopologyAdmin1() {}
    MOCK_METHOD3(AllocateChunkRandomInSingleLogicalPool,
        bool(FileType, uint32_t, std::vector<CopysetIdInfo> *));
};

}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_NAMESERVER2_MOCK_TOPOLOGY_ADMIN_H_
