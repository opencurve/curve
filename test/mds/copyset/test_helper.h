/*
 * Project: curve
 * Created Date: Mon May 13 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef TEST_MDS_COPYSET_TEST_HELPER_H_
#define TEST_MDS_COPYSET_TEST_HELPER_H_

#include <vector>

#include "src/mds/copyset/copyset_policy.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/common/mds_define.h"

namespace curve {
namespace mds {
namespace copyset {

using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::ZoneIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::PoolIdType;

class TestCluster : public ClusterInfo {
 public:
    void SetUniformCluster() {
        set_servers({
            {1, {1, 0}},
            {2, {1, 0}},
            {3, {1, 0}},

            {4, {2, 0}},
            {5, {2, 0}},
            {6, {2, 0}},

            {7, {3, 0}},
            {8, {3, 0}},
            {9, {3, 0}},
        });
    }

    void SetIncompleteCluster() {
        set_servers({
            {1, {1, 0}},
            {2, {1, 0}},
            {3, {1, 0}},

            {4, {2, 0}},
            {5, {2, 0}},
            {6, {2, 0}},
        });
    }

    void SetSlantClustser() {
        set_servers({
            {1, {1, 0}},
            {2, {1, 0}},

            {3, {2, 0}},
            {4, {2, 0}},
            {5, {2, 0}},
            {6, {2, 0}},
            {7, {2, 0}},

            {8, {3, 0}},
            {9, {3, 0}},
        });
    }

    void SetMultiZoneCluster() {
        set_servers({
            {1, {1, 0}},
            {2, {1, 0}},
            {3, {1, 0}},

            {4, {2, 0}},
            {5, {2, 0}},
            {6, {2, 0}},

            {7, {3, 0}},
            {8, {3, 0}},
            {9, {3, 0}},

            {10, {4, 0}},
            {11, {4, 0}},
            {12, {4, 0}},
        });
    }

    void SetMassiveCluster() {
        constexpr int Node = 180;
        constexpr int Zone = 3;

        std::vector<ChunkServerInfo> servers;
        for (int i = 0; i < Node; i++) {
            ChunkServerInfo server{
                static_cast<ChunkServerIdType>(i),
                {static_cast<ZoneIdType>(std::rand() % Zone), 0}};
            servers.emplace_back(std::move(server));
        }
        set_servers(servers);
    }

    int num_servers() const { return csInfo_.size(); }

    void set_servers(const std::vector<ChunkServerInfo>& servers) {
        csInfo_ = servers; }
};


}  // namespace copyset
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_COPYSET_TEST_HELPER_H_
