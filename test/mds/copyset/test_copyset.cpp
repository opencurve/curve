/*
 * Project: curve
 * Created Date: Wed Oct 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <unordered_set>

#include "src/mds/copyset/copyset_policy.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/common/topology_define.h"

namespace curve {
namespace mds {
namespace copyset {

using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::ZoneIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::PoolIdType;

class CopysetConstraints {
 public:
    CopysetConstraints(int zone, int replica, PoolIdType pool = kNullPool)
        : num_zones_(zone), numReplicas_(replica), pool_(pool) {}
    ~CopysetConstraints() {}

    // Valid whether a copsyet meets all constraints
    bool Valid(const ClusterInfo& cluster, const Copyset& copyset) const;

    bool Valid(const std::vector<ChunkServerInfo>& copyset) const;

    int num_replicas() const {
        return numReplicas_;
    }
    int num_zone() const {
        return num_zones_;
    }
    int pool() const {
        return pool_;
    }
    void set_pool(PoolIdType pool) {
        pool_ = pool;
    }

    // which has no pool constraint
    static const PoolIdType kNullPool = 0;

 private:
    int num_zones_;
    int numReplicas_;
    PoolIdType pool_;
};

bool CopysetConstraints::Valid(const ClusterInfo& cluster,
    const Copyset& copyset) const {
    if (copyset.replicas.size() < numReplicas_) {
        return false;
    }
    std::unordered_set<int> zones;
    ChunkServerInfo server;
    for (auto replica : copyset.replicas) {
        if (cluster.GetChunkServerInfo(replica, &server)) {
            if (server.location.logicalPoolId != pool()) {
                return false;
            }
            zones.insert(server.location.zoneId);
        } else {
            return false;
        }
    }
    return zones.size() >= num_zone();
}

bool CopysetConstraints::Valid(
    const std::vector<ChunkServerInfo>& copyset) const {
    if (copyset.size() < num_replicas()) {
        return false;
    }
    std::unordered_set<int> zones;
    for (auto& server : copyset) {
        if (server.location.logicalPoolId != pool()) {
            return false;
        }
        zones.insert(server.location.zoneId);
    }
    return zones.size() >= num_zone();
}


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

class TestCopyset : public  testing::Test {
 public:
    TestCopyset() {}
    ~TestCopyset() {}

 protected:
    virtual void SetUp() {}

    virtual void TearDown() {}
};


TEST_F(TestCopyset,
    test_CopysetZoneShufflePolicy333_GenCopyset_uniformClusterSuccess) {

    std::shared_ptr<CopysetPermutationPolicy> permutation =
        std::make_shared<CopysetPermutationPolicy333>();
    std::shared_ptr<CopysetPolicy> policy =
        std::make_shared<CopysetZoneShufflePolicy>(permutation);


    TestCluster cluster;
    cluster.SetUniformCluster();

    int numCopysets = 2;

    std::vector<Copyset> out;
    bool ret = policy->GenCopyset(cluster, numCopysets, &out);

    ASSERT_TRUE(ret);

    CopysetConstraints constraint(3, 3);

    for (auto& copyset : out) {
        ASSERT_TRUE(constraint.Valid(cluster, copyset)) << copyset;
    }
}

TEST_F(TestCopyset,
    test_CopysetZoneShufflePolicy333_GenCopyset_MassiveClusterSuccess) {

    std::shared_ptr<CopysetPermutationPolicy> permutation =
        std::make_shared<CopysetPermutationPolicy333>();
    std::shared_ptr<CopysetPolicy> policy =
        std::make_shared<CopysetZoneShufflePolicy>(permutation);


    TestCluster cluster;
    cluster.SetMassiveCluster();

    int numCopysets = 6000;

    std::vector<Copyset> out;
    bool ret = policy->GenCopyset(cluster, numCopysets, &out);

    ASSERT_TRUE(ret);

    CopysetConstraints constraint(3, 3);

    for (auto& copyset : out) {
        ASSERT_TRUE(constraint.Valid(cluster, copyset)) << copyset;
    }
}

TEST_F(TestCopyset,
    test_CopysetZoneShufflePolicy333_GenCopyset_IncompleteClusterFail) {

    std::shared_ptr<CopysetPermutationPolicy> permutation =
        std::make_shared<CopysetPermutationPolicy333>();
    std::shared_ptr<CopysetPolicy> policy =
        std::make_shared<CopysetZoneShufflePolicy>(permutation);


    TestCluster cluster;
    cluster.SetIncompleteCluster();

    int numCopysets = 2;

    std::vector<Copyset> out;
    bool ret = policy->GenCopyset(cluster, numCopysets, &out);

    ASSERT_FALSE(ret);
}

TEST_F(TestCopyset,
    test_CopysetZoneShufflePolicy333_GenCopyset_SlantClustserSuccess) {

    std::shared_ptr<CopysetPermutationPolicy> permutation =
        std::make_shared<CopysetPermutationPolicy333>();
    std::shared_ptr<CopysetPolicy> policy =
        std::make_shared<CopysetZoneShufflePolicy>(permutation);


    TestCluster cluster;
    cluster.SetSlantClustser();

    int numCopysets = 2;

    std::vector<Copyset> out;
    bool ret = policy->GenCopyset(cluster, numCopysets, &out);

    ASSERT_TRUE(ret);

    CopysetConstraints constraint(3, 3);

    for (auto& copyset : out) {
        ASSERT_TRUE(constraint.Valid(cluster, copyset)) << copyset;
    }
}

TEST_F(TestCopyset,
    test_CopysetZoneShufflePolicy333_GenCopyset_MultiZoneClusterSuccess) {

    std::shared_ptr<CopysetPermutationPolicy> permutation =
        std::make_shared<CopysetPermutationPolicy333>();
    std::shared_ptr<CopysetPolicy> policy =
        std::make_shared<CopysetZoneShufflePolicy>(permutation);

    TestCluster cluster;
    cluster.SetMultiZoneCluster();

    int numCopysets = 2;

    std::vector<Copyset> out;
    bool ret = policy->GenCopyset(cluster, numCopysets, &out);

    ASSERT_FALSE(ret);
}

}  // namespace copyset
}  // namespace mds
}  // namespace curve



int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}

