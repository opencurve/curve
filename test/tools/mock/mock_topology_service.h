/*
 * Project: curve
 * File Created: 2020-06-01
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#ifndef TEST_TOOLS_MOCK_MOCK_TOPOLOGY_SERVICE_H_
#define TEST_TOOLS_MOCK_MOCK_TOPOLOGY_SERVICE_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "proto/topology.pb.h"


using google::protobuf::RpcController;
using google::protobuf::Closure;

namespace curve {
namespace mds {
namespace topology {
class MockTopologyService : public TopologyService {
 public:
    MOCK_METHOD4(RegistChunkServer,
        void(RpcController *controller,
        const ChunkServerRegistRequest *request,
        ChunkServerRegistResponse *response,
        Closure *done));
    MOCK_METHOD4(ListChunkServer,
        void(RpcController *controller,
        const ListChunkServerRequest *request,
        ListChunkServerResponse *response,
        Closure *done));
    MOCK_METHOD4(GetChunkServer,
        void(RpcController *controller,
        const GetChunkServerInfoRequest *request,
        GetChunkServerInfoResponse *response,
        Closure *done));
    MOCK_METHOD4(DeleteChunkServer,
        void(RpcController *controller,
        const DeleteChunkServerRequest *request,
        DeleteChunkServerResponse *response,
        Closure *done));
    MOCK_METHOD4(SetChunkServer,
        void(RpcController *controller,
        const SetChunkServerStatusRequest *request,
        SetChunkServerStatusResponse *response,
        Closure *done));
    MOCK_METHOD4(RegistServer,
        void(RpcController *controller,
        const ServerRegistRequest *request,
        ServerRegistResponse *response,
        Closure *done));
    MOCK_METHOD4(GetServer,
        void(RpcController *controller,
        const GetServerRequest *request,
        GetServerResponse *response,
        Closure *done));
    MOCK_METHOD4(DeleteServer,
        void(RpcController *controller,
        const DeleteServerRequest *request,
        DeleteServerResponse *response,
        Closure *done));
    MOCK_METHOD4(ListZoneServer,
        void(RpcController *controller,
        const ListZoneServerRequest *request,
        ListZoneServerResponse *response,
        Closure *done));
    MOCK_METHOD4(CreateZone,
        void(RpcController *controller,
        const ZoneRequest *request,
        ZoneResponse *response,
        Closure *done));
    MOCK_METHOD4(DeleteZone,
        void(RpcController *controller,
        const ZoneRequest *request,
        ZoneResponse *response,
        Closure *done));
    MOCK_METHOD4(GetZone,
        void(RpcController *controller,
        const ZoneRequest *request,
        ZoneResponse *response,
        Closure *done));
    MOCK_METHOD4(ListPoolZone,
        void(RpcController *controller,
        const ListPoolZoneRequest *request,
        ListPoolZoneResponse *response,
        Closure *done));
    MOCK_METHOD4(CreatePhysicalPool,
        void(RpcController *controller,
        const PhysicalPoolRequest *request,
        PhysicalPoolResponse *response,
        Closure *done));
    MOCK_METHOD4(DeletePhysicalPool,
        void(RpcController *controller,
        const PhysicalPoolRequest *request,
        PhysicalPoolResponse *response,
        Closure *done));
    MOCK_METHOD4(GetPhysicalPool,
        void(RpcController *controller,
        const PhysicalPoolRequest *request,
        PhysicalPoolResponse *response,
        Closure *done));
    MOCK_METHOD4(ListPhysicalPool,
        void(RpcController *controller,
        const ListPhysicalPoolRequest *request,
        ListPhysicalPoolResponse *response,
        Closure *done));
    MOCK_METHOD4(CreateLogicalPool,
        void(RpcController *controller,
        const CreateLogicalPoolRequest *request,
        CreateLogicalPoolResponse *response,
        Closure *done));
    MOCK_METHOD4(DeleteLogicalPool,
        void(RpcController *controller,
        const DeleteLogicalPoolRequest *request,
        DeleteLogicalPoolResponse *response,
        Closure *done));
    MOCK_METHOD4(GetLogicalPool,
        void(RpcController *controller,
        const GetLogicalPoolRequest *request,
        GetLogicalPoolResponse *response,
        Closure *done));
    MOCK_METHOD4(ListLogicalPool,
        void(RpcController *controller,
        const ListLogicalPoolRequest *request,
        ListLogicalPoolResponse *response,
        Closure *done));

    MOCK_METHOD4(SetLogicalPoolScanState,
                 void(RpcController* controller,
                      const SetLogicalPoolScanStateRequest* request,
                      SetLogicalPoolScanStateResponse* response,
                      Closure* done));

    MOCK_METHOD4(GetChunkServerListInCopySets,
        void(RpcController *controller,
        const GetChunkServerListInCopySetsRequest *request,
        GetChunkServerListInCopySetsResponse *response,
        Closure *done));
    MOCK_METHOD4(GetCopySetsInChunkServer,
        void(RpcController *controller,
        const GetCopySetsInChunkServerRequest *request,
        GetCopySetsInChunkServerResponse *response,
        Closure *done));
    MOCK_METHOD4(GetCopySetsInCluster,
        void(RpcController *controller,
        const GetCopySetsInClusterRequest *request,
        GetCopySetsInClusterResponse *response,
        Closure *done));

    MOCK_METHOD4(GetCopyset, void(RpcController* controller,
                                  const GetCopysetRequest* request,
                                  GetCopysetResponse* response,
                                  Closure* done));

    MOCK_METHOD4(GetClusterInfo,
        void(RpcController *controller,
        const GetClusterInfoRequest *request,
        GetClusterInfoResponse *response,
        Closure *done));
    MOCK_METHOD4(SetCopysetsAvailFlag,
        void(RpcController *controller,
        const SetCopysetsAvailFlagRequest *request,
        SetCopysetsAvailFlagResponse *response,
        Closure *done));
    MOCK_METHOD4(ListUnAvailCopySets,
        void(RpcController *controller,
        const ListUnAvailCopySetsRequest *request,
        ListUnAvailCopySetsResponse *response,
        Closure *done));
};
}  // namespace topology
}  // namespace mds
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_MOCK_TOPOLOGY_SERVICE_H_
