package maydamagedvolumes

import (
	"context"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"google.golang.org/grpc"
)

type ListPoolsetsRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPoolsetRequest
	TopologyClient topology.TopologyServiceClient
}

func (lPstRpc *ListPoolsetsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lPstRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (lPstRpc *ListPoolsetsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lPstRpc.TopologyClient.ListPoolset(ctx, lPstRpc.Request)
}

type ListPhyPoolsInPstRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPhyPoolsInPoolsetRequest
	TopologyClient topology.TopologyServiceClient
}

func (LPhyRpc *ListPhyPoolsInPstRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	LPhyRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (lpRpc *ListPhyPoolsInPstRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lpRpc.TopologyClient.ListPhyPoolsInPoolset(ctx, lpRpc.Request)
}

type ListZonesRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPoolZoneRequest
	TopologyClient topology.TopologyServiceClient
}

func (lZRpc *ListZonesRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lZRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (lZRpc *ListZonesRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lZRpc.TopologyClient.ListPoolZone(ctx, lZRpc.Request)
}

type ListServersRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListZoneServerRequest
	TopologyClient topology.TopologyServiceClient
}

func (lSRpc *ListServersRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lSRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (lSRpc *ListServersRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lSRpc.TopologyClient.ListZoneServer(ctx, lSRpc.Request)
}

func (mdvCmd *MayDamVolCmd) listPoolsets() *cmderror.CmdError {
	request := &topology.ListPoolsetRequest{}
	mdvCmd.listPoolsetsRpc = &ListPoolsetsRpc{
		Request: request,
	}
	mdvCmd.listPoolsetsRpc.Info = basecmd.NewRpc(mdvCmd.addrs, mdvCmd.timeout, mdvCmd.retryTimes, "ListPoolset")
	result, err := basecmd.GetRpcResponse(mdvCmd.listPoolsetsRpc.Info, mdvCmd.listPoolsetsRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	response := result.(*topology.ListPoolsetResponse)
	if response.GetStatusCode() != 0 {
		return cmderror.ErrListPoolsets(statuscode.TopoStatusCode(response.GetStatusCode()))
	}
	mdvCmd.clusterPoolsetsInfo = append(mdvCmd.clusterPoolsetsInfo, response.GetPoolsetInfos()...)
	return cmderror.ErrSuccess()
}

func (mdvCmd *MayDamVolCmd) ListPhyPools() *cmderror.CmdError {
	var pstIds []uint32
	for _, pstInfo := range mdvCmd.clusterPoolsetsInfo {
		pstIds = append(pstIds, *pstInfo.PoolsetID)
	}
	request := &topology.ListPhyPoolsInPoolsetRequest{
		PoolsetId: pstIds,
	}
	mdvCmd.listPhyPoolsInPstRpc = &ListPhyPoolsInPstRpc{
		Request: request,
	}
	mdvCmd.listPhyPoolsInPstRpc.Info = basecmd.NewRpc(mdvCmd.addrs, mdvCmd.timeout, mdvCmd.retryTimes, "ListPhyPoolsInPoolset")
	result, err := basecmd.GetRpcResponse(mdvCmd.listPhyPoolsInPstRpc.Info, mdvCmd.listPhyPoolsInPstRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	response := result.(*topology.ListPhysicalPoolResponse)
	if response.GetStatusCode() != 0 {
		return cmderror.ErrListPhyPoolsInPst(statuscode.TopoStatusCode(response.GetStatusCode()))
	}
	mdvCmd.clusterPhyPoolsInfo = append(mdvCmd.clusterPhyPoolsInfo, response.GetPhysicalPoolInfos()...)
	return cmderror.ErrSuccess()
}

func (mdvCmd *MayDamVolCmd) ListZones() *cmderror.CmdError {
	for _, phyPool := range mdvCmd.clusterPhyPoolsInfo {
		poolId := phyPool.GetPhysicalPoolID()
		poolName := phyPool.GetPhysicalPoolName()
		request := &topology.ListPoolZoneRequest{
			PhysicalPoolID:   &poolId,
			PhysicalPoolName: &poolName,
		}
		mdvCmd.listZonesRpc = &ListZonesRpc{
			Request: request,
		}
		mdvCmd.listZonesRpc.Info = basecmd.NewRpc(mdvCmd.addrs, mdvCmd.timeout, mdvCmd.retryTimes, "ListZonesInPhysicalPool")
		result, err := basecmd.GetRpcResponse(mdvCmd.listZonesRpc.Info, mdvCmd.listZonesRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.ListPoolZoneResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrListZonesInPhyPool(statuscode.TopoStatusCode(response.GetStatusCode()))
		}
		mdvCmd.clusterZonesInfo = append(mdvCmd.clusterZonesInfo, response.GetZones()...)
	}
	return cmderror.ErrSuccess()
}

func (mdvCmd *MayDamVolCmd) ListServers() *cmderror.CmdError {
	for _, zone := range mdvCmd.clusterZonesInfo {
		zoneId := zone.GetZoneID()
		zoneName := zone.GetZoneName()
		phyPoolName := zone.GetPhysicalPoolName()
		request := &topology.ListZoneServerRequest{
			ZoneID:           &zoneId,
			ZoneName:         &zoneName,
			PhysicalPoolName: &phyPoolName,
		}
		mdvCmd.listServersRpc = &ListServersRpc{
			Request: request,
		}
		mdvCmd.listServersRpc.Info = basecmd.NewRpc(mdvCmd.addrs, mdvCmd.timeout, mdvCmd.retryTimes, "ListServersInZone")
		result, err := basecmd.GetRpcResponse(mdvCmd.listServersRpc.Info, mdvCmd.listServersRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.ListZoneServerResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrListServers(statuscode.TopoStatusCode(response.GetStatusCode()))
		}
		mdvCmd.clusterServersInfo = append(mdvCmd.clusterServersInfo, response.GetServerInfo()...)
	}
	return cmderror.ErrSuccess()
}

func (mdvCmd *MayDamVolCmd) GetServersInfo() *cmderror.CmdError {
	err := mdvCmd.listPoolsets()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	err = mdvCmd.ListPhyPools()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	err = mdvCmd.ListZones()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	err = mdvCmd.ListServers()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	return err
}
