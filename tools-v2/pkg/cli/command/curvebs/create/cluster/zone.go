package cluster

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

type Zone struct {
	Name         string
	PhysicalPool string
	Desc         string
}

type ListZonesRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPoolZoneRequest
	TopologyClient topology.TopologyServiceClient
}

type DelZoneRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ZoneRequest
	TopologyClient topology.TopologyServiceClient
}

type CreateZoneRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ZoneRequest
	TopologyClient topology.TopologyServiceClient
}

// servers with the same zone field value are grouped together to form a zone
func (ctCmd *ClusterTopoCmd) genZones() *cmderror.CmdError {
	// generate zones
	for _, server := range ctCmd.topology.Servers {
		err := ctCmd.CheckPhysicalPool(server.PhysicalPool)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		zone := Zone{
			Name:         server.Zone,
			PhysicalPool: server.PhysicalPool,
			Desc:         "",
		}
		index := slices.Index(ctCmd.topology.Zones, zone)
		if index == -1 {
			ctCmd.topology.Zones = append(ctCmd.topology.Zones, zone)
		}
	}
	return cmderror.ErrSuccess()
}
func (lZRpc *ListZonesRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lZRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (lZRpc *ListZonesRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lZRpc.TopologyClient.ListPoolZone(ctx, lZRpc.Request)
}

func (ctCmd *ClusterTopoCmd) listZonesInPhyPool(poolId uint32, name string) (*topology.ListPoolZoneResponse, *cmderror.CmdError) {
	request := &topology.ListPoolZoneRequest{
		PhysicalPoolID:   &poolId,
		PhysicalPoolName: &name,
	}
	ctCmd.listZonesRpc = &ListZonesRpc{
		Request: request,
	}
	ctCmd.listZonesRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "ListZonesInPhysicalPool")
	result, err := basecmd.GetRpcResponse(ctCmd.listZonesRpc.Info, ctCmd.listZonesRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err
	}
	response := result.(*topology.ListPoolZoneResponse)
	return response, cmderror.ErrSuccess()
}

func (ctCmd *ClusterTopoCmd) scanZones() *cmderror.CmdError {
	// scan zone
	for _, pool := range ctCmd.clusterPhyPoolsInfo {
		response, err := ctCmd.listZonesInPhyPool(pool.GetPhysicalPoolID(), pool.GetPhysicalPoolName())
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		if response.GetStatusCode() != 0 {
			return cmderror.ErrListZonesInPhyPool(statuscode.TopoStatusCode(response.GetStatusCode()))
		}
		ctCmd.clusterZonesInfo = append(ctCmd.clusterZonesInfo, response.GetZones()...)
	}

	// update delete zone
	compare := func(zone Zone, zoneInfo *topology.ZoneInfo) bool {
		return zone.Name == zoneInfo.GetZoneName() && zone.PhysicalPool == zoneInfo.GetPhysicalPoolName()
	}
	for _, zoneInfo := range ctCmd.clusterZonesInfo {
		index := slices.IndexFunc(ctCmd.topology.Zones, func(zone Zone) bool {
			return compare(zone, zoneInfo)
		})
		if index == -1 {
			id := zoneInfo.GetZoneID()
			zName := zoneInfo.GetZoneName()
			phyPoolName := zoneInfo.GetPhysicalPoolName()
			request := &topology.ZoneRequest{
				ZoneID:           &id,
				ZoneName:         &zName,
				PhysicalPoolName: &phyPoolName,
			}
			ctCmd.zonesToBeDeleted = append(ctCmd.zonesToBeDeleted, request)
			row := make(map[string]string)
			row[cobrautil.ROW_NAME] = zoneInfo.GetZoneName()
			row[cobrautil.ROW_TYPE] = cobrautil.TYPE_ZONE
			row[cobrautil.ROW_OPERATION] = cobrautil.ROW_VALUE_DEL
			row[cobrautil.ROW_PARENT] = zoneInfo.GetPhysicalPoolName()
			ctCmd.rows = append(ctCmd.rows, row)
			ctCmd.TableNew.Append(cobrautil.Map2List(row, ctCmd.Header))
		}
	}

	// update create zone
	for _, zone := range ctCmd.topology.Zones {
		index := slices.IndexFunc(ctCmd.clusterZonesInfo, func(zoneInfo *topology.ZoneInfo) bool {
			return compare(zone, zoneInfo)
		})
		if index == -1 {
			desc := ""
			z := zone
			request := &topology.ZoneRequest{
				ZoneName:         &z.Name,
				PhysicalPoolName: &z.PhysicalPool,
				Desc:             &desc,
			}
			ctCmd.zonesToBeCreated = append(ctCmd.zonesToBeCreated, request)
			row := make(map[string]string)
			row[cobrautil.ROW_NAME] = z.Name
			row[cobrautil.ROW_TYPE] = cobrautil.TYPE_ZONE
			row[cobrautil.ROW_OPERATION] = cobrautil.ROW_VALUE_ADD
			row[cobrautil.ROW_PARENT] = z.PhysicalPool
			ctCmd.rows = append(ctCmd.rows, row)
			ctCmd.TableNew.Append(cobrautil.Map2List(row, ctCmd.Header))
		}
	}

	return cmderror.ErrSuccess()
}

func (dzRpc *DelZoneRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	dzRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (dzRpc *DelZoneRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return dzRpc.TopologyClient.DeleteZone(ctx, dzRpc.Request)
}

func (ctCmd *ClusterTopoCmd) removeZones() *cmderror.CmdError {
	ctCmd.delZoneRpc = &DelZoneRpc{}
	ctCmd.delZoneRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "DeleteZone")
	for _, delReq := range ctCmd.zonesToBeDeleted {
		ctCmd.delZoneRpc.Request = delReq
		result, err := basecmd.GetRpcResponse(ctCmd.delZoneRpc.Info, ctCmd.delZoneRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.ZoneResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrDelZone(statuscode.TopoStatusCode(response.GetStatusCode()), cobrautil.TYPE_ZONE, fmt.Sprintf("%d", delReq.GetZoneID()))
		}
	}
	return cmderror.ErrSuccess()
}

func (czRpc *CreateZoneRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	czRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (czRpc *CreateZoneRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return czRpc.TopologyClient.CreateZone(ctx, czRpc.Request)
}

func (ctCmd *ClusterTopoCmd) CreateZones() *cmderror.CmdError {
	ctCmd.createZoneRpc = &CreateZoneRpc{}
	ctCmd.createZoneRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "CreateZone")
	for _, req := range ctCmd.zonesToBeCreated {
		ctCmd.createZoneRpc.Request = req
		result, err := basecmd.GetRpcResponse(ctCmd.createZoneRpc.Info, ctCmd.createZoneRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.ZoneResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrCreateBsTopology(statuscode.TopoStatusCode(response.GetStatusCode()), cobrautil.TYPE_ZONE, req.GetZoneName())
		}
	}
	return cmderror.ErrSuccess()
}
