package cluster

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

type Server struct {
	Name         string `json:"name"`
	InternalIp   string `json:"internalip"`
	InternalPort uint32 `json:"internalport"`
	ExternalIp   string `json:"externalip"`
	ExternalPort uint32 `json:"externalport"`
	Zone         string `json:"zone"`
	PhysicalPool string `json:"physicalpool"`
}
type ListServersRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListZoneServerRequest
	TopologyClient topology.TopologyServiceClient
}

type DelServerRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.DeleteServerRequest
	TopologyClient topology.TopologyServiceClient
}

type RegServerRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ServerRegistRequest
	TopologyClient topology.TopologyServiceClient
}

func (lSRpc *ListServersRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lSRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (lSRpc *ListServersRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lSRpc.TopologyClient.ListZoneServer(ctx, lSRpc.Request)
}

func (ctCmd *ClusterTopoCmd) listServersInZone(zoneId uint32, zoneName string, phyPoolName string) (*topology.ListZoneServerResponse, *cmderror.CmdError) {
	request := &topology.ListZoneServerRequest{
		ZoneID:           &zoneId,
		ZoneName:         &zoneName,
		PhysicalPoolName: &phyPoolName,
	}
	ctCmd.listServersRpc = &ListServersRpc{
		Request: request,
	}
	ctCmd.listServersRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "ListServersInZone")
	result, err := basecmd.GetRpcResponse(ctCmd.listServersRpc.Info, ctCmd.listServersRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err
	}
	response := result.(*topology.ListZoneServerResponse)
	return response, cmderror.ErrSuccess()
}

func (ctCmd *ClusterTopoCmd) scanServers() *cmderror.CmdError {
	// scan server
	for _, zone := range ctCmd.clusterZonesInfo {
		response, err := ctCmd.listServersInZone(zone.GetZoneID(), zone.GetZoneName(), zone.GetPhysicalPoolName())
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		if response.GetStatusCode() != 0 {
			return cmderror.ErrListServers(statuscode.TopoStatusCode(response.GetStatusCode()))
		}
		ctCmd.clusterServersInfo = append(ctCmd.clusterServersInfo, response.GetServerInfo()...)
	}
	// update delete server
	compare := func(server Server, serverInfo *topology.ServerInfo) bool {
		return server.Name == serverInfo.GetHostName() && server.Zone == serverInfo.GetZoneName() && server.PhysicalPool == serverInfo.GetPhysicalPoolName()
	}
	for _, serverInfo := range ctCmd.clusterServersInfo {
		index := slices.IndexFunc(ctCmd.topology.Servers, func(server Server) bool {
			return compare(server, serverInfo)
		})
		if index == -1 {
			id := serverInfo.GetServerID()
			request := &topology.DeleteServerRequest{
				ServerID: &id,
			}
			ctCmd.serversToBeDeleted = append(ctCmd.serversToBeDeleted, request)
			row := make(map[string]string)
			row[curveutil.ROW_NAME] = serverInfo.GetHostName()
			row[curveutil.ROW_TYPE] = curveutil.TYPE_SERVER
			row[curveutil.ROW_OPERATION] = curveutil.ROW_VALUE_DEL
			row[curveutil.ROW_PARENT] = serverInfo.GetZoneName()
			ctCmd.rows = append(ctCmd.rows, row)
			ctCmd.TableNew.Append(curveutil.Map2List(row, ctCmd.Header))
		}
	}

	// update create server
	for _, server := range ctCmd.topology.Servers {
		index := slices.IndexFunc(ctCmd.clusterServersInfo, func(serverInfo *topology.ServerInfo) bool {
			return compare(server, serverInfo)
		})
		if index == -1 {
			var desc string = ""
			s := server
			request := &topology.ServerRegistRequest{
				HostName:         &s.Name,
				InternalIp:       &s.InternalIp,
				InternalPort:     &s.InternalPort,
				ExternalIp:       &s.ExternalIp,
				ExternalPort:     &s.ExternalPort,
				ZoneName:         &s.Zone,
				PhysicalPoolName: &s.PhysicalPool,
				Desc:             &desc,
			}
			ctCmd.serversToBeReg = append(ctCmd.serversToBeReg, request)
			row := make(map[string]string)
			row[curveutil.ROW_NAME] = s.Name
			row[curveutil.ROW_TYPE] = curveutil.TYPE_SERVER
			row[curveutil.ROW_OPERATION] = curveutil.ROW_VALUE_ADD
			row[curveutil.ROW_PARENT] = s.Zone
			ctCmd.rows = append(ctCmd.rows, row)
			ctCmd.TableNew.Append(curveutil.Map2List(row, ctCmd.Header))
		}
	}

	return cmderror.ErrSuccess()
}
func (dsRpc *DelServerRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	dsRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (dsRpc *DelServerRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return dsRpc.TopologyClient.DeleteServer(ctx, dsRpc.Request)
}
func (ctCmd *ClusterTopoCmd) removeServers() *cmderror.CmdError {
	ctCmd.delServerRpc = &DelServerRpc{}
	ctCmd.delServerRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "DeleteServer")
	for _, delReq := range ctCmd.serversToBeDeleted {
		ctCmd.delServerRpc.Request = delReq
		result, err := basecmd.GetRpcResponse(ctCmd.delServerRpc.Info, ctCmd.delServerRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.DeleteServerResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrDelServer(statuscode.TopoStatusCode(response.GetStatusCode()), curveutil.TYPE_SERVER, fmt.Sprintf("%d", delReq.GetServerID()))
		}
	}
	return cmderror.ErrSuccess()
}

func (rsRpc *RegServerRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	rsRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (rsRpc *RegServerRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return rsRpc.TopologyClient.RegistServer(ctx, rsRpc.Request)
}

func (ctCmd *ClusterTopoCmd) RegistServers() *cmderror.CmdError {
	ctCmd.regServerRpc = &RegServerRpc{}
	ctCmd.regServerRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "RegistServer")
	for _, req := range ctCmd.serversToBeReg {
		ctCmd.regServerRpc.Request = req
		result, err := basecmd.GetRpcResponse(ctCmd.regServerRpc.Info, ctCmd.regServerRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.ServerRegistResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrCreateBsTopology(statuscode.TopoStatusCode(response.GetStatusCode()), curveutil.TYPE_SERVER, req.GetHostName())
		}
	}
	return cmderror.ErrSuccess()
}
