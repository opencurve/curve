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

type PhysicalPool struct {
	Name string `json:"name"`
	Desc string `json:"desc"`
}

type ListPhyPoolsRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPhysicalPoolRequest
	TopologyClient topology.TopologyServiceClient
}

type DelPhyPoolRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.PhysicalPoolRequest
	TopologyClient topology.TopologyServiceClient
}

type CreatePhyPoolRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.PhysicalPoolRequest
	TopologyClient topology.TopologyServiceClient
}

func (lPRpc *ListPhyPoolsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lPRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (lPRpc *ListPhyPoolsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lPRpc.TopologyClient.ListPhysicalPool(ctx, lPRpc.Request)
}

// servers with the same physcial field value are grouped together to form a physicalpool
func (ctCmd *ClusterTopoCmd) genPhyPools() *cmderror.CmdError {
	// generate physicalPools
	for _, server := range ctCmd.topology.Servers {
		phyPool := PhysicalPool{
			Name: server.PhysicalPool,
			Desc: "",
		}
		index := slices.Index(ctCmd.topology.PhysicalPools, phyPool)
		if index == -1 {
			ctCmd.topology.PhysicalPools = append(ctCmd.topology.PhysicalPools, phyPool)
		}
	}
	return cmderror.ErrSuccess()
}

// Check if the physicalPool that a server belongs to exists in the cluster.
// if not exist return error
func (ctCmd *ClusterTopoCmd) CheckPhysicalPool(poolName string) *cmderror.CmdError {
	indexPool := slices.IndexFunc(ctCmd.topology.PhysicalPools, func(phyPool PhysicalPool) bool {
		return phyPool.Name == poolName
	})
	indexCluster := slices.IndexFunc(ctCmd.clusterPhyPoolsInfo, func(poolInfo *topology.PhysicalPoolInfo) bool {
		return poolInfo.GetPhysicalPoolName() == poolName
	})
	if indexPool == -1 && indexCluster == -1 {
		err := cmderror.ErrCheckPoolTopology()
		err.Format(poolName)
		return err
	}
	return cmderror.ErrSuccess()
}

func (ctCmd *ClusterTopoCmd) scanPhyPools() *cmderror.CmdError {
	// scan physicalpool
	request := &topology.ListPhysicalPoolRequest{}
	ctCmd.listPhyPoolsRpc = &ListPhyPoolsRpc{
		Request: request,
	}
	ctCmd.listPhyPoolsRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "ListPhyPools")
	result, err := basecmd.GetRpcResponse(ctCmd.listPhyPoolsRpc.Info, ctCmd.listPhyPoolsRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	response := result.(*topology.ListPhysicalPoolResponse)
	if response.GetStatusCode() != 0 {
		return cmderror.ErrBsListPhysicalPoolRpc(statuscode.TopoStatusCode(response.GetStatusCode()))
	}

	ctCmd.clusterPhyPoolsInfo = append(ctCmd.clusterPhyPoolsInfo, response.GetPhysicalPoolInfos()...)
	// update delete physical pool
	// the physical pool in cluster but not in topology should be deleted
	compare := func(phyPool PhysicalPool, phyPoolInfo *topology.PhysicalPoolInfo) bool {
		return phyPool.Name == phyPoolInfo.GetPhysicalPoolName()
	}
	for _, phyPoolInfo := range ctCmd.clusterPhyPoolsInfo {
		index := slices.IndexFunc(ctCmd.topology.PhysicalPools, func(pool PhysicalPool) bool {
			return compare(pool, phyPoolInfo)
		})
		if index == -1 {
			id := phyPoolInfo.GetPhysicalPoolID()
			request := &topology.PhysicalPoolRequest{
				PhysicalPoolID: &id,
			}
			ctCmd.phyPoolsToBeDeleted = append(ctCmd.phyPoolsToBeDeleted, request)
			row := make(map[string]string)
			row[cobrautil.ROW_NAME] = phyPoolInfo.GetPhysicalPoolName()
			row[cobrautil.ROW_PHYPOOL] = cobrautil.TYPE_PHYPOOL
			row[cobrautil.ROW_OPERATION] = cobrautil.ROW_VALUE_DEL
			ctCmd.rows = append(ctCmd.rows, row)
			ctCmd.TableNew.Append(cobrautil.Map2List(row, ctCmd.Header))
		}
	}
	// update create physical pool
	// the physical pool in topology but not in cluster should be created
	for _, phyPool := range ctCmd.topology.PhysicalPools {
		index := slices.IndexFunc(ctCmd.clusterPhyPoolsInfo,
			func(phyPoolInfo *topology.PhysicalPoolInfo) bool {
				return compare(phyPool, phyPoolInfo)
			})
		if index == -1 {
			desc := ""
			name := phyPool.Name
			request := &topology.PhysicalPoolRequest{
				PhysicalPoolName: &name,
				Desc:             &desc,
			}
			ctCmd.phyPoolsToBeCreated = append(ctCmd.phyPoolsToBeCreated, request)
			row := make(map[string]string)
			row[cobrautil.ROW_NAME] = phyPool.Name
			row[cobrautil.ROW_TYPE] = cobrautil.TYPE_PHYPOOL
			row[cobrautil.ROW_OPERATION] = cobrautil.ROW_VALUE_ADD
			row[cobrautil.ROW_PARENT] = ""
			ctCmd.rows = append(ctCmd.rows, row)
			ctCmd.TableNew.Append(cobrautil.Map2List(row, ctCmd.Header))
		}
	}

	return err
}

func (dpoolRpc *DelPhyPoolRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	dpoolRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (dpoolRpc *DelPhyPoolRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return dpoolRpc.TopologyClient.DeletePhysicalPool(ctx, dpoolRpc.Request)
}

func (ctCmd *ClusterTopoCmd) removePhyPools() *cmderror.CmdError {
	ctCmd.delPhyPoolRpc = &DelPhyPoolRpc{}
	ctCmd.delZoneRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "DeletePhysicalPool")
	for _, delReq := range ctCmd.phyPoolsToBeDeleted {
		ctCmd.delPhyPoolRpc.Request = delReq
		result, err := basecmd.GetRpcResponse(ctCmd.delPhyPoolRpc.Info, ctCmd.delPhyPoolRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.PhysicalPoolResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrDelPhyPool(statuscode.TopoStatusCode(response.GetStatusCode()), cobrautil.TYPE_PHYPOOL, fmt.Sprintf("%d", delReq.GetPhysicalPoolID()))
		}
	}
	return cmderror.ErrSuccess()
}

func (cpRpc *CreatePhyPoolRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	cpRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (cpRpc *CreatePhyPoolRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return cpRpc.TopologyClient.CreatePhysicalPool(ctx, cpRpc.Request)
}

func (ctCmd *ClusterTopoCmd) CreatePhysicalPools() *cmderror.CmdError {
	ctCmd.createPhyPoolRpc = &CreatePhyPoolRpc{}
	ctCmd.createPhyPoolRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "CreatePhyscialPools")
	for _, req := range ctCmd.phyPoolsToBeCreated {
		ctCmd.createPhyPoolRpc.Request = req
		result, err := basecmd.GetRpcResponse(ctCmd.createPhyPoolRpc.Info, ctCmd.createPhyPoolRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.PhysicalPoolResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrCreateBsTopology(statuscode.TopoStatusCode(response.GetStatusCode()), cobrautil.TYPE_PHYPOOL, req.GetPhysicalPoolName())
		}
	}
	return cmderror.ErrSuccess()
}
