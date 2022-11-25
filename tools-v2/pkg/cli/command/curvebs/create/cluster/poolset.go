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

type Poolset struct {
	Name string               `json:"name"`
	Type topology.PoolsetType `json:"type"`
}

type ListPoolsetsRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPoolsetRequest
	TopologyClient topology.TopologyServiceClient
}

type CreatePoolsetRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.PoolsetRequest
	TopologyClient topology.TopologyServiceClient
}

type DelPoolsetRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.PoolsetRequest
	TopologyClient topology.TopologyServiceClient
}

func (ctCmd *ClusterTopoCmd) CheckPoolset(pstName string) *cmderror.CmdError {
	indexPst := slices.IndexFunc(ctCmd.topology.Poolsets, func(poolset Poolset) bool {
		return poolset.Name == pstName
	})
	indexCluster := slices.IndexFunc(ctCmd.clusterPoolsetsInfo, func(pstInfo *topology.PoolsetInfo) bool {
		return pstInfo.GetPoolsetName() == pstName
	})
	if indexPst == -1 && indexCluster == -1 {
		err := cmderror.ErrBsCheckPoolsetTopology()
		err.Format(pstName)
		return err
	}
	return cmderror.ErrSuccess()
}
func (lPstRpc *ListPoolsetsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lPstRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (lPstRpc *ListPoolsetsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lPstRpc.TopologyClient.ListPoolset(ctx, lPstRpc.Request)
}

func (ctCmd *ClusterTopoCmd) listPoolsets() (*topology.ListPoolsetResponse, *cmderror.CmdError) {
	request := &topology.ListPoolsetRequest{}
	ctCmd.listPoolsetsRpc = &ListPoolsetsRpc{
		Request: request,
	}
	ctCmd.listPoolsetsRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "ListPoolset")
	result, err := basecmd.GetRpcResponse(ctCmd.listPoolsetsRpc.Info, ctCmd.listPoolsetsRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err
	}
	response := result.(*topology.ListPoolsetResponse)
	return response, cmderror.ErrSuccess()
}

func (ctCmd *ClusterTopoCmd) scanPoolsets() *cmderror.CmdError {
	// scan poolsets
	response, err := ctCmd.listPoolsets()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	if response.GetStatusCode() != 0 {
		return cmderror.ErrListPoolsets(statuscode.TopoStatusCode(response.GetStatusCode()))
	}
	ctCmd.clusterPoolsetsInfo = append(ctCmd.clusterPoolsetsInfo, response.GetPoolsetInfos()...)

	// update delete poolset
	compare := func(pst Poolset, pstInfo *topology.PoolsetInfo) bool {
		return pst.Name == pstInfo.GetPoolsetName()
	}
	for _, pstInfo := range ctCmd.clusterPoolsetsInfo {
		index := slices.IndexFunc(ctCmd.topology.Poolsets, func(pst Poolset) bool {
			return compare(pst, pstInfo)
		})
		if index == -1 {
			id := pstInfo.GetPoolsetID()
			name := pstInfo.GetPoolsetName()
			t := pstInfo.GetType()
			request := &topology.PoolsetRequest{
				PoolsetID:   &id,
				PoolsetName: &name,
				Type:        &t,
			}
			ctCmd.poolsetsToBeDeleted = append(ctCmd.poolsetsToBeDeleted, request)
			row := make(map[string]string)
			row[cobrautil.ROW_NAME] = pstInfo.GetPoolsetName()
			row[cobrautil.ROW_TYPE] = cobrautil.TYPE_POOLSET
			row[cobrautil.ROW_OPERATION] = cobrautil.ROW_VALUE_DEL
			row[cobrautil.ROW_PARENT] = ""
			ctCmd.rows = append(ctCmd.rows, row)
			ctCmd.TableNew.Append(cobrautil.Map2List(row, ctCmd.Header))
		}
	}

	// update create poolset
	for _, pst := range ctCmd.topology.Poolsets {
		index := slices.IndexFunc(ctCmd.clusterPoolsetsInfo, func(pstInfo *topology.PoolsetInfo) bool {
			return compare(pst, pstInfo)
		})
		if index == -1 {
			request := &topology.PoolsetRequest{
				PoolsetName: &pst.Name,
				Type:        &pst.Type,
			}
			ctCmd.poolsetsToBeCreated = append(ctCmd.poolsetsToBeCreated, request)
			row := make(map[string]string)
			row[cobrautil.ROW_NAME] = pst.Name
			row[cobrautil.ROW_TYPE] = cobrautil.TYPE_POOLSET
			row[cobrautil.ROW_OPERATION] = cobrautil.ROW_VALUE_ADD
			row[cobrautil.ROW_PARENT] = ""
			ctCmd.rows = append(ctCmd.rows, row)
			ctCmd.TableNew.Append(cobrautil.Map2List(row, ctCmd.Header))
		}
	}

	return cmderror.ErrSuccess()
}

func (dpstRpc *DelPoolsetRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	dpstRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (dpstRpc *DelPoolsetRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return dpstRpc.TopologyClient.DeletePoolset(ctx, dpstRpc.Request)
}

func (ctCmd *ClusterTopoCmd) removePoolsets() *cmderror.CmdError {
	ctCmd.delPoolsetRpc = &DelPoolsetRpc{}
	ctCmd.delZoneRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "DeletePoolset")
	for _, delReq := range ctCmd.poolsetsToBeDeleted {
		ctCmd.delPoolsetRpc.Request = delReq
		result, err := basecmd.GetRpcResponse(ctCmd.delPoolsetRpc.Info, ctCmd.delPoolsetRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.PoolsetResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrDelPoolset(statuscode.TopoStatusCode(response.GetStatusCode()), cobrautil.TYPE_POOLSET, fmt.Sprintf("%d", delReq.GetPoolsetID()))
		}
	}
	return cmderror.ErrSuccess()
}

func (cpRpc *CreatePoolsetRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	cpRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (cpRpc *CreatePoolsetRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return cpRpc.TopologyClient.CreatePoolset(ctx, cpRpc.Request)
}

func (ctCmd *ClusterTopoCmd) CreatePoolsets() *cmderror.CmdError {
	ctCmd.createPoolsetRpc = &CreatePoolsetRpc{}
	ctCmd.createPoolsetRpc.Info = basecmd.NewRpc(ctCmd.addrs, ctCmd.timeout, ctCmd.retryTimes, "CreatePoolsets")
	for _, req := range ctCmd.poolsetsToBeCreated {
		ctCmd.createPoolsetRpc.Request = req
		result, err := basecmd.GetRpcResponse(ctCmd.createPoolsetRpc.Info, ctCmd.createPoolsetRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.PoolsetResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrCreateBsTopology(statuscode.TopoStatusCode(response.GetStatusCode()), cobrautil.TYPE_POOLSET, req.GetPoolsetName())
		}
	}
	return cmderror.ErrSuccess()
}
