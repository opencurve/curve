/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: CurveCli
 * Created Date: 2022-06-30
 * Author: chengyi (Cyber-SiKu)
 */

package topology

import (
	"context"
	"encoding/json"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

type Pool struct {
	Name         string `json:"name"`
	ReplicasNum  uint32 `json:"replicasnum"`
	ZoneNum      uint32 `json:"zonenum"`
	CopysetNum   uint64 `json:"copysetnum"`
	PoolType     uint64 `json:"type"`
	ScatterWidth uint64 `json:"scatterwidth"`
	PhysicalPool string `json:"physicalpool"`
}

type Policy struct {
	ReplicaNum uint32 `json:"replicaNum"`
	CopysetNum uint64 `json:"copysetNum"`
	ZoneNum    uint32 `json:"zoneNum"`
}

type DeletePoolRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.DeletePoolRequest
	topologyClient topology.TopologyServiceClient
}

func (dpRpc *DeletePoolRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	dpRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (dpRpc *DeletePoolRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return dpRpc.topologyClient.DeletePool(ctx, dpRpc.Request)
}

var _ basecmd.RpcFunc = (*DeletePoolRpc)(nil) // check interface

type CreatePoolRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.CreatePoolRequest
	topologyClient topology.TopologyServiceClient
}

func (cpRpc *CreatePoolRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	cpRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (cpRpc *CreatePoolRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return cpRpc.topologyClient.CreatePool(ctx, cpRpc.Request)
}

var _ basecmd.RpcFunc = (*CreatePoolRpc)(nil) // check interface

type ListPoolRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPoolRequest
	topologyClient topology.TopologyServiceClient
}

func (lpRpc *ListPoolRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lpRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lpRpc *ListPoolRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lpRpc.topologyClient.ListPool(ctx, lpRpc.Request)
}

func (tCmd *TopologyCommand) listPool() (*topology.ListPoolResponse, *cmderror.CmdError) {
	tCmd.listPoolRpc = &ListPoolRpc{}
	tCmd.listPoolRpc.Request = &topology.ListPoolRequest{}
	tCmd.listPoolRpc.Info = basecmd.NewRpc(tCmd.addrs, tCmd.timeout, tCmd.retryTimes, "ListPool")
	result, err := basecmd.GetRpcResponse(tCmd.listPoolRpc.Info, tCmd.listPoolRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err
	}
	response := result.(*topology.ListPoolResponse)
	return response, cmderror.ErrSuccess()
}

func (tCmd *TopologyCommand) scanPools() *cmderror.CmdError {
	// scan pool
	response, err := tCmd.listPool()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	if response.GetStatusCode() != topology.TopoStatusCode_TOPO_OK {
		return cmderror.ErrListPool(response.GetStatusCode())
	}
	tCmd.clusterPoolsInfo = response.GetPoolInfos()
	// update delete pool
	compare := func(pool Pool, poolInfo *topology.PoolInfo) bool {
		return pool.Name == poolInfo.GetPoolName()
	}
	for _, poolInfo := range tCmd.clusterPoolsInfo {
		index := slices.IndexFunc(tCmd.topology.Pools, func(pool Pool) bool {
			return compare(pool, poolInfo)
		})
		if index == -1 {
			id := poolInfo.GetPoolID()
			request := &topology.DeletePoolRequest{
				PoolID: &id,
			}
			tCmd.deletePool = append(tCmd.deletePool, request)
			row := make(map[string]string)
			row[cobrautil.ROW_NAME] = poolInfo.GetPoolName()
			row[cobrautil.ROW_TYPE] = cobrautil.TYPE_POOL
			row[cobrautil.ROW_OPERATION] = cobrautil.ROW_VALUE_DEL
			row[cobrautil.ROW_PARENT] = ""
			tCmd.rows = append(tCmd.rows, row)
			tCmd.TableNew.Append(cobrautil.Map2List(row, tCmd.Header))
		}
	}

	// update create pool
	for _, pool := range tCmd.topology.Pools {
		index := slices.IndexFunc(tCmd.clusterPoolsInfo, func(poolInfo *topology.PoolInfo) bool {
			return compare(pool, poolInfo)
		})
		if index == -1 {
			policy := Policy{
				ReplicaNum: pool.ReplicasNum,
				CopysetNum: pool.CopysetNum,
				ZoneNum:    pool.ZoneNum,
			}
			policyByte, _ := json.Marshal(policy)
			request := &topology.CreatePoolRequest{
				PoolName:                     &pool.Name,
				RedundanceAndPlaceMentPolicy: policyByte,
			}
			tCmd.createPool = append(tCmd.createPool, request)
			row := make(map[string]string)
			row[cobrautil.ROW_NAME] = pool.Name
			row[cobrautil.ROW_TYPE] = cobrautil.TYPE_POOL
			row[cobrautil.ROW_OPERATION] = cobrautil.ROW_VALUE_ADD
			row[cobrautil.ROW_PARENT] = ""
			tCmd.rows = append(tCmd.rows, row)
			tCmd.TableNew.Append(cobrautil.Map2List(row, tCmd.Header))
		}
	}

	return err
}

func (tCmd *TopologyCommand) removePools() *cmderror.CmdError {
	tCmd.deletePoolRpc = &DeletePoolRpc{}
	tCmd.deletePoolRpc.Info = basecmd.NewRpc(tCmd.addrs, tCmd.timeout, tCmd.retryTimes, "DeleteServer")
	for _, delReuest := range tCmd.deletePool {
		tCmd.deletePoolRpc.Request = delReuest
		result, err := basecmd.GetRpcResponse(tCmd.deletePoolRpc.Info, tCmd.deletePoolRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.DeletePoolResponse)
		if response.GetStatusCode() != topology.TopoStatusCode_TOPO_OK {
			return cmderror.ErrDeleteTopology(response.GetStatusCode(), cobrautil.TYPE_POOL, fmt.Sprintf("%d", delReuest.GetPoolID()))
		}
	}
	return cmderror.ErrSuccess()
}

func (tCmd *TopologyCommand) createPools() *cmderror.CmdError {
	tCmd.createPoolRpc = &CreatePoolRpc{}
	tCmd.createPoolRpc.Info = basecmd.NewRpc(tCmd.addrs, tCmd.timeout, tCmd.retryTimes, "CreatePool")
	for _, crtReuest := range tCmd.createPool {
		tCmd.createPoolRpc.Request = crtReuest
		result, err := basecmd.GetRpcResponse(tCmd.createPoolRpc.Info, tCmd.createPoolRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.CreatePoolResponse)
		if response.GetStatusCode() != topology.TopoStatusCode_TOPO_OK {
			return cmderror.ErrCreateTopology(response.GetStatusCode(), cobrautil.TYPE_POOL, crtReuest.GetPoolName())
		}
	}
	return cmderror.ErrSuccess()
}

// Check if the pool in the server exists in the cluster and json.
// if not exist return error
func (tCmd *TopologyCommand) checkPool(poolName string) *cmderror.CmdError {
	indexPool := slices.IndexFunc(tCmd.topology.Pools, func(pool Pool) bool {
		return pool.Name == poolName
	})
	indexCluster := slices.IndexFunc(tCmd.clusterPoolsInfo, func(poolInfo *topology.PoolInfo) bool {
		return poolInfo.GetPoolName() == poolName
	})
	if indexPool == -1 && indexCluster == -1 {
		err := cmderror.ErrCheckPoolTopology()
		err.Format(poolName)
		return err
	}
	return cmderror.ErrSuccess()
}
