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
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

type Zone struct {
	Name     string
	PoolName string
}

type DeleteZoneRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.DeleteZoneRequest
	topologyClient topology.TopologyServiceClient
}

func (dzRpc *DeleteZoneRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	dzRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (dzRpc *DeleteZoneRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return dzRpc.topologyClient.DeleteZone(ctx, dzRpc.Request)
}

var _ basecmd.RpcFunc = (*DeleteZoneRpc)(nil) // check interface

type CreateZoneRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.CreateZoneRequest
	topologyClient topology.TopologyServiceClient
}

func (czRpc *CreateZoneRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	czRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (czRpc *CreateZoneRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return czRpc.topologyClient.CreateZone(ctx, czRpc.Request)
}

var _ basecmd.RpcFunc = (*CreateZoneRpc)(nil) // check interface

type ListPoolZoneRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPoolZoneRequest
	topologyClient topology.TopologyServiceClient
}

func (lpzRpc *ListPoolZoneRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lpzRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lpzRpc *ListPoolZoneRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lpzRpc.topologyClient.ListPoolZone(ctx, lpzRpc.Request)
}

var _ basecmd.RpcFunc = (*ListPoolZoneRpc)(nil) // check interface

func (tCmd *TopologyCommand) listPoolZone(poolId uint32) (*topology.ListPoolZoneResponse, *cmderror.CmdError) {
	request := &topology.ListPoolZoneRequest{
		PoolID: &poolId,
	}
	tCmd.listPoolZoneRpc = &ListPoolZoneRpc{
		Request: request,
	}
	tCmd.listPoolZoneRpc.Info = basecmd.NewRpc(tCmd.addrs, tCmd.timeout, tCmd.retryTimes, "ListPoolZone")
	result, err := basecmd.GetRpcResponse(tCmd.listPoolZoneRpc.Info, tCmd.listPoolZoneRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err
	}
	response := result.(*topology.ListPoolZoneResponse)
	return response, cmderror.ErrSuccess()
}

func (tCmd *TopologyCommand) scanZones() *cmderror.CmdError {
	// scan zone
	for _, pool := range tCmd.clusterPoolsInfo {
		response, err := tCmd.listPoolZone(pool.GetPoolID())
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		if response.GetStatusCode() != topology.TopoStatusCode_TOPO_OK {
			return cmderror.ErrListPool(response.GetStatusCode())
		}
		tCmd.clusterZonesInfo = append(tCmd.clusterZonesInfo, response.GetZones()...)
	}

	// update delete zone
	compare := func(zone Zone, zoneInfo *topology.ZoneInfo) bool {
		return zone.Name == zoneInfo.GetZoneName() && zone.PoolName == zoneInfo.GetPoolName()
	}
	for _, zoneInfo := range tCmd.clusterZonesInfo {
		index := slices.IndexFunc(tCmd.topology.Zones, func(zone Zone) bool {
			return compare(zone, zoneInfo)
		})
		if index == -1 {
			id := zoneInfo.GetZoneID()
			request := &topology.DeleteZoneRequest{
				ZoneID: &id,
			}
			tCmd.deleteZone = append(tCmd.deleteZone, request)
			row := make(map[string]string)
			row[curveutil.ROW_NAME] = zoneInfo.GetZoneName()
			row[curveutil.ROW_TYPE] = curveutil.TYPE_ZONE
			row[curveutil.ROW_OPERATION] = curveutil.ROW_VALUE_DEL
			row[curveutil.ROW_PARENT] = zoneInfo.GetPoolName()
			tCmd.rows = append(tCmd.rows, row)
			tCmd.TableNew.Append(curveutil.Map2List(row, tCmd.Header))
		}
	}

	// update create zone
	for _, zone := range tCmd.topology.Zones {
		index := slices.IndexFunc(tCmd.clusterZonesInfo, func(zoneInfo *topology.ZoneInfo) bool {
			return compare(zone, zoneInfo)
		})
		if index == -1 {
			request := &topology.CreateZoneRequest{
				ZoneName: &zone.Name,
				PoolName: &zone.PoolName,
			}
			tCmd.createZone = append(tCmd.createZone, request)
			row := make(map[string]string)
			row[curveutil.ROW_NAME] = zone.Name
			row[curveutil.ROW_TYPE] = curveutil.TYPE_ZONE
			row[curveutil.ROW_OPERATION] = curveutil.ROW_VALUE_ADD
			row[curveutil.ROW_PARENT] = zone.PoolName
			tCmd.rows = append(tCmd.rows, row)
			tCmd.TableNew.Append(curveutil.Map2List(row, tCmd.Header))
		}
	}

	return cmderror.ErrSuccess()
}

func (tCmd *TopologyCommand) removeZones() *cmderror.CmdError {
	tCmd.deleteZoneRpc = &DeleteZoneRpc{}
	tCmd.deleteZoneRpc.Info = basecmd.NewRpc(tCmd.addrs, tCmd.timeout, tCmd.retryTimes, "DeleteZone")
	for _, delReuest := range tCmd.deleteZone {
		tCmd.deleteZoneRpc.Request = delReuest
		result, err := basecmd.GetRpcResponse(tCmd.deleteZoneRpc.Info, tCmd.deleteZoneRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.DeleteZoneResponse)
		if response.GetStatusCode() != topology.TopoStatusCode_TOPO_OK {
			return cmderror.ErrDeleteTopology(response.GetStatusCode(), curveutil.TYPE_ZONE, fmt.Sprintf("%d", delReuest.GetZoneID()))
		}
	}
	return cmderror.ErrSuccess()
}

func (tCmd *TopologyCommand) createZones() *cmderror.CmdError {
	tCmd.createZoneRpc = &CreateZoneRpc{}
	tCmd.createZoneRpc.Info = basecmd.NewRpc(tCmd.addrs, tCmd.timeout, tCmd.retryTimes, "CreateZone")
	for _, crtReuest := range tCmd.createZone {
		tCmd.createZoneRpc.Request = crtReuest
		result, err := basecmd.GetRpcResponse(tCmd.createZoneRpc.Info, tCmd.createZoneRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.CreateZoneResponse)
		if response.GetStatusCode() != topology.TopoStatusCode_TOPO_OK {
			return cmderror.ErrCreateTopology(response.GetStatusCode(), curveutil.TYPE_ZONE, crtReuest.GetZoneName())
		}
	}
	return cmderror.ErrSuccess()
}

// The zone is not marked separately
// so it needs to be read and created from the servers
func (tCmd *TopologyCommand) updateZone() *cmderror.CmdError {
	// update zone
	for _, server := range tCmd.topology.Servers {
		err := tCmd.checkPool(server.PoolName)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		zone := Zone{
			Name:     server.ZoneName,
			PoolName: server.PoolName,
		}
		index := slices.Index(tCmd.topology.Zones, zone)
		if index == -1 {
			tCmd.topology.Zones = append(tCmd.topology.Zones, zone)
		}
	}
	return cmderror.ErrSuccess()
}
