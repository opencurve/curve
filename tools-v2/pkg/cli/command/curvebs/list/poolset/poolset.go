/*
 *  Copyright (c) 2023 NetEase Inc.
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

package poolset

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/dustin/go-humanize"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	logicalpool "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/logicalPool"
	physicalpool "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/physicalPool"
	filesize "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/file"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	poolsetsExample = `$ curve bs list poolset`
)

type ListPoolsetRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPoolsetRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListPoolsetRpc)(nil) // check interface

type PoolsetCommand struct {
	basecmd.FinalCurveCmd
	Rpc *ListPoolsetRpc
}

type PoolsetUsage struct {
	id            uint32
	name          string
	total         uint64
	created       uint64
	allocated     uint64
	recyclable    uint64
	physicalPools []uint32
}

var _ basecmd.FinalCurveCmdFunc = (*PoolsetCommand)(nil) // check interface

func (rpc *ListPoolsetRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	rpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (rpc *ListPoolsetRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return rpc.topologyClient.ListPoolset(ctx, rpc.Request)
}

func NewPoolsetCommand() *cobra.Command {
	return NewListPoolsetCommand().Cmd
}

func NewListPoolsetCommand() *PoolsetCommand {
	poolsetCmd := &PoolsetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "poolset",
			Short:   "list all poolsets and space usage",
			Example: poolsetsExample,
		},
	}

	basecmd.NewFinalCurveCli(&poolsetCmd.FinalCurveCmd, poolsetCmd)
	return poolsetCmd
}

func (poolsetCmd *PoolsetCommand) AddFlags() {
	config.AddBsMdsFlagOption(poolsetCmd.Cmd)
	config.AddRpcRetryTimesFlag(poolsetCmd.Cmd)
	config.AddRpcTimeoutFlag(poolsetCmd.Cmd)
	config.AddHttpTimeoutFlag(poolsetCmd.Cmd)
}

func (poolsetCmd *PoolsetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&poolsetCmd.FinalCurveCmd, poolsetCmd)
}

func (poolsetCmd *PoolsetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&poolsetCmd.FinalCurveCmd)
}

func (poolsetCmd *PoolsetCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(poolsetCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	timeout := config.GetFlagDuration(poolsetCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(poolsetCmd.Cmd, config.RPCRETRYTIMES)
	poolsetCmd.Rpc = &ListPoolsetRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListPoolset"),
		Request: &topology.ListPoolsetRequest{},
	}

	header := []string{
		cobrautil.ROW_ID,
		cobrautil.ROW_NAME,
		cobrautil.ROW_PHYPOOLS,
		cobrautil.ROW_TOTAL,
		cobrautil.ROW_CREATED,
		cobrautil.ROW_ALLOC,
		cobrautil.ROW_RECYCLE,
		cobrautil.ROW_ALLOC_PERCENT,
	}
	poolsetCmd.SetHeader(header)
	return nil
}

func buildPhysicalPoolToPoolsetMap(physicalPools []*topology.PhysicalPoolInfo) map[uint32]uint32 {
	phyPoolToPoolset := make(map[uint32]uint32)
	for _, phyPool := range physicalPools {
		phyPoolToPoolset[phyPool.GetPhysicalPoolID()] = phyPool.GetPoolsetId()
	}
	return phyPoolToPoolset
}

func buildPoolsetIdToNameMap(resp *topology.ListPoolsetResponse) map[uint32]string {
	idToMap := make(map[uint32]string)

	for _, info := range resp.GetPoolsetInfos() {
		idToMap[info.GetPoolsetID()] = info.GetPoolsetName()
	}

	return idToMap
}

func stringifyPhysicalPools(pools []uint32) string {
	sort.Slice(pools, func(i, j int) bool {
		return pools[i] < pools[j]
	})
	strs := make([]string, 0)
	for _, pool := range pools {
		strs = append(strs, fmt.Sprintf("%d", pool))
	}
	return strings.Join(strs, ", ")
}

func getCreatedFileSize(fileSizeMap map[string]uint64, name string) string {
	if size, ok := fileSizeMap[name]; ok {
		return humanize.IBytes(size)
	}
	return cobrautil.ROW_VALUE_NO_VALUE
}

func (poolsetCmd *PoolsetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	listPoolsetResp, err := basecmd.GetRpcResponse(poolsetCmd.Rpc.Info, poolsetCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	phyPools, err := physicalpool.GetPhysicalPool(poolsetCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	logPools, capacity, allocated, recyclable, err := logicalpool.ListLogicalPoolInfoAndAllocSize(poolsetCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	logicalPoolInfos := make([]*topology.LogicalPoolInfo, 0)
	for _, info := range logPools {
		logicalPoolInfos = append(logicalPoolInfos, info.GetLogicalPoolInfos()...)
	}

	fileSizeMap, _ := filesize.GetCreatedFileSizeGroupedByPoolset(poolsetCmd.Cmd)
	poolsetIdToName := buildPoolsetIdToNameMap(listPoolsetResp.(*topology.ListPoolsetResponse))
	phyPoolToPoolset := buildPhysicalPoolToPoolsetMap(phyPools)

	poolsets := make(map[uint32]PoolsetUsage)

	for idx, logicalPool := range logicalPoolInfos {
		phyPoolId := logicalPool.GetPhysicalPoolID()
		poolsetId, ok := phyPoolToPoolset[phyPoolId]
		if !ok {
			err := cmderror.NewInternalCmdError(1, "physical pool not found")
			return err.ToError()
		}

		if poolset, ok := poolsets[poolsetId]; !ok {
			poolsets[poolsetId] = PoolsetUsage{
				id:            poolsetId,
				name:          poolsetIdToName[poolsetId],
				total:         capacity[idx],
				allocated:     allocated[idx],
				recyclable:    recyclable[idx],
				physicalPools: []uint32{phyPoolId},
			}
		} else {
			poolset.total += capacity[idx]
			poolset.allocated += allocated[idx]
			poolset.recyclable += recyclable[idx]
			poolset.physicalPools = append(poolset.physicalPools, phyPoolId)

			poolsets[poolsetId] = poolset
		}
	}

	rows := make([]map[string]string, 0)
	for _, poolset := range poolsets {
		row := make(map[string]string)
		row[cobrautil.ROW_ID] = fmt.Sprintf("%d", poolset.id)
		row[cobrautil.ROW_NAME] = poolset.name
		row[cobrautil.ROW_PHYPOOLS] = stringifyPhysicalPools(poolset.physicalPools)
		row[cobrautil.ROW_TOTAL] = humanize.IBytes(poolset.total)
		row[cobrautil.ROW_CREATED] = getCreatedFileSize(fileSizeMap, poolset.name)
		row[cobrautil.ROW_ALLOC] = humanize.IBytes(poolset.allocated)
		row[cobrautil.ROW_RECYCLE] = humanize.IBytes(poolset.recyclable)
		row[cobrautil.ROW_ALLOC_PERCENT] = fmt.Sprintf("%.2f", float64(poolset.allocated)/float64(poolset.total)*100)

		rows = append(rows, row)
	}

	list := cobrautil.ListMap2ListSortByKeys(rows, poolsetCmd.Header, []string{
		cobrautil.ROW_ID,
	})
	poolsetCmd.TableNew.AppendBulk(list)
	poolsetCmd.Error = nil
	poolsetCmd.Result = rows

	return nil
}
