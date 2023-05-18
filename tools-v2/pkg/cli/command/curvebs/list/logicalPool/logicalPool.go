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
 * Created Date: 2022-08-25
 * Author: chengyi (Cyber-SiKu)
 */

package logicalpool

import (
	"context"
	"fmt"
	"strconv"

	"github.com/dustin/go-humanize"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	physicalpool "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/physicalPool"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/file"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	logicalPoolExample = `$ curve bs list logical-pool`
)

type ListLogicalPoolRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListLogicalPoolRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListLogicalPoolRpc)(nil) // check interface

type LogicalPoolCommand struct {
	basecmd.FinalCurveCmd
	Rpc              []*ListLogicalPoolRpc
	Metric           *basecmd.Metric
	recycleAllocRes  *nameserver2.GetAllocatedSizeResponse
	logicalPoolInfo  []*topology.ListLogicalPoolResponse
	totalCapacity    uint64
	allocatedSize    uint64
	recycleAllocSize uint64
}

var _ basecmd.FinalCurveCmdFunc = (*LogicalPoolCommand)(nil) // check interface

func (lRpc *ListLogicalPoolRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lRpc *ListLogicalPoolRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.topologyClient.ListLogicalPool(ctx, lRpc.Request)
}

func NewLogicalPoolCommand() *cobra.Command {
	return NewListLogicalPoolCommand().Cmd
}

func NewListLogicalPoolCommand() *LogicalPoolCommand {
	fsCmd := &LogicalPoolCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "logical-pool",
			Short:   "list all logical pool information",
			Example: logicalPoolExample,
		},
	}

	basecmd.NewFinalCurveCli(&fsCmd.FinalCurveCmd, fsCmd)
	return fsCmd
}

func (lCmd *LogicalPoolCommand) AddFlags() {
	config.AddBsMdsFlagOption(lCmd.Cmd)
	config.AddRpcRetryTimesFlag(lCmd.Cmd)
	config.AddRpcTimeoutFlag(lCmd.Cmd)
}

func (lCmd *LogicalPoolCommand) Init(cmd *cobra.Command, args []string) error {
	phyPools, err := physicalpool.GetPhysicalPool(lCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	mdsAddrs, err := config.GetBsMdsAddrSlice(lCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(lCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(lCmd.Cmd, config.RPCRETRYTIMES)
	for _, phy := range phyPools {
		rpc := &ListLogicalPoolRpc{
			Request: &topology.ListLogicalPoolRequest{
				PhysicalPoolID: phy.PhysicalPoolID,
			},
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListLogicalPool"),
		}
		lCmd.Rpc = append(lCmd.Rpc, rpc)
	}
	header := []string{curveutil.ROW_ID, curveutil.ROW_NAME,
		curveutil.ROW_PHYPOOL, curveutil.ROW_TYPE, curveutil.ROW_ALLOC,
		curveutil.ROW_SCAN, curveutil.ROW_TOTAL, curveutil.ROW_USED,
		curveutil.ROW_LEFT, curveutil.ROW_RECYCLE,
	}
	lCmd.SetHeader(header)
	lCmd.TableNew.SetAutoMergeCellsByColumnIndex(curveutil.GetIndexSlice(
		lCmd.Header, []string{curveutil.ROW_PHYPOOL},
	))
	lCmd.Cmd.Flags().String(config.CURVEBS_PATH, curveutil.RECYCLEBIN_PATH, "file path")
	lCmd.Cmd.Flag(config.CURVEBS_PATH).Changed = true
	lCmd.Metric = basecmd.NewMetric(mdsAddrs, "", timeout)
	res, err := file.GetAllocatedSize(lCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	lCmd.recycleAllocRes = res
	lCmd.recycleAllocSize = res.GetAllocatedSize()
	return nil
}

func (lCmd *LogicalPoolCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&lCmd.FinalCurveCmd, lCmd)
}

func (lCmd *LogicalPoolCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var infos []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range lCmd.Rpc {
		infos = append(infos, rpc.Info)
		funcs = append(funcs, rpc)
	}
	results, errs := basecmd.GetRpcListResponse(infos, funcs)
	if len(errs) == len(infos) {
		mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
		return mergeErr.ToError()
	}
	rows := make([]map[string]string, 0)
	var errors []*cmderror.CmdError
	for _, res := range results {
		if res == nil {
			continue
		}
		infos := res.(*topology.ListLogicalPoolResponse)
		for _, loPoolInfo := range infos.GetLogicalPoolInfos() {
			row := make(map[string]string)
			row[curveutil.ROW_ID] = fmt.Sprintf("%d", loPoolInfo.GetLogicalPoolID())
			row[curveutil.ROW_NAME] = loPoolInfo.GetLogicalPoolName()
			row[curveutil.ROW_PHYPOOL] = fmt.Sprintf("%d", loPoolInfo.GetPhysicalPoolID())
			row[curveutil.ROW_TYPE] = loPoolInfo.GetType().String()
			row[curveutil.ROW_ALLOC] = loPoolInfo.GetAllocateStatus().String()
			row[curveutil.ROW_SCAN] = fmt.Sprintf("%t", loPoolInfo.GetScanEnable())

			total := uint64(0)
			// capacity
			metricName := curveutil.GetPoolLogicalCapacitySubUri(loPoolInfo.GetLogicalPoolName())
			value, err := lCmd.queryMetric(metricName)
			if err.TypeCode() != cmderror.CODE_SUCCESS {
				errors = append(errors, err)
			}
			row[curveutil.ROW_TOTAL] = humanize.IBytes(value)
			total = value
			lCmd.totalCapacity += value

			// alloc size
			metricName = curveutil.GetPoolLogicalAllocSubUri(loPoolInfo.GetLogicalPoolName())
			value, err = lCmd.queryMetric(metricName)
			if err.TypeCode() != cmderror.CODE_SUCCESS {
				errors = append(errors, err)
			}
			row[curveutil.ROW_USED] = humanize.IBytes(value)
			row[curveutil.ROW_LEFT] = humanize.IBytes(total - value)
			lCmd.allocatedSize += value

			// recycle
			recycle := lCmd.recycleAllocRes.AllocSizeMap[loPoolInfo.GetLogicalPoolID()]
			row[curveutil.ROW_RECYCLE] = humanize.IBytes(recycle)
			rows = append(rows, row)
		}
	}
	list := curveutil.ListMap2ListSortByKeys(rows, lCmd.Header, []string{
		curveutil.ROW_PHYPOOL, curveutil.ROW_ID,
	})
	lCmd.TableNew.AppendBulk(list)
	errRet := cmderror.MergeCmdError(errors)
	lCmd.Error = errRet
	lCmd.Result = rows
	return nil
}

func (lCmd *LogicalPoolCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&lCmd.FinalCurveCmd)
}

func (lCmd *LogicalPoolCommand) queryMetric(metricName string) (uint64, *cmderror.CmdError) {
	lCmd.Metric.SubUri = metricName
	metric, err := basecmd.QueryMetric(lCmd.Metric)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return 0, err
	} else {
		valueStr, err := basecmd.GetMetricValue(metric)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return 0, err
		}
		value, errP := strconv.ParseUint(valueStr, 10, 64)
		if errP != nil {
			pErr := cmderror.ErrParse()
			pErr.Format(metricName, pErr)
			return 0, err
		}
		return value, cmderror.Success()
	}
}

func ListLogicalPoolInfoAndAllocSize(caller *cobra.Command) ([]*topology.ListLogicalPoolResponse, uint64, uint64, uint64, *cmderror.CmdError) {
	listCmd := NewListLogicalPoolCommand()
	config.AlignFlagsValue(caller, listCmd.Cmd, []string{
		config.CURVEBS_MDSADDR, config.RPCRETRYTIMES, config.RPCTIMEOUT,
	})
	listCmd.Cmd.SilenceErrors = true
	listCmd.Cmd.SilenceUsage = true
	listCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := listCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsListLogicalPoolInfo()
		retErr.Format(err.Error())
		return nil, 0, 0, 0, retErr
	}
	return listCmd.logicalPoolInfo, listCmd.totalCapacity, listCmd.allocatedSize, listCmd.recycleAllocSize, cmderror.Success()
}
