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
 * Created Date: 2022-06-14
 * Author: chengyi (Cyber-SiKu)
 */

package partition

import (
	"context"
	"fmt"
	"strconv"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/list/fs"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type ListPartitionRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPartitionRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListPartitionRpc)(nil) // check interface

type PartitionCommand struct {
	basecmd.FinalCurveCmd
	Rpc       []*ListPartitionRpc
	fsId2Rows map[uint32][]map[string]string
}

var _ basecmd.FinalCurveCmdFunc = (*PartitionCommand)(nil) // check interface

func (lpRp *ListPartitionRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lpRp.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lpRp *ListPartitionRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lpRp.topologyClient.ListPartition(ctx, lpRp.Request)
}

func NewPartitionCommand() *cobra.Command {
	pCmd := &PartitionCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "partition",
			Short: "list partition in curvefs by fsid",
		},
	}
	basecmd.NewFinalCurveCli(&pCmd.FinalCurveCmd, pCmd)
	return pCmd.Cmd
}

func (pCmd *PartitionCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
	config.AddFsMdsAddrFlag(pCmd.Cmd)
	config.AddFsIdOptionDefaultAllFlag(pCmd.Cmd)
}

func (pCmd *PartitionCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(pCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	table, err := gotable.Create("partition id", "fs id", "pool id", "copyset id", "start", "end", "status")
	if err != nil {
		return err
	}
	pCmd.Table = table
	fsIds, _ := pCmd.Cmd.Flags().GetStringSlice(config.CURVEFS_FSID)
	if fsIds[0] == "*" {
		var getFsIdErr *cmderror.CmdError
		fsIds, getFsIdErr = fs.GetFsIds()
		if getFsIdErr.TypeCode() != cmderror.CODE_SUCCESS {
			return fmt.Errorf(getFsIdErr.Message)
		}
	}
	pCmd.fsId2Rows = make(map[uint32][]map[string]string)
	for _, fsId := range fsIds {
		id, err := strconv.ParseUint(fsId, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid fsId: %s", fsId)
		}
		request := &topology.ListPartitionRequest{}
		id32 := uint32(id)
		request.FsId = &id32
		rpc := &ListPartitionRpc{
			Request: request,
		}
		timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
		retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
		rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "ListPartition")
		pCmd.Rpc = append(pCmd.Rpc, rpc)
		pCmd.fsId2Rows[id32] = make([]map[string]string, 1)
		pCmd.fsId2Rows[id32][0] = make(map[string]string)
		pCmd.fsId2Rows[id32][0]["fs id"] = fsId
		pCmd.fsId2Rows[id32][0]["pool id"] = "DNE"
		pCmd.fsId2Rows[id32][0]["copyset id"] = "DNE"
		pCmd.fsId2Rows[id32][0]["partition id"] = "DNE"
		pCmd.fsId2Rows[id32][0]["start"] = "DNE"
		pCmd.fsId2Rows[id32][0]["end"] = "DNE"
		pCmd.fsId2Rows[id32][0]["status"] = "DNE"
	}

	return nil
}

func (pCmd *PartitionCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *PartitionCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var infos []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range pCmd.Rpc {
		infos = append(infos, rpc.Info)
		funcs = append(funcs, rpc)
	}
	results, err := basecmd.GetRpcListResponse(infos, funcs)
	var errs []*cmderror.CmdError
	var resList []interface{}
	errs = append(errs, err)
	for _, result := range results {
		response := result.(*topology.ListPartitionResponse)
		res, err := output.MarshalProtoJson(response)
		if err != nil {
			errMar := cmderror.ErrMarShalProtoJson()
			errMar.Format(err.Error())
			errs = append(errs, errMar)
		}
		resList = append(resList, res)

		// update fsId2Rows
		partitionList := response.GetPartitionInfoList()
		for _, partition := range partitionList {
			fsId := partition.GetFsId()
			rows := pCmd.fsId2Rows[fsId]
			var row map[string]string
			if len(rows) == 1 && rows[0]["pool id"] == "DNE" {
				row = rows[0]
			} else {
				row = make(map[string]string)
				row["fs id"] = strconv.FormatUint(uint64(fsId), 10)
			}
			row["pool id"] = strconv.FormatUint(uint64(partition.GetPoolId()), 10)
			row["copyset id"] = strconv.FormatUint(uint64(partition.GetCopysetId()), 10)
			row["partition id"] = strconv.FormatUint(uint64(partition.GetPartitionId()), 10)
			row["start"] = strconv.FormatUint(uint64(partition.GetStart()), 10)
			row["end"] = strconv.FormatUint(uint64(partition.GetEnd()), 10)
			row["status"] = partition.GetStatus().String()
		}
	}

	pCmd.updateTable()
	pCmd.Result = resList
	pCmd.Error = cmderror.MostImportantCmdError(errs)

	return nil
}

func (pCmd *PartitionCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *PartitionCommand) updateTable() {
	for _, rows := range pCmd.fsId2Rows {
		for _, row := range rows {
			pCmd.Table.AddRow(row)
		}
	}
}
