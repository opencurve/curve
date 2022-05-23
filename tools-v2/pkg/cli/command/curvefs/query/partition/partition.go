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
 * Created Date: 2022-06-17
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
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type QueryPartitionRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.GetCopysetOfPartitionRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*QueryPartitionRpc)(nil) // check interface

type PartitionCommand struct {
	basecmd.FinalCurveCmd
	Rpc *QueryPartitionRpc
}

var _ basecmd.FinalCurveCmdFunc = (*PartitionCommand)(nil) // check interface

func (qpRpc *QueryPartitionRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	qpRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (qpRpc *QueryPartitionRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return qpRpc.topologyClient.GetCopysetOfPartition(ctx, qpRpc.Request)
}

func NewPartitionCommand() *cobra.Command {
	partitionCmd := &PartitionCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "partition",
			Short: "query the copyset of partition",
		},
	}
	basecmd.NewFinalCurveCli(&partitionCmd.FinalCurveCmd, partitionCmd)
	return partitionCmd.Cmd
}

func (pCmd *PartitionCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
	config.AddFsMdsAddrFlag(pCmd.Cmd)
	config.AddPartitionIdRequiredFlag(pCmd.Cmd)
}

func (pCmd *PartitionCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(pCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	table, err := gotable.Create("id", "pool id", "copyset id", "peer id", "peer address")
	if err != nil {
		return err
	}
	pCmd.Table = table

	partitionIds := viper.GetStringSlice(config.VIPER_CURVEFS_PARTITIONID)

	var partitionIdList []uint32
	for i := range partitionIds {
		id, err := strconv.ParseUint(partitionIds[i], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid %s: %s", config.CURVEFS_PARTITIONID, partitionIds[i])
		}
		id32 := uint32(id)
		partitionIdList = append(partitionIdList, id32)
	}
	request := &topology.GetCopysetOfPartitionRequest{
		PartitionId: partitionIdList,
	}
	pCmd.Rpc = &QueryPartitionRpc{
		Request: request,
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	pCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetCopysetOfPartition")

	return nil
}

func (pCmd *PartitionCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *PartitionCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(pCmd.Rpc.Info, pCmd.Rpc)
	var errs []*cmderror.CmdError
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		errs = append(errs, err)
	}
	response := result.(*topology.GetCopysetOfPartitionResponse)
	errStatus := cmderror.ErrGetCopysetOfPartition(int(response.GetStatusCode()))
	errs = append(errs, errStatus)

	res, errTranslate := output.MarshalProtoJson(response)
	if errTranslate != nil {
		errMar := cmderror.ErrMarShalProtoJson()
		errMar.Format(errTranslate.Error())
		errs = append(errs, errMar)
	}

	var rows []map[string]string
	copysetMap := response.GetCopysetMap()
	for k, v := range copysetMap {
		for _, peer := range v.GetPeers() {
			row := make(map[string]string)
			row["id"] = strconv.Itoa(int(k))
			row["pool id"] = strconv.Itoa(int(v.GetPoolId()))
			row["copyset id"] = strconv.Itoa(int(v.GetCopysetId()))
			row["peer id"] = strconv.Itoa(int(peer.GetId()))
			row["peer address"] = peer.GetAddress()
			rows = append(rows, row)
		}
	}

	pCmd.Table.AddRows(rows)
	pCmd.Result = res
	pCmd.Error = cmderror.MostImportantCmdError(errs)

	return nil
}

func (pCmd *PartitionCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd, pCmd)
}
