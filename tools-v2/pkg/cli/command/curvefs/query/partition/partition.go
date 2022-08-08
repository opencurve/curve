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

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	partitionExample = `$ curve fs query partition --partitionid 1,2,3`
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
			Use:     "partition",
			Short:   "query the copyset of partition",
			Example: partitionExample,
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

	header := []string{
		cobrautil.ROW_ID, cobrautil.ROW_POOL_ID, cobrautil.ROW_COPYSET_ID, cobrautil.ROW_PEER_ID, cobrautil.ROW_PEER_ADDR,
	}
	pCmd.SetHeader(header)
	pCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		pCmd.Header, []string{
			cobrautil.ROW_POOL_ID,cobrautil.ROW_COPYSET_ID, cobrautil.ROW_ID,
	}))

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
		return fmt.Errorf(err.Message)
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
			row[cobrautil.ROW_ID] = strconv.Itoa(int(k))
			row[cobrautil.ROW_POOL_ID] = strconv.Itoa(int(v.GetPoolId()))
			row[cobrautil.ROW_COPYSET_ID] = strconv.Itoa(int(v.GetCopysetId()))
			row[cobrautil.ROW_PEER_ID] = strconv.Itoa(int(peer.GetId()))
			row[cobrautil.ROW_PEER_ADDR] = peer.GetAddress()
			rows = append(rows, row)
		}
	}

	list := cobrautil.ListMap2ListSortByKeys(rows, pCmd.Header, []string{
		cobrautil.ROW_POOL_ID, cobrautil.ROW_COPYSET_ID, cobrautil.ROW_ID,
	})
	pCmd.TableNew.AppendBulk(list)
	pCmd.Result = res
	pCmd.Error = cmderror.MostImportantCmdError(errs)

	return nil
}

func (pCmd *PartitionCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}
