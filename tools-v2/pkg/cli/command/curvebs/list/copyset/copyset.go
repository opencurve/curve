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
/*
* Project: curve
* Created Date: 2023-05-04
* Author: lianzhanbiao
 */

package copyset

import (
	"context"
	"fmt"
	"log"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	copysetExample = `$ curve bs list copyset`
)

type ListCopysetRpc struct {
	Info      *basecmd.Rpc
	Request   *topology.GetCopySetsInClusterRequest
	mdsClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListCopysetRpc)(nil) // check interface

func (gRpc *ListCopysetRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = topology.NewTopologyServiceClient(cc)
}

func (gRpc *ListCopysetRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetCopySetsInCluster(ctx, gRpc.Request)
}

type CopysetCommand struct {
	basecmd.FinalCurveCmd
	Response *topology.GetCopySetsInClusterResponse
	Rpc      *ListCopysetRpc
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

func NewListCopysetCommand() *CopysetCommand {
	copysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "copyset",
			Short:   "list curvebs all copyset",
			Example: copysetExample,
		},
	}

	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func NewCopysetCommand() *cobra.Command {
	return NewListCopysetCommand().Cmd
}

func (cCmd *CopysetCommand) AddFlags() {
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddBsMdsFlagOption(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)

	// FilterScanning 赋值 true
	fiterscanning := true
	cCmd.Rpc = &ListCopysetRpc{
		Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListCopyset"),
		Request: &topology.GetCopySetsInClusterRequest{
			FilterScaning: &fiterscanning,
		},
	}

	header := []string{
		cobrautil.ROW_LOGICALPOOL,
		cobrautil.ROW_COPYSET_ID,
	}
	cCmd.SetHeader(header)
	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	log.Printf("rpc request = %v", cCmd.Rpc.Request)
	result, err := basecmd.GetRpcResponse(cCmd.Rpc.Info, cCmd.Rpc)
	log.Printf("result = %v", result)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		cCmd.Error = err
		cCmd.Result = result
		return err.ToError()
	}
	cCmd.Response = result.(*topology.GetCopySetsInClusterResponse)
	log.Printf("response = %v", cCmd.Response)
	infos := cCmd.Response.GetCopysetInfos()
	rows := make([]map[string]string, 0)
	var errs []*cmderror.CmdError
	for _, info := range infos {
		row := make(map[string]string)
		row[cobrautil.ROW_LOGICALPOOL] = fmt.Sprintf("%d", info.GetLogicalPoolId())
		row[cobrautil.ROW_COPYSET_ID] = fmt.Sprintf("%d", info.GetCopysetId())
		rows = append(rows, row)
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, cCmd.Header, []string{
		cobrautil.ROW_LOGICALPOOL,
		cobrautil.ROW_COPYSET_ID,
	})
	cCmd.TableNew.AppendBulk(list)
	if len(errs) != 0 {
		mergeErr := cmderror.MergeCmdError(errs)
		cCmd.Result, cCmd.Error = result, mergeErr
		return mergeErr.ToError()
	}
	cCmd.Result, cCmd.Error = result, cmderror.Success()
	return nil
}

func (cCmd *CopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}
