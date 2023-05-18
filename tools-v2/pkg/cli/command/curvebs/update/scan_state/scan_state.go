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
* Project: curvecli
* Created Date: 2023-05-08
* Author: montaguelhz
 */

package scan_state

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type UpdateScanStateRpc struct {
	Info    *basecmd.Rpc
	Request *topology.SetLogicalPoolScanStateRequest
	Client  topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*UpdateScanStateRpc)(nil) // check interface

func (sRpc *UpdateScanStateRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	sRpc.Client = topology.NewTopologyServiceClient(cc)
}

func (sRpc *UpdateScanStateRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return sRpc.Client.SetLogicalPoolScanState(ctx, sRpc.Request)
}

type ScanStateCommand struct {
	basecmd.FinalCurveCmd

	Rpc           *UpdateScanStateRpc
	Response      *topology.SetLogicalPoolScanStateResponse
	LogicalPoolID uint32
	Scan          bool
}

var _ basecmd.FinalCurveCmdFunc = (*ScanStateCommand)(nil) // check interface

const (
	scanStateExample = `$ curve bs update scan-state --logicalpoolid 1 [--scan=true/false]`
)

func NewScanStateCommand() *cobra.Command {
	return NewUpdateScanStateCommand().Cmd
}

func (sCmd *ScanStateCommand) AddFlags() {
	config.AddBsMdsFlagOption(sCmd.Cmd)
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)

	config.AddBSLogicalPoolIdRequiredFlag(sCmd.Cmd)
	config.AddBsScanOptionFlag(sCmd.Cmd)
}

func NewUpdateScanStateCommand() *ScanStateCommand {
	sCmd := &ScanStateCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "scan-state",
			Short:   "enable/disable scan for logical pool",
			Example: scanStateExample,
		},
	}

	basecmd.NewFinalCurveCli(&sCmd.FinalCurveCmd, sCmd)
	return sCmd
}

func (sCmd *ScanStateCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	header := []string{curveutil.ROW_ID, curveutil.ROW_SCAN, curveutil.ROW_RESULT, curveutil.ROW_REASON}
	sCmd.SetHeader(header)

	Timeout := config.GetFlagDuration(sCmd.Cmd, config.RPCTIMEOUT)
	RetryTimes := config.GetFlagInt32(sCmd.Cmd, config.RPCRETRYTIMES)
	sCmd.LogicalPoolID = config.GetBsFlagUint32(sCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	sCmd.Scan = config.GetBsFlagBool(sCmd.Cmd, config.CURVEBS_SCAN)
	sCmd.Rpc = &UpdateScanStateRpc{
		Info: basecmd.NewRpc(mdsAddrs, Timeout, RetryTimes, "UpdateScanState"),
		Request: &topology.SetLogicalPoolScanStateRequest{
			LogicalPoolID: &sCmd.LogicalPoolID,
			ScanEnable:    &sCmd.Scan,
		},
	}
	return nil
}

func (sCmd *ScanStateCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *ScanStateCommand) RunCommand(cmd *cobra.Command, args []string) error {
	out := make(map[string]string)
	out[curveutil.ROW_ID] = fmt.Sprintf("%d", sCmd.LogicalPoolID)
	out[curveutil.ROW_SCAN] = fmt.Sprintf("%t", sCmd.Scan)
	result, err := basecmd.GetRpcResponse(sCmd.Rpc.Info, sCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	sCmd.Response = result.(*topology.SetLogicalPoolScanStateResponse)
	if *sCmd.Response.StatusCode != int32(statuscode.TopoStatusCode_Success) {
		out[curveutil.ROW_RESULT] = curveutil.ROW_VALUE_FAILED
		out[curveutil.ROW_REASON] = statuscode.TopoStatusCode_name[*sCmd.Response.StatusCode]
	} else {
		out[curveutil.ROW_RESULT] = curveutil.ROW_VALUE_SUCCESS
		out[curveutil.ROW_REASON] = curveutil.ROW_VALUE_NULL
	}

	res := curveutil.Map2List(out, sCmd.Header)
	sCmd.TableNew.Append(res)

	sCmd.Result = out
	sCmd.Error = cmderror.ErrSuccess()
	return nil
}

func (sCmd *ScanStateCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}
