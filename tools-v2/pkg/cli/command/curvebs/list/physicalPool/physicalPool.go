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

package physicalpool

import (
	"context"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type ListPhysicalPoolRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPhysicalPoolRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListPhysicalPoolRpc)(nil) // check interface

type PhysicalPoolCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *ListPhysicalPoolRpc
	response *topology.ListPhysicalPoolResponse
}

var _ basecmd.FinalCurveCmdFunc = (*PhysicalPoolCommand)(nil) // check interface

func (lRpc *ListPhysicalPoolRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lRpc *ListPhysicalPoolRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.topologyClient.ListPhysicalPool(ctx, lRpc.Request)
}

func NewPhysicalPoolCommand() *cobra.Command {
	return NewListPhysicalPoolCommand().Cmd
}

func NewListPhysicalPoolCommand() *PhysicalPoolCommand {
	ppCmd := &PhysicalPoolCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&ppCmd.FinalCurveCmd, ppCmd)
	return ppCmd
}

func (pCmd *PhysicalPoolCommand) AddFlags() {
	config.AddBsMdsFlagOption(pCmd.Cmd)
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
}

func (pCmd *PhysicalPoolCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)
	pCmd.Rpc = &ListPhysicalPoolRpc{
		Request: &topology.ListPhysicalPoolRequest{},
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListPhysicalPool"),
	}
	return nil
}

func (pCmd *PhysicalPoolCommand) RunCommand(cmd *cobra.Command, args []string) error {
	response, errCmd := basecmd.GetRpcResponse(pCmd.Rpc.Info, pCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return errCmd.ToError()
	}
	pCmd.response = response.(*topology.ListPhysicalPoolResponse)
	if pCmd.response.StatusCode == nil ||
		*pCmd.response.StatusCode != int32(statuscode.TopoStatusCode_Success) {
		code := statuscode.TopoStatusCode(*pCmd.response.StatusCode)
		return cmderror.ErrBsListPhysicalPoolRpc(code).ToError()
	}
	return nil
}

func GetPhysicalPool(caller *cobra.Command) ([]*topology.PhysicalPoolInfo, *cmderror.CmdError) {
	listPhysicalPool := NewListPhysicalPoolCommand()
	listPhysicalPool.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	listPhysicalPool.Cmd.SilenceErrors = true
	config.AlignFlagsValue(caller, listPhysicalPool.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
	})
	err := listPhysicalPool.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetPhysicalPool()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return listPhysicalPool.response.PhysicalPoolInfos, cmderror.ErrSuccess()
}

func (pCmd *PhysicalPoolCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *PhysicalPoolCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}
