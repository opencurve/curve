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
 * Project: tools-v2
 * Created Date: 2023-04-24
 * Author: baytan0720
 */

package copyset

import (
	"context"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/copyset"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetCopysetStatusRpc struct {
	Info      *basecmd.Rpc
	Request   *copyset.CopysetStatusRequest
	mdsClient copyset.CopysetServiceClient
}

var _ basecmd.RpcFunc = (*GetCopysetStatusRpc)(nil) // check interface

func (gRpc *GetCopysetStatusRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = copyset.NewCopysetServiceClient(cc)
}

func (gRpc *GetCopysetStatusRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetCopysetStatus(ctx, gRpc.Request)
}

type GetCopysetStatusCommand struct {
	basecmd.FinalCurveCmd
	Rpc         []*GetCopysetStatusRpc
	peer2Status map[string]*copyset.CopysetStatusResponse
}

var _ basecmd.FinalCurveCmdFunc = (*GetCopysetStatusCommand)(nil) // check interface

func NewCopysetStatusCommand() *cobra.Command {
	return NewGetCopysetStatusCommand().Cmd
}

func NewGetCopysetStatusCommand() *GetCopysetStatusCommand {
	copysetCmd := &GetCopysetStatusCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func (cCmd *GetCopysetStatusCommand) Init(cmd *cobra.Command, args []string) error {
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)

	copysetid := config.GetBsFlagUint32(cCmd.Cmd, config.CURVEBS_COPYSET_ID)
	logicalpoolid := config.GetBsFlagUint32(cCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	peersAddress := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_PEERS_ADDRESS)
	queryHash := false
	for _, address := range peersAddress {
		rpc := &GetCopysetStatusRpc{
			Request: &copyset.CopysetStatusRequest{
				CopysetId:   &copysetid,
				LogicPoolId: &logicalpoolid,
				Peer:        &common.Peer{Address: &address},
				QueryHash:   &queryHash,
			},
		}
		rpc.Info = basecmd.NewRpc([]string{address}, timeout, retrytimes, "GetCopysetStatus")
		cCmd.Rpc = append(cCmd.Rpc, rpc)
	}
	return nil
}

func (cCmd *GetCopysetStatusCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var infos []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range cCmd.Rpc {
		infos = append(infos, rpc.Info)
		funcs = append(funcs, rpc)
	}
	results, errs := basecmd.GetRpcListResponse(infos, funcs)
	if len(errs) == len(infos) {
		mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
		return mergeErr.ToError()
	}
	addr2Status := make(map[string]*copyset.CopysetStatusResponse)
	cCmd.peer2Status = addr2Status
	for i, result := range results {
		if respone, ok := result.(*copyset.CopysetStatusResponse); ok {
			cCmd.peer2Status[infos[i].Addrs[0]] = respone
		} else {
			cCmd.peer2Status[infos[i].Addrs[0]] = nil
		}
	}
	return nil
}

func (cCmd *GetCopysetStatusCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *GetCopysetStatusCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *GetCopysetStatusCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)

	config.AddBsCopysetIdRequiredFlag(cCmd.Cmd)
	config.AddBsLogicalPoolIdRequiredFlag(cCmd.Cmd)
	config.AddBsPeersConfFlag(cCmd.Cmd)
}

func GetCopysetStatus(caller *cobra.Command) (map[string]*copyset.CopysetStatusResponse, *cmderror.CmdError) {
	sCmd := NewGetCopysetStatusCommand()
	config.AlignFlagsValue(caller, sCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_LOGIC_POOL_ID, config.CURVEBS_COPYSET_ID, config.CURVEBS_PEERS_ADDRESS,
	})
	sCmd.Cmd.SilenceErrors = true
	sCmd.Cmd.SilenceUsage = true
	sCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := sCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetCopysetStatus()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return sCmd.peer2Status, cmderror.Success()
}
