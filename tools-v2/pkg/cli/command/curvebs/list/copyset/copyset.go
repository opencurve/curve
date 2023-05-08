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

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetCopySetsInClusterRpc struct {
	Info      *basecmd.Rpc
	Request   *topology.GetCopySetsInClusterRequest
	mdsClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*GetCopySetsInClusterRpc)(nil) // check interface

func (gRpc *GetCopySetsInClusterRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = topology.NewTopologyServiceClient(cc)
}

func (gRpc *GetCopySetsInClusterRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetCopySetsInCluster(ctx, gRpc.Request)
}

type CopysetCommand struct {
	basecmd.FinalCurveCmd
	CopysetInfoList []*common.CopysetInfo
	Rpc             *GetCopySetsInClusterRpc
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

func NewListCopysetCommand() *CopysetCommand {
	copysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
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

	// FilterScanning is true by default
	fiterscanning := true
	cCmd.Rpc = &GetCopySetsInClusterRpc{
		Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetCopySetsInCluster"),
		Request: &topology.GetCopySetsInClusterRequest{
			FilterScaning: &fiterscanning,
		},
	}
	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(cCmd.Rpc.Info, cCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		cCmd.Error = err
		cCmd.Result = result
		return err.ToError()
	}
	res := result.(*topology.GetCopySetsInClusterResponse)
	cCmd.CopysetInfoList = res.GetCopysetInfos()
	cCmd.Result, cCmd.Error = cCmd.CopysetInfoList, cmderror.Success()
	return nil
}

func (cCmd *CopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func GetCopySetsInCluster(caller *cobra.Command) ([]*common.CopysetInfo, *cmderror.CmdError) {
	getCmd := NewListCopysetCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.CURVEBS_MDSADDR, config.RPCRETRYTIMES, config.RPCTIMEOUT,
	})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsListScanStatus()
		retErr.Format(err.Error())
		return getCmd.CopysetInfoList, retErr
	}
	return getCmd.CopysetInfoList, cmderror.Success()
}
