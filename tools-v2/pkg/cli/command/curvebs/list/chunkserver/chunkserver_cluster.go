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
* Project: CurveCli
* Created Date: 2023-05-11
* Author: chengyi01
 */
package chunkserver

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

type GetChunkServerInClusterRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.GetChunkServerInClusterRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*GetChunkServerInClusterRpc)(nil) // check interface

type ChunkServerInClusterCommand struct {
	basecmd.FinalCurveCmd
	Rpc              *GetChunkServerInClusterRpc
	ChunkServerInfos []*topology.ChunkServerInfo
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkServerInClusterCommand)(nil) // check interface

func (gRpc *GetChunkServerInClusterRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (gRpc *GetChunkServerInClusterRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.topologyClient.GetChunkServerInCluster(ctx, gRpc.Request)
}

func NewChunkServerInClusterCommand() *cobra.Command {
	return NewListChunkServerInClusterCommand().Cmd
}

func NewListChunkServerInClusterCommand() *ChunkServerInClusterCommand {
	lsCmd := &ChunkServerInClusterCommand{FinalCurveCmd: basecmd.FinalCurveCmd{}}
	basecmd.NewFinalCurveCli(&lsCmd.FinalCurveCmd, lsCmd)
	return lsCmd
}

func (csicCmd *ChunkServerInClusterCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(csicCmd.Cmd)
	config.AddRpcTimeoutFlag(csicCmd.Cmd)

	config.AddBsMdsFlagOption(csicCmd.Cmd)
}

func (csicCmd *ChunkServerInClusterCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(csicCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	timeout := config.GetFlagDuration(csicCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(csicCmd.Cmd, config.RPCRETRYTIMES)

	csicCmd.Rpc = &GetChunkServerInClusterRpc{
		Request: &topology.GetChunkServerInClusterRequest{},
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetChunkServerInCluster"),
	}
	return nil
}

func (csicCmd *ChunkServerInClusterCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&csicCmd.FinalCurveCmd, csicCmd)
}

func (csicCmd *ChunkServerInClusterCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(csicCmd.Rpc.Info, csicCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	res := result.(*topology.GetChunkServerInClusterResponse)
	if res.GetStatusCode() != int32(statuscode.TopoStatusCode_Success) {
		err = cmderror.ErrBsGetChunkServerInClusterRpc(statuscode.TopoStatusCode(res.GetStatusCode()))
		return err.ToError()
	}
	csicCmd.ChunkServerInfos = res.GetChunkServerInfos()

	return nil
}

func (csicCmd *ChunkServerInClusterCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&csicCmd.FinalCurveCmd)
}

func GetChunkServerInCluster(caller *cobra.Command) ([]*topology.ChunkServerInfo, *cmderror.CmdError) {
	getCmd := NewListChunkServerInClusterCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{config.CURVEBS_MDSADDR})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetChunkServerInCluster()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return getCmd.ChunkServerInfos, cmderror.Success()
}
