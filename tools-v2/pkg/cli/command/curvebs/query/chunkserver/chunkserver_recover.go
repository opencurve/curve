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
* Created Date: 2023-05-15
* Author: chengyi01
 */
package chunkserver

import (
	"context"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/schedule"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetChunkServerRecoverStatusRpc struct {
	Info      *basecmd.Rpc
	Request   *schedule.QueryChunkServerRecoverStatusRequest
	mdsClient schedule.ScheduleServiceClient
}

var _ basecmd.RpcFunc = (*GetChunkServerRecoverStatusRpc)(nil)

func (gRpc *GetChunkServerRecoverStatusRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = schedule.NewScheduleServiceClient(cc)
}

func (gRpc *GetChunkServerRecoverStatusRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.QueryChunkServerRecoverStatus(ctx, gRpc.Request)
}

type ChunkServerRecoverStatusCommand struct {
	basecmd.FinalCurveCmd
	Rpc              *GetChunkServerRecoverStatusRpc
	RecoverStatusMap map[uint32]bool
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkServerRecoverStatusCommand)(nil)

func (csrsCmd *ChunkServerRecoverStatusCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(csrsCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(csrsCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(csrsCmd.Cmd, config.RPCRETRYTIMES)

	request := schedule.QueryChunkServerRecoverStatusRequest{}
	chunkseverIdSlice := config.GetBsChunkServerId(csrsCmd.Cmd)
	request.ChunkServerID = append(request.ChunkServerID, chunkseverIdSlice...)

	csrsCmd.Rpc = &GetChunkServerRecoverStatusRpc{
		Request: &request,
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "QueryChunkServerRecoverStatus"),
	}

	return nil
}

func (csrsCmd *ChunkServerRecoverStatusCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(csrsCmd.Rpc.Info, csrsCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	res := result.(*schedule.QueryChunkServerRecoverStatusResponse)
	RetErr := cmderror.ErrBsQueryChunkserverRecoverStatus(statuscode.TopoStatusCode(res.GetStatusCode()))
	csrsCmd.RecoverStatusMap = res.GetRecoverStatusMap()
	csrsCmd.Result = res
	csrsCmd.Error = RetErr
	return nil
}

func (csrsCmd *ChunkServerRecoverStatusCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&csrsCmd.FinalCurveCmd, csrsCmd)
}

func (csrsCmd *ChunkServerRecoverStatusCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&csrsCmd.FinalCurveCmd)
}

func (csrsCmd *ChunkServerRecoverStatusCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(csrsCmd.Cmd)
	config.AddRpcTimeoutFlag(csrsCmd.Cmd)

	config.AddBsMdsFlagOption(csrsCmd.Cmd)
	config.AddBsChunkServerIdOptionFlag(csrsCmd.Cmd)
}

func NewQueryChunkServerRecoverStatusCommand() *ChunkServerRecoverStatusCommand {
	csrsCmd := &ChunkServerRecoverStatusCommand{FinalCurveCmd: basecmd.FinalCurveCmd{}}
	basecmd.NewFinalCurveCli(&csrsCmd.FinalCurveCmd, csrsCmd)
	return csrsCmd
}

func NewChunkServerRecoverStatusCommand() *cobra.Command {
	return NewQueryChunkServerRecoverStatusCommand().Cmd
}

func GetQueryChunkServerRecoverStatus(caller *cobra.Command) (map[uint32]bool, *cmderror.CmdError) {
	getCmd := NewQueryChunkServerRecoverStatusCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{config.CURVEBS_MDSADDR, config.CUERVEBS_CHUNKSERVER_ID})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsQueryChunkServerRecoverStatus()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return getCmd.RecoverStatusMap, cmderror.Success()
}
