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
* Created Date: 2023-06-02
* Author: montaguelhz
 */
package chunkserver

import (
	"context"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/copyset"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetChunkServerStatusRpc struct {
	Info      *basecmd.Rpc
	Request   *copyset.CopysetsStatusOnChunkserverRequest
	mdsClient copyset.CopysetServiceClient
}

var _ basecmd.RpcFunc = (*GetChunkServerStatusRpc)(nil)

func (gRpc *GetChunkServerStatusRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = copyset.NewCopysetServiceClient(cc)
}

func (gRpc *GetChunkServerStatusRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetCopysetsStatusOnChunkserver(ctx, gRpc.Request)
}

type ChunkServerStatusCommand struct {
	basecmd.FinalCurveCmd
	Rpc        []*GetChunkServerStatusRpc
	key2Status *map[uint64]*copyset.CopysetStatusResponse
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkServerStatusCommand)(nil)

func (csiCmd *ChunkServerStatusCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(csiCmd.Cmd)
	config.AddRpcTimeoutFlag(csiCmd.Cmd)

	config.AddBsChunkServerAddrOptionFlag(csiCmd.Cmd)
}

func NewGetChunkServerStatusCommand() *ChunkServerStatusCommand {
	cssCmd := &ChunkServerStatusCommand{FinalCurveCmd: basecmd.FinalCurveCmd{}}
	basecmd.NewFinalCurveCli(&cssCmd.FinalCurveCmd, cssCmd)
	return cssCmd
}

func NewChunkServerStatusCommand() *cobra.Command {
	return NewGetChunkServerStatusCommand().Cmd
}

func (cssCmd *ChunkServerStatusCommand) Init(cmd *cobra.Command, args []string) error {
	timeout := config.GetFlagDuration(cssCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cssCmd.Cmd, config.RPCRETRYTIMES)

	addrs, err := config.GetBsChunkserverAddrSlice(cssCmd.Cmd)
	if err != cmderror.ErrSuccess() {
		return err.ToError()
	}
	for _, addr := range addrs {
		rpc := &GetChunkServerStatusRpc{
			Request: &copyset.CopysetsStatusOnChunkserverRequest{},
			Info:    basecmd.NewRpc([]string{addr}, timeout, retrytimes, "GetCopysetsStatusOnChunkserver"),
		}
		cssCmd.Rpc = append(cssCmd.Rpc, rpc)
	}
	return nil
}

func (cssCmd *ChunkServerStatusCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var infos []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range cssCmd.Rpc {
		infos = append(infos, rpc.Info)
		funcs = append(funcs, rpc)
	}
	results, errs := basecmd.GetRpcListResponse(infos, funcs)
	if len(errs) == len(infos) {
		mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
		return mergeErr.ToError()
	}
	key2Status := make(map[uint64]*copyset.CopysetStatusResponse)
	cssCmd.key2Status = &key2Status
	for _, result := range results {
		response := result.(*copyset.CopysetsStatusOnChunkserverResponse)
		copysets := response.GetCopysets()
		copysetResponses := response.GetCopysetResponses()
		for i := range copysets {
			logicpoolid := copysets[i].GetLogicPoolId()
			copysetid := copysets[i].GetCopysetId()
			key := cobrautil.GetCopysetKey(uint64(logicpoolid), uint64(copysetid))
			(*cssCmd.key2Status)[key] = copysetResponses[i]
		}
	}
	return nil
}

func (cssCmd *ChunkServerStatusCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cssCmd.FinalCurveCmd, cssCmd)
}

func (cssCmd *ChunkServerStatusCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cssCmd.FinalCurveCmd)
}

func GetChunkServerStatus(caller *cobra.Command) (*map[uint64]*copyset.CopysetStatusResponse, *cmderror.CmdError) {
	cssCmd := NewGetChunkServerStatusCommand()
	config.AlignFlagsValue(caller, cssCmd.Cmd, []string{config.CURVEBS_CHUNKSERVER_ADDR})
	cssCmd.Cmd.SilenceErrors = true
	cssCmd.Cmd.SilenceUsage = true
	cssCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := cssCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetChunkServerStatus()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return cssCmd.key2Status, cmderror.Success()
}
