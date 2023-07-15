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
 * Created Date: 2023-07-11
 * Author: montaguelhz
 */

package copyset

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/cli2"
)

type SnapshotAllRpc struct {
	Info    *basecmd.Rpc
	Request *cli2.SnapshotAllRequest
	Client  cli2.CliService2Client
}

func (sRpc *SnapshotAllRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	sRpc.Client = cli2.NewCliService2Client(cc)
}

func (sRpc *SnapshotAllRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return sRpc.Client.SnapshotAll(ctx, sRpc.Request)
}

type SnapshotAllCopysetCommand struct {
	basecmd.FinalCurveCmd

	rpcs    []*SnapshotAllRpc
	csAddrs []string
	res     []bool
}

var _ basecmd.FinalCurveCmdFunc = (*SnapshotAllCopysetCommand)(nil) // check interface

func NewSnapshotAllCopysetCommand() *SnapshotAllCopysetCommand {
	sCmd := &SnapshotAllCopysetCommand{FinalCurveCmd: basecmd.FinalCurveCmd{}}
	basecmd.NewFinalCurveCli(&sCmd.FinalCurveCmd, sCmd)
	return sCmd
}

func NewAllCopysetCommand() *cobra.Command {
	return NewSnapshotAllCopysetCommand().Cmd
}
func (sCmd *SnapshotAllCopysetCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)
}

func (sCmd *SnapshotAllCopysetCommand) Init(cmd *cobra.Command, args []string) error {
	timeout := config.GetFlagDuration(sCmd.Cmd, config.RPCTIMEOUT)
	retryTimes := config.GetFlagInt32(sCmd.Cmd, config.RPCRETRYTIMES)

	chunkserverInfos, err := chunkserver.GetChunkServerInCluster(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	sCmd.csAddrs = make([]string, len(chunkserverInfos))
	sCmd.res = make([]bool, len(chunkserverInfos))

	for i, info := range chunkserverInfos {
		sCmd.csAddrs[i] = fmt.Sprintf("%s:%d", info.GetHostIp(), info.GetPort())
	}

	for _, addr := range sCmd.csAddrs {
		rpc := &SnapshotAllRpc{
			Request: &cli2.SnapshotAllRequest{},
			Info:    basecmd.NewRpc([]string{addr}, timeout, retryTimes, "SnapshotAll"),
		}
		sCmd.rpcs = append(sCmd.rpcs, rpc)
	}

	return nil
}

func (sCmd *SnapshotAllCopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var infos []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range sCmd.rpcs {
		infos = append(infos, rpc.Info)
		funcs = append(funcs, rpc)
	}

	results, errs := basecmd.GetRpcListResponse(infos, funcs)
	mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	sCmd.Error = mergeErr
	if len(errs) == len(infos) {
		return mergeErr.ToError()
	}

	for i, result := range results {
		sCmd.res[i] = (result != nil)
	}

	return nil
}

func (sCmd *SnapshotAllCopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SnapshotAllCopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func GetAllSnapshotResult(caller *cobra.Command) ([]string, []bool, *cmderror.CmdError) {
	sCmd := NewSnapshotAllCopysetCommand()
	config.AlignFlagsValue(caller, sCmd.Cmd, []string{})
	sCmd.Cmd.SilenceErrors = true
	sCmd.Cmd.SilenceUsage = true
	sCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := sCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetAllSnapshotResult()
		retErr.Format(err.Error())
		return nil, nil, retErr
	}
	return sCmd.csAddrs, sCmd.res, cmderror.Success()
}
