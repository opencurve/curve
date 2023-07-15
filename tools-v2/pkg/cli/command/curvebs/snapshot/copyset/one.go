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
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/delete/peer"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/cli2"
)

type SnapshotRpc struct {
	Info    *basecmd.Rpc
	Request *cli2.SnapshotRequest2
	Client  cli2.CliService2Client
}

func (sRpc *SnapshotRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	sRpc.Client = cli2.NewCliService2Client(cc)
}

func (sRpc *SnapshotRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return sRpc.Client.Snapshot(ctx, sRpc.Request)
}

type SnapshotOneCopysetCommand struct {
	basecmd.FinalCurveCmd

	Rpc     *SnapshotRpc
	peer    string
	copyset string
}

var _ basecmd.FinalCurveCmdFunc = (*SnapshotOneCopysetCommand)(nil) // check interface

// NewCommand ...
func NewSnapshotOneCopysetCommand() *SnapshotOneCopysetCommand {
	sCmd := &SnapshotOneCopysetCommand{FinalCurveCmd: basecmd.FinalCurveCmd{}}
	basecmd.NewFinalCurveCli(&sCmd.FinalCurveCmd, sCmd)
	return sCmd
}

func NewOneCopysetCommand() *cobra.Command {
	return NewSnapshotOneCopysetCommand().Cmd
}

func (sCmd *SnapshotOneCopysetCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)

	config.AddBsLogicalPoolIdRequiredFlag(sCmd.Cmd)
	config.AddBsCopysetIdRequiredFlag(sCmd.Cmd)
}

func (sCmd *SnapshotOneCopysetCommand) Init(cmd *cobra.Command, args []string) error {
	timeout := config.GetFlagDuration(sCmd.Cmd, config.RPCTIMEOUT)
	retryTimes := config.GetFlagInt32(sCmd.Cmd, config.RPCRETRYTIMES)

	// parse peer conf
	if len(args) < 1 {
		pErr := cmderror.ErrGetPeer()
		pErr.Format("should specified the peer address")
		return pErr.ToError()
	}
	snapshotPeer, err := peer.ParsePeer(args[0])
	if err != nil {
		return err.ToError()
	}

	copysetID := config.GetBsFlagUint32(sCmd.Cmd, config.CURVEBS_COPYSET_ID)
	logicalPoolID := config.GetBsFlagUint32(sCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	sCmd.peer = fmt.Sprintf("%s:%d", snapshotPeer.GetAddress(), snapshotPeer.GetId())
	sCmd.copyset = fmt.Sprintf("(%d:%d)", logicalPoolID, copysetID)

	sCmd.Rpc = &SnapshotRpc{
		Info: basecmd.NewRpc([]string{snapshotPeer.GetAddress()}, timeout, retryTimes, "Snapshot"),
		Request: &cli2.SnapshotRequest2{
			LogicPoolId: &logicalPoolID,
			CopysetId:   &copysetID,
			Peer:        snapshotPeer,
		},
	}

	return nil
}

func (sCmd *SnapshotOneCopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	_, err := basecmd.GetRpcResponse(sCmd.Rpc.Info, sCmd.Rpc)
	sCmd.Error = err
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	return nil
}

func (sCmd *SnapshotOneCopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SnapshotOneCopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func GetOneSnapshotResult(caller *cobra.Command, args []string) (string, string, *cmderror.CmdError) {
	sCmd := NewSnapshotOneCopysetCommand()
	config.AlignFlagsValue(caller, sCmd.Cmd, []string{config.CURVEBS_COPYSET_ID, config.CURVEBS_LOGIC_POOL_ID})
	sCmd.Cmd.SilenceErrors = true
	sCmd.Cmd.SilenceUsage = true
	args = append(args, []string{"--format", config.FORMAT_NOOUT}...)
	sCmd.Cmd.SetArgs(args)
	err := sCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetOneSnapshotResult()
		retErr.Format(err.Error())
		return "", "", retErr
	}
	return sCmd.peer, sCmd.copyset, cmderror.Success()
}
