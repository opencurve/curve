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
 * Created Date: 2023-04-28
 * Author: Xinlong-Chen
 */

package copyset

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/delete/peer"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/cli2"
)

const (
	updateExample = `$ curve bs snapshot copyset 127.0.0.0:8200:0 --logicalpoolid=1 --copysetid=1`
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

type SnapshotOneCommand struct {
	basecmd.FinalCurveCmd

	Rpc      *SnapshotRpc
	Response *cli2.SnapshotResponse2
	row      map[string]string
}

var _ basecmd.FinalCurveCmdFunc = (*SnapshotOneCommand)(nil) // check interface

// NewCommand ...
func NewSnapshotOneCommand() *cobra.Command {
	peerCmd := &SnapshotOneCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "copyset",
			Short:   "take snapshot for copyset",
			Example: updateExample,
		},
	}
	basecmd.NewFinalCurveCli(&peerCmd.FinalCurveCmd, peerCmd)
	return peerCmd.Cmd
}

func (sCmd *SnapshotOneCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)

	config.AddBSLogicalPoolIdRequiredFlag(sCmd.Cmd)
	config.AddBSCopysetIdRequiredFlag(sCmd.Cmd)
}

func (sCmd *SnapshotOneCommand) Init(cmd *cobra.Command, args []string) error {
	sCmd.SetHeader([]string{cobrautil.ROW_PEER, cobrautil.ROW_COPYSET, cobrautil.ROW_RESULT})
	sCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		sCmd.Header, []string{},
	))

	timeout := config.GetFlagDuration(sCmd.Cmd, config.RPCTIMEOUT)
	retryTimes := config.GetFlagInt32(sCmd.Cmd, config.RPCRETRYTIMES)

	copysetID := config.GetBsFlagUint32(sCmd.Cmd, config.CURVEBS_COPYSET_ID)

	logicalPoolID := config.GetBsFlagUint32(sCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)

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

	out := make(map[string]string)
	out[cobrautil.ROW_PEER] = fmt.Sprintf("%s:%d", snapshotPeer.GetAddress(), snapshotPeer.GetId())
	out[cobrautil.ROW_COPYSET] = fmt.Sprintf("(%d:%d)", logicalPoolID, copysetID)
	sCmd.row = out

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

func (sCmd *SnapshotOneCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SnapshotOneCommand) RunCommand(cmd *cobra.Command, args []string) error {
	response, err := basecmd.GetRpcResponse(sCmd.Rpc.Info, sCmd.Rpc)
	sCmd.Error = err
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	sCmd.row[cobrautil.ROW_RESULT] = "success"
	sCmd.Response = response.(*cli2.SnapshotResponse2)

	list := cobrautil.Map2List(sCmd.row, sCmd.Header)
	sCmd.TableNew.Append(list)
	return nil
}

func (sCmd *SnapshotOneCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}
