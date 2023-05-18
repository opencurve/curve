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

package leader

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/delete/peer"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/cli2"
)

const (
	leaderExample = `$ curve bs update leader 127.0.0.1:8200:0 --logicalpoolid=1 --copysetid=10001
	--peers=127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0`
)

type LeaderRpc struct {
	Info    *basecmd.Rpc
	Request *cli2.TransferLeaderRequest2
	Client  cli2.CliService2Client
}

func (lRpc *LeaderRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.Client = cli2.NewCliService2Client(cc)
}

func (lRpc *LeaderRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.Client.TransferLeader(ctx, lRpc.Request)
}

type leaderCommand struct {
	basecmd.FinalCurveCmd

	Rpc      *LeaderRpc
	Response *cli2.TransferLeaderResponse2
	row      map[string]string
}

var _ basecmd.FinalCurveCmdFunc = (*leaderCommand)(nil) // check interface

// NewCommand ...
func NewleaderCommand() *cobra.Command {
	peerCmd := &leaderCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "leader",
			Short:   "update(reset) the leader from the copyset",
			Example: leaderExample,
		},
	}
	basecmd.NewFinalCurveCli(&peerCmd.FinalCurveCmd, peerCmd)
	return peerCmd.Cmd
}

func (lCmd *leaderCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(lCmd.Cmd)
	config.AddRpcTimeoutFlag(lCmd.Cmd)

	config.AddBSLogicalPoolIdRequiredFlag(lCmd.Cmd)
	config.AddBSCopysetIdRequiredFlag(lCmd.Cmd)

	config.AddBSPeersConfFlag(lCmd.Cmd)
}

func (lCmd *leaderCommand) Init(cmd *cobra.Command, args []string) error {
	lCmd.SetHeader([]string{curveutil.ROW_LEADER, curveutil.ROW_OLDLEADER, curveutil.ROW_COPYSET, curveutil.ROW_RESULT})
	lCmd.TableNew.SetAutoMergeCellsByColumnIndex(curveutil.GetIndexSlice(
		lCmd.Header, []string{},
	))

	opts := peer.Options{}
	opts.Timeout = config.GetFlagDuration(lCmd.Cmd, config.RPCTIMEOUT)
	opts.RetryTimes = config.GetFlagInt32(lCmd.Cmd, config.RPCRETRYTIMES)

	copysetID := config.GetBsFlagUint32(lCmd.Cmd, config.CURVEBS_COPYSET_ID)
	logicalPoolID := config.GetBsFlagUint32(lCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)

	// parse peers config
	peers := config.GetBsFlagStringSlice(lCmd.Cmd, config.CURVEBS_PEERS_ADDRESS)
	c, err := peer.ParseConfiguration(peers)
	if err != nil {
		return err.ToError()
	}
	conf := *c

	// parse peer conf
	if len(args) < 1 {
		pErr := cmderror.ErrGetPeer()
		pErr.Format("should specified the peer address")
		return pErr.ToError()
	}
	peerPara := args[0]
	leaderPeer, err := peer.ParsePeer(peerPara)
	if err != nil {
		return err.ToError()
	}

	// 1. acquire leader peer info.
	oldLeader, err := peer.GetLeader(logicalPoolID, copysetID, conf, opts)
	if err != nil {
		return err.ToError()
	}

	// 2. judge is same?
	oldLeaderPeer, err := peer.ParsePeer(oldLeader.GetAddress())
	if err != nil {
		return err.ToError()
	}

	if oldLeaderPeer.GetAddress() == leaderPeer.GetAddress() {
		return errors.New("don't need transfer!")
	}

	out := make(map[string]string)
	out[curveutil.ROW_OLDLEADER] = oldLeader.GetAddress()
	out[curveutil.ROW_LEADER] = fmt.Sprintf("%s:%d", leaderPeer.GetAddress(), leaderPeer.GetId())
	out[curveutil.ROW_COPYSET] = fmt.Sprintf("(%d:%d)", logicalPoolID, copysetID)
	lCmd.row = out

	lCmd.Rpc = &LeaderRpc{
		Info: basecmd.NewRpc([]string{oldLeaderPeer.GetAddress()}, opts.Timeout, opts.RetryTimes, "TransferLeader"),
		Request: &cli2.TransferLeaderRequest2{
			LogicPoolId: &logicalPoolID,
			CopysetId:   &copysetID,
			Leader:      oldLeaderPeer,
			Transferee:  leaderPeer,
		},
	}

	return nil
}

func (lCmd *leaderCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&lCmd.FinalCurveCmd, lCmd)
}

func (lCmd *leaderCommand) RunCommand(cmd *cobra.Command, args []string) error {
	// 3. transfer leader
	response, err := basecmd.GetRpcResponse(lCmd.Rpc.Info, lCmd.Rpc)
	lCmd.Error = err
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	lCmd.row[curveutil.ROW_RESULT] = "success"
	lCmd.Response = response.(*cli2.TransferLeaderResponse2)

	list := curveutil.Map2List(lCmd.row, lCmd.Header)
	lCmd.TableNew.Append(list)
	return nil
}

func (lCmd *leaderCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&lCmd.FinalCurveCmd)
}
