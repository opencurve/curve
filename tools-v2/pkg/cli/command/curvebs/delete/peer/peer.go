/*  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-11-30
 * Author: zls1129@gmail.com
 */

package peer

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/cli"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
)

const (
	deleteExample = `$ curve bs delete peer 127.0.0.1:8200:0 --logicalpoolid=1 --copysetid=10001
 --peers=127.0.0.1:8200:0,127.0.0.1:8201:1,127.0.0.1:8202:2 --rpcretrytimes=1 --rpctimeout=10s`
)

// RPCClient the rpc client for the rpc function RemovePeer
type RPCClient struct {
	Info    *basecmd.Rpc
	Request *cli.RemovePeerRequest
	cli     cli.CliServiceClient
}

func (rpp *RPCClient) NewRpcClient(cc grpc.ClientConnInterface) {
	rpp.cli = cli.NewCliServiceClient(cc)
}

func (rpp *RPCClient) Stub_Func(ctx context.Context) (interface{}, error) {
	return rpp.cli.RemovePeer(ctx, rpp.Request)
}

var _ basecmd.RpcFunc = (*RPCClient)(nil) // check interface

// Command the command to perform remove peer.
type Command struct {
	basecmd.FinalCurveCmd

	// request parameters
	opts          Options
	logicalPoolID uint32
	copysetID     uint32

	conf       Configuration
	removePeer *common.Peer
}

var _ basecmd.FinalCurveCmdFunc = (*Command)(nil) // check interface
// NewCommand ...
func NewCommand() *cobra.Command {
	cCmd := &Command{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "peer",
			Short:   "delete the peer from the copyset",
			Example: deleteExample,
		},
	}
	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd.Cmd
}

func (cCmd *Command) AddFlags() {
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)

	config.AddBSLogicalPoolIdFlag(cCmd.Cmd)
	config.AddBSCopysetIdFlag(cCmd.Cmd)

	config.AddBSPeersConfFlag(cCmd.Cmd)
}

func (cCmd *Command) Init(cmd *cobra.Command, args []string) error {
	cCmd.opts = Options{}

	cCmd.SetHeader([]string{cobrautil.ROW_LEADER, cobrautil.ROW_PEER, cobrautil.ROW_COPYSET, cobrautil.ROW_RESULT, cobrautil.ROW_REASON})
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		cCmd.Header, []string{},
	))

	var err error
	cCmd.opts.Timeout = config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	cCmd.opts.RetryTimes = config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)

	cCmd.copysetID, err = config.GetBsFlagUint32(cCmd.Cmd, config.CURVEBS_COPYSET_ID)
	if err != nil {
		return err
	}
	cCmd.logicalPoolID, err = config.GetBsFlagUint32(cCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	if err != nil {
		return err
	}

	// parse peers config
	peers := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_PEERS_ADDRESS)
	c, e := ParseConfiguration(peers)
	if e != nil {
		return e.ToError()
	}
	fmt.Println(c)
	cCmd.conf = *c

	// parse peer conf
	if len(args) < 1 {
		pErr := cmderror.ErrGetPeer()
		pErr.Format("should specified the peer address")
		return pErr.ToError()
	}
	peer := args[0]
	cCmd.removePeer, e = ParsePeer(peer)
	if e != nil {
		return e.ToError()
	}
	return nil
}

func (cCmd *Command) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *Command) RunCommand(cmd *cobra.Command, args []string) error {
	out := make(map[string]string)
	out[cobrautil.ROW_PEER] = fmt.Sprintf("%s:%d", cCmd.removePeer.GetAddress(), cCmd.removePeer.GetId())
	out[cobrautil.ROW_COPYSET] = fmt.Sprintf("(%d:%d)", cCmd.logicalPoolID, cCmd.copysetID)

	// 1. acquire leader peer info.
	leader, err := GetLeader(cCmd.logicalPoolID, cCmd.copysetID, cCmd.conf, cCmd.opts)
	if err != nil {
		out[cobrautil.ROW_RESULT] = "failed"
		out[cobrautil.ROW_REASON] = err.ToError().Error()
		cCmd.Error = err
		return nil
	}

	out[cobrautil.ROW_LEADER] = fmt.Sprintf("%s", leader.GetAddress())
	// 2. remove peer
	cCmd.Result, err = cCmd.execRemovePeer(leader)
	if err != nil {
		out[cobrautil.ROW_RESULT] = "failed"
		out[cobrautil.ROW_REASON] = err.ToError().Error()
		cCmd.Error = err
		return nil
	}
	out[cobrautil.ROW_RESULT] = "success"
	out[cobrautil.ROW_REASON] = "null"
	list := cobrautil.Map2List(out, cCmd.Header)
	cCmd.TableNew.Append(list)
	return nil
}

func (cCmd *Command) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *Command) execRemovePeer(leader *common.Peer) (interface{}, *cmderror.CmdError) {
	peer, err := ParsePeer(leader.GetAddress())
	if err != nil {
		return nil, err
	}
	leaderID := leader.GetAddress()
	peerID := fmt.Sprintf("%s:%v", cCmd.removePeer.GetAddress(), cCmd.removePeer.GetId())
	rpcCli := &RPCClient{
		Info: basecmd.NewRpc([]string{peer.GetAddress()}, cCmd.opts.Timeout, cCmd.opts.RetryTimes, "RemovePeer"),
		Request: &cli.RemovePeerRequest{
			LogicPoolId: &cCmd.logicalPoolID,
			CopysetId:   &cCmd.copysetID,
			LeaderId:    &leaderID,
			PeerId:      &peerID,
		},
	}

	response, errCmd := basecmd.GetRpcResponse(rpcCli.Info, rpcCli)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return response, errCmd
	}
	return response, nil
}
