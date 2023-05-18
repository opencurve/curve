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
 * Created Date: 2023-02-27
 * Author: zweix
 */

package peer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/cli2"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
)

const (
	peerExample = `$ curve bs update peer 127.0.0.0:8200:0 --logicalpoolid=1 --copysetid=1`
)

type ResetRpc struct {
	Info    *basecmd.Rpc
	Request *cli2.ResetPeerRequest2
	Client  cli2.CliService2Client
}

func (rRpc *ResetRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	rRpc.Client = cli2.NewCliService2Client(cc)
}

func (rRpc *ResetRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return rRpc.Client.ResetPeer(ctx, rRpc.Request)
}

// Options rpc request options.
type Options struct {
	Timeout    time.Duration
	RetryTimes int32
}

type PeerCommand struct {
	basecmd.FinalCurveCmd

	// request parameters
	opts          Options
	logicalPoolID uint32
	copysetID     uint32

	resetPeer *common.Peer
}

var _ basecmd.FinalCurveCmdFunc = (*PeerCommand)(nil) // check interface

// NewCommand ...
func NewPeerCommand() *cobra.Command {
	peerCmd := &PeerCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "peer",
			Short:   "update(reset) the peer from the copyset",
			Example: peerExample,
		},
	}
	basecmd.NewFinalCurveCli(&peerCmd.FinalCurveCmd, peerCmd)
	return peerCmd.Cmd
}

func (pCmd *PeerCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)

	config.AddBSLogicalPoolIdRequiredFlag(pCmd.Cmd)
	config.AddBSCopysetIdRequiredFlag(pCmd.Cmd)
}

// ParsePeer parse the peer string
func ParsePeer(peer string) (*common.Peer, *cmderror.CmdError) {
	cs := strings.Split(peer, ":")
	if len(cs) != 3 {
		pErr := cmderror.ErrSplitPeer()
		pErr.Format(peer)
		return nil, pErr
	}
	id, err := strconv.ParseUint(cs[2], 10, 64)
	if err != nil {
		pErr := cmderror.ErrSplitPeer()
		pErr.Format(peer)
		return nil, pErr
	}
	cs = cs[:2]
	address := strings.Join(cs, ":")
	return &common.Peer{
		Id:      &id,
		Address: &address,
	}, nil
}

func (pCmd *PeerCommand) Init(cmd *cobra.Command, args []string) error {
	pCmd.opts = Options{}

	pCmd.SetHeader([]string{curveutil.ROW_PEER, curveutil.ROW_COPYSET, curveutil.ROW_RESULT, curveutil.ROW_REASON})
	pCmd.TableNew.SetAutoMergeCellsByColumnIndex(curveutil.GetIndexSlice(
		pCmd.Header, []string{},
	))

	var e *cmderror.CmdError

	pCmd.opts.Timeout = config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	pCmd.opts.RetryTimes = config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)

	pCmd.copysetID = config.GetBsFlagUint32(pCmd.Cmd, config.CURVEBS_COPYSET_ID)

	pCmd.logicalPoolID = config.GetBsFlagUint32(pCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)

	// parse peer conf
	if len(args) < 1 {
		pErr := cmderror.ErrGetPeer()
		pErr.Format("should specified the peer address")
		return pErr.ToError()
	}
	pCmd.resetPeer, e = ParsePeer(args[0])
	if e != nil {
		return e.ToError()
	}
	return nil
}

func (pCmd *PeerCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *PeerCommand) RunCommand(cmd *cobra.Command, args []string) error {
	out := make(map[string]string)
	out[curveutil.ROW_PEER] = fmt.Sprintf("%s:%d", pCmd.resetPeer.GetAddress(), pCmd.resetPeer.GetId())
	out[curveutil.ROW_COPYSET] = fmt.Sprintf("(%d:%d)", pCmd.logicalPoolID, pCmd.copysetID)

	var err *cmderror.CmdError
	pCmd.Result, err = pCmd.execResetPeer()
	if err != nil {
		out[curveutil.ROW_RESULT] = curveutil.ROW_VALUE_FAILED
		out[curveutil.ROW_REASON] = err.ToError().Error()
		pCmd.Error = err
		return nil
	}
	out[curveutil.ROW_RESULT] = curveutil.ROW_VALUE_SUCCESS
	out[curveutil.ROW_REASON] = curveutil.ROW_VALUE_NULL
	list := curveutil.Map2List(out, pCmd.Header)
	pCmd.TableNew.Append(list)
	return nil
}

func (pCmd *PeerCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}

func (pCmd *PeerCommand) execResetPeer() (interface{}, *cmderror.CmdError) {
	rpcCli := &ResetRpc{
		Info: basecmd.NewRpc([]string{pCmd.resetPeer.GetAddress()}, pCmd.opts.Timeout, pCmd.opts.RetryTimes, "RsetPeer"),
		Request: &cli2.ResetPeerRequest2{
			LogicPoolId: &pCmd.logicalPoolID,
			CopysetId:   &pCmd.copysetID,
			RequestPeer: pCmd.resetPeer,
			OldPeers:    []*common.Peer{pCmd.resetPeer},
			NewPeers:    []*common.Peer{pCmd.resetPeer},
		},
	}

	response, errCmd := basecmd.GetRpcResponse(rpcCli.Info, rpcCli)

	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return response, errCmd
	}

	return response, nil
}
