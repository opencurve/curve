/*
 *  Copyright (c) 2022 NetEase Inc.
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
	"strconv"
	"strings"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/cli2"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"google.golang.org/grpc"
)

// LeaderRpc the rpc client for the rpc function GetLeader
type LeaderRpc struct {
	Info    *basecmd.Rpc
	Request *cli2.GetLeaderRequest2
	Cli     cli2.CliService2Client
}

var _ basecmd.RpcFunc = (*LeaderRpc)(nil) // check interface

// NewRpcClient ...
func (ufRp *LeaderRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	ufRp.Cli = cli2.NewCliService2Client(cc)
}

// Stub_Func ...
func (ufRp *LeaderRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return ufRp.Cli.GetLeader(ctx, ufRp.Request)
}

// Configuration the configuration for peer.
type Configuration struct {
	Peers []*common.Peer
}

// Options rpc request options.
type Options struct {
	Timeout    time.Duration
	RetryTimes int32
}

// ParsePeer parse the peer string
func ParsePeer(peer string) (*common.Peer, *cmderror.CmdError) {
	cs := strings.Split(peer, ":")
	if len(cs) != 3 {
		pErr := cmderror.ErrGetPeer()
		pErr.Format(fmt.Sprintf("error format for the peer info %s", peer))
		return nil, pErr
	}
	id, err := strconv.ParseUint(cs[2], 10, 64)
	if err != nil {
		pErr := cmderror.ErrGetPeer()
		pErr.Format(fmt.Sprintf("error format for the peer id %s", cs[2]))
		return nil, pErr
	}
	cs = cs[:2]
	address := strings.Join(cs, ":")
	return &common.Peer{
		Id:      &id,
		Address: &address,
	}, nil
}

// ParseConfiguration parse the conf string into Configuration
func ParseConfiguration(peers []string) (*Configuration, *cmderror.CmdError) {
	if len(peers) == 0 {
		pErr := cmderror.ErrGetPeer()
		pErr.Format("empty group configuration")
		return nil, pErr
	}

	configuration := &Configuration{}
	for _, c := range peers {
		peer, err := ParsePeer(c)
		if err != nil {
			return nil, err
		}
		configuration.Peers = append(configuration.Peers, peer)
	}
	return configuration, nil
}

// GetLeader get leader for the address.
func GetLeader(logicalPoolID, copysetID uint32, conf Configuration, opts Options) (*common.Peer, *cmderror.CmdError) {
	if len(conf.Peers) == 0 {
		pErr := cmderror.ErrGetAddr()
		pErr.Format("peers", conf.Peers)
		return nil, pErr
	}
	var errors []*cmderror.CmdError
	for _, peer := range conf.Peers {
		rpcCli := &LeaderRpc{
			Request: &cli2.GetLeaderRequest2{
				LogicPoolId: &logicalPoolID,
				CopysetId:   &copysetID,
				Peer:        peer,
			},
			Info: basecmd.NewRpc([]string{peer.GetAddress()}, opts.Timeout, opts.RetryTimes, "GetLeader"),
		}
		response, errCmd := basecmd.GetRpcResponse(rpcCli.Info, rpcCli)
		if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
			errors = append(errors, errCmd)
			continue
		}
		resp, ok := response.(*cli2.GetLeaderResponse2)
		if !ok {
			pErr := cmderror.ErrRespTypeNoExpected()
			pErr.Format("cli2.GetLeaderResponse2")
			errors = append(errors, pErr)
			continue
		}
		if resp.Leader != nil {
			return resp.Leader, nil
		}
	}
	ret := cmderror.MergeCmdError(errors)
	return nil, ret
}
