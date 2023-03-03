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

	"google.golang.org/grpc"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/copyset"
)

// DeleteBrokenCopysetRPC the rpc client for the rpc function DeleteBrokenCopyset
type DeleteBrokenCopysetRPC struct {
	Info    *basecmd.Rpc
	Request *copyset.CopysetRequest
	Cli     copyset.CopysetServiceClient
}

var _ basecmd.RpcFunc = (*DeleteBrokenCopysetRPC)(nil) // check interface

// NewRpcClient ...
func (ufRp *DeleteBrokenCopysetRPC) NewRpcClient(cc grpc.ClientConnInterface) {
	ufRp.Cli = copyset.NewCopysetServiceClient(cc)
}

// Stub_Func ...
func (ufRp *DeleteBrokenCopysetRPC) Stub_Func(ctx context.Context) (interface{}, error) {
	return ufRp.Cli.DeleteBrokenCopyset(ctx, ufRp.Request)
}

// DeleteBrokenCopyset delete broken copyset.
func DeleteBrokenCopyset(logicalPoolID, copysetID uint32, peer *common.Peer, opts Options) (interface{}, *cmderror.CmdError) {
	rpcCli := &DeleteBrokenCopysetRPC{
		Request: &copyset.CopysetRequest{
			LogicPoolId: &logicalPoolID,
			CopysetId:   &copysetID,
		},
		Info: basecmd.NewRpc([]string{peer.GetAddress()}, opts.Timeout, opts.RetryTimes, "DeleteBrokenCopyset"),
	}
	response, errCmd := basecmd.GetRpcResponse(rpcCli.Info, rpcCli)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, errCmd
	}
	resp, ok := response.(*copyset.CopysetResponse)
	if !ok {
		pErr := cmderror.ErrRespTypeNoExpected()
		pErr.Format(resp, "copyset.CopysetResponse")
		return response, pErr
	}
	return resp, cmderror.ErrBsCopysetOpStatus(resp.GetStatus(), peer.GetAddress())
}
