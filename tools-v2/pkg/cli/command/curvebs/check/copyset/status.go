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
 * Created Date: 2023-04-24
 * Author: baytan
 */

package copyset

import (
	"context"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/proto/proto/copyset"
	"google.golang.org/grpc"
	"time"
)

type GetCopysetStatusRpc struct {
	Info      *basecmd.Rpc
	Request   *copyset.CopysetStatusRequest
	mdsClient copyset.CopysetServiceClient
}

type StatusResult struct {
	Request *copyset.CopysetStatusRequest
	Status  *copyset.CopysetStatusResponse
	Error   *cmderror.CmdError
	Addr    string
}

var _ basecmd.RpcFunc = (*GetCopysetStatusRpc)(nil) // check interface

func (gRpc *GetCopysetStatusRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = copyset.NewCopysetServiceClient(cc)
}

func (gRpc *GetCopysetStatusRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetCopysetStatus(ctx, gRpc.Request)
}

func GetCopysetStatus(reqs []*copyset.CopysetStatusRequest, timeout time.Duration, retrytimes int32) []*StatusResult {
	chanSize := len(reqs)
	if chanSize > config.MaxChannelSize() {
		chanSize = config.MaxChannelSize()
	}
	results := make(chan StatusResult, chanSize)
	size := 0
	for _, req := range reqs {
		size++
		rpc := &GetCopysetStatusRpc{
			Request: req,
		}
		rpc.Info = basecmd.NewRpc([]string{*req.Peer.Address}, timeout, retrytimes, "GetCopysetStatus")
		go func(rpc *GetCopysetStatusRpc, addr string) {
			result, err := basecmd.GetRpcResponse(rpc.Info, rpc)
			var response *copyset.CopysetStatusResponse
			if err.TypeCode() == cmderror.CODE_SUCCESS {
				response = result.(*copyset.CopysetStatusResponse)
			} else {
				response = nil
			}
			results <- StatusResult{
				Request: rpc.Request,
				Status:  response,
				Error:   err,
				Addr:    addr,
			}
		}(rpc, *req.Peer.Address)

	}
	retStatus := make([]*StatusResult, size)
	count := 0
	for res := range results {
		retStatus[count] = &StatusResult{
			Request: res.Request,
			Status:  res.Status,
			Error:   res.Error,
			Addr:    res.Addr,
		}
		count++
		if count >= size {
			break
		}
	}
	return retStatus
}
