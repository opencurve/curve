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
 * Project: CurveCli
 * Created Date: 2022-06-22
 * Author: chengyi (Cyber-SiKu)
 */

package copyset

import (
	"context"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/copyset"
	"google.golang.org/grpc"
)

type StatusCopysetRpc struct {
	Info          *basecmd.Rpc
	Request       *copyset.CopysetsStatusRequest
	copysetClient copyset.CopysetServiceClient
}

type StatusResult struct {
	Request *copyset.CopysetsStatusRequest
	Status  *copyset.CopysetsStatusResponse
	Error   *cmderror.CmdError
	Addr    string
}

var _ basecmd.RpcFunc = (*StatusCopysetRpc)(nil) // check interface

func (scRpc *StatusCopysetRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	scRpc.copysetClient = copyset.NewCopysetServiceClient(cc)
}

func (scRpc *StatusCopysetRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return scRpc.copysetClient.GetCopysetsStatus(ctx, scRpc.Request)
}

func GetCopysetsStatus(addr2Request map[string]*copyset.CopysetsStatusRequest, timeout time.Duration, retrytimes int32) []*StatusResult {
	chanSize := len(addr2Request)
	if chanSize > config.MaxChannelSize() {
		chanSize = config.MaxChannelSize()
	}
	results := make(chan StatusResult, chanSize)
	size := 0
	for k, v := range addr2Request {
		size++
		rpc := &StatusCopysetRpc{
			Request: v,
		}
		rpc.Info = basecmd.NewRpc([]string{k}, timeout, retrytimes, "GetCopysetsStatus")
		go func(rpc *StatusCopysetRpc, addr string) {
			result, err := basecmd.GetRpcResponse(rpc.Info, rpc)
			var response *copyset.CopysetsStatusResponse
			if err.TypeCode() == cmderror.CODE_SUCCESS {
				response = result.(*copyset.CopysetsStatusResponse)
			} else {
				response = nil
			}
			results <- StatusResult{
				Request: rpc.Request,
				Status:  response,
				Error:   err,
				Addr:    addr,
			}
		}(rpc, k)
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
