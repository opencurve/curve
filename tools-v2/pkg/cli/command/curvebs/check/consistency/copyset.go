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
 * Created Date: 2022-11-04
 * Author: ShiYue
 */
package consistency

import (
	"context"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

type Set struct {
	Elems map[uint32][]uint32
}

func NewSet() *Set {
	var m = make(map[uint32][]uint32)
	newSet := &Set{
		Elems: m,
	}
	return newSet
}

func (s *Set) Exsit(lpid, cid uint32) {
	_, ok := s.Elems[lpid]
	if !ok {
		s.Elems[lpid] = append(s.Elems[lpid], cid)
	} else {
		index := slices.Index(s.Elems[lpid], cid)
		if index == -1 {
			s.Elems[lpid] = append(s.Elems[lpid], cid)
		}
	}
}

func (s *Set) Add(lpid uint32, cids []uint32) {
	s.Elems[lpid] = cids
}

type GetChunkServerListInCopySetsRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.GetChunkServerListInCopySetsRequest
	TopologyClient topology.TopologyServiceClient
}

func (rpc *GetChunkServerListInCopySetsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	rpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (rpc *GetChunkServerListInCopySetsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return rpc.TopologyClient.GetChunkServerListInCopySets(ctx, rpc.Request)
}

func (csCmd *ConsistencyCmd) GetChunkServerListOfCopySets(lpid uint32, cpsIds []uint32) (*topology.GetChunkServerListInCopySetsResponse, *cmderror.CmdError) {

	request := &topology.GetChunkServerListInCopySetsRequest{
		LogicalPoolId: &lpid,
		CopysetId:     cpsIds,
	}
	csCmd.getCksInCopySetsRpc = &GetChunkServerListInCopySetsRpc{
		Request: request,
	}
	csCmd.getCksInCopySetsRpc.Info = basecmd.NewRpc(csCmd.addrs, csCmd.timeout, csCmd.retryTimes, "GetChunkServerListInCopySets")
	res, err := basecmd.GetRpcResponse(csCmd.getCksInCopySetsRpc.Info, csCmd.getCksInCopySetsRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err
	}
	response := res.(*topology.GetChunkServerListInCopySetsResponse)
	if response.GetStatusCode() != 0 {
		return nil, cmderror.ErrGetChunkServerListInCopySetsRpc(statuscode.TopoStatusCode(response.GetStatusCode()), lpid)
	}
	return response, cmderror.ErrSuccess()
}
