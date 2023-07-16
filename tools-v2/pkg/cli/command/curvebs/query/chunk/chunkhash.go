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
 * Project: tools-v2
 * Created Date: 2023-10-03
 * Author: victorseptember
 */

package chunk

import (
	"context"
	"fmt"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/chunk"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetChunkHashRpc struct {
	Info      *basecmd.Rpc
	Request   *chunk.GetChunkHashRequest
	mdsClient chunk.ChunkServiceClient
}

var _ basecmd.RpcFunc = (*GetChunkHashRpc)(nil) // check interface

func (gRpc *GetChunkHashRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = chunk.NewChunkServiceClient(cc)
}

func (gRpc *GetChunkHashRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetChunkHash(ctx, gRpc.Request)
}

type GetChunkHashCommand struct {
	basecmd.FinalCurveCmd
	Rpc     []*GetChunkHashRpc
	result  bool
	explain string
}

var _ basecmd.FinalCurveCmdFunc = (*GetChunkHashCommand)(nil)

func NewChunkHashCommand() *cobra.Command {
	return NewGetChunkHashCommand().Cmd
}

func NewGetChunkHashCommand() *GetChunkHashCommand {
	chunkHashCmd := &GetChunkHashCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
		result:        true,
	}
	basecmd.NewFinalCurveCli(&chunkHashCmd.FinalCurveCmd, chunkHashCmd)
	return chunkHashCmd
}

func (cCmd *GetChunkHashCommand) Init(cmd *cobra.Command, args []string) error {
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)
	copysetid := config.GetBsFlagUint32(cCmd.Cmd, config.CURVEBS_COPYSET_ID)
	logicalpoolid := config.GetBsFlagUint32(cCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	chunkid := config.GetBsFlagUint64(cCmd.Cmd, config.CURVEBS_CHUNK_ID)
	chunksize := config.GetBsFlagUint32(cCmd.Cmd, config.CURVEBS_CHUNK_SIZE)
	peers := config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_PEERS_ADDRESS)

	var offset uint32 = 0
	peersAddress := strings.Split(peers, ",")
	for _, address := range peersAddress {
		rpc := &GetChunkHashRpc{
			Request: &chunk.GetChunkHashRequest{
				LogicPoolId: &logicalpoolid,
				CopysetId:   &copysetid,
				ChunkId:     &chunkid,
				Offset:      &offset,
				Length:      &chunksize,
			},
		}
		rpc.Info = basecmd.NewRpc([]string{address}, timeout, retrytimes, "GetChunkHash")
		cCmd.Rpc = append(cCmd.Rpc, rpc)
	}
	return nil
}

func (cCmd *GetChunkHashCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var infos []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range cCmd.Rpc {
		infos = append(infos, rpc.Info)
		funcs = append(funcs, rpc)
	}
	results, errs := basecmd.GetRpcListResponse(infos, funcs)
	if len(errs) == len(infos) {
		mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
		return mergeErr.ToError()
	}

	var preHash string
	var curHash string
	flag := true
	for _, result := range results {
		response, ok := result.(*chunk.GetChunkHashResponse)
		if !ok {
			return fmt.Errorf("response is nil")
		}
		if *response.Status != chunk.CHUNK_OP_STATUS_CHUNK_OP_STATUS_SUCCESS {
			cCmd.result = false
			cCmd.explain += chunk.CHUNK_OP_STATUS_name[int32(*response.Status)]
			break
		}
		curHash = *response.Hash
		if flag {
			flag = false
			preHash = curHash
			continue
		}
		if preHash != curHash {
			cCmd.result = false
			cCmd.explain += "chunk hash inconsistent"
			break
		}
	}
	return nil
}

func (cCmd *GetChunkHashCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *GetChunkHashCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *GetChunkHashCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)

	config.AddBsCopysetIdRequiredFlag(cCmd.Cmd)
	config.AddBsLogicalPoolIdRequiredFlag(cCmd.Cmd)
	config.AddBsPeersAddressFlag(cCmd.Cmd)
	config.AddBsChunkIdRequiredFlag(cCmd.Cmd)
	config.AddBsChunkSizeRequiredFlag(cCmd.Cmd)
}

func GetChunkHash(caller *cobra.Command) (bool, string, *cmderror.CmdError) {
	cCmd := NewGetChunkHashCommand()
	config.AlignFlagsValue(caller, cCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_LOGIC_POOL_ID, config.CURVEBS_PEERS_ADDRESS, config.CURVEBS_CHUNK_ID,
		config.CURVEBS_CHUNK_SIZE, config.CURVEBS_COPYSET_ID,
	})
	cCmd.Cmd.SilenceErrors = true
	cCmd.Cmd.SilenceUsage = true
	cCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := cCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetChunkHash()
		retErr.Format(err.Error())
		return false, "", retErr
	}
	return cCmd.result, cCmd.explain, nil
}
