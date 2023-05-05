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
 * Created Date: 2023-04-24
 * Author: baytan0720
 */

package chunk

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/chunk"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetChunkInfoRpc struct {
	Info      *basecmd.Rpc
	Request   *chunk.GetChunkInfoRequest
	mdsClient chunk.ChunkServiceClient
}

var _ basecmd.RpcFunc = (*GetChunkInfoRpc)(nil) // check interface

func (gRpc *GetChunkInfoRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = chunk.NewChunkServiceClient(cc)
}

func (gRpc *GetChunkInfoRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetChunkInfo(ctx, gRpc.Request)
}

type GetChunkInfoCommand struct {
	basecmd.FinalCurveCmd
	Rpc        []*GetChunkInfoRpc
	addr2Chunk *map[string]*chunk.GetChunkInfoResponse
}

var _ basecmd.FinalCurveCmdFunc = (*GetChunkInfoCommand)(nil) // check interface

func NewChunkInfoCommand() *cobra.Command {
	return NewGetChunkInfoCommand().Cmd
}

func NewGetChunkInfoCommand() *GetChunkInfoCommand {
	chunkCmd := &GetChunkInfoCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}
	basecmd.NewFinalCurveCli(&chunkCmd.FinalCurveCmd, chunkCmd)
	return chunkCmd
}

func (cCmd *GetChunkInfoCommand) Init(cmd *cobra.Command, args []string) error {
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)

	logicalpoolidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	copysetidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_COPYSET_ID)
	chunkidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_CHUNK_ID)
	addressList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_CHUNKSERVER_ADDRESS)
	if len(copysetidList) != len(logicalpoolidList) || len(copysetidList) != len(chunkidList) || len(copysetidList) != len(addressList) {
		return fmt.Errorf("copysetid, logicalpoolid, chunkid, address length not equal")
	}
	logicalpoolIds, errParse := cobrautil.StringList2Uint32List(logicalpoolidList)
	if errParse != nil {
		return fmt.Errorf("parse logicalpoolid %v fail", logicalpoolidList)
	}
	copysetIds, errParse := cobrautil.StringList2Uint32List(copysetidList)
	if errParse != nil {
		return fmt.Errorf("parse copysetid %v fail", copysetidList)
	}
	chunkIds, errParse := cobrautil.StringList2Uint64List(chunkidList)
	if errParse != nil {
		return fmt.Errorf("parse chunkid %v fail", chunkidList)
	}
	for i := 0; i < len(copysetIds); i++ {
		rpc := &GetChunkInfoRpc{
			Request: &chunk.GetChunkInfoRequest{
				LogicPoolId: &logicalpoolIds[i],
				CopysetId:   &copysetIds[i],
				ChunkId:     &chunkIds[i],
			},
			Info: basecmd.NewRpc([]string{addressList[i]}, timeout, retrytimes, "GetChunkInfo"),
		}
		cCmd.Rpc = append(cCmd.Rpc, rpc)
	}
	return nil
}

func (cCmd *GetChunkInfoCommand) RunCommand(cmd *cobra.Command, args []string) error {
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
	addr2Chunk := make(map[string]*chunk.GetChunkInfoResponse)
	cCmd.addr2Chunk = &addr2Chunk
	for i, result := range results {
		if resp, ok := result.(*chunk.GetChunkInfoResponse); ok {
			(*cCmd.addr2Chunk)[cCmd.Rpc[i].Info.Addrs[0]] = resp
		} else {
			(*cCmd.addr2Chunk)[cCmd.Rpc[i].Info.Addrs[0]] = nil
		}
	}
	return nil
}

func (cCmd *GetChunkInfoCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *GetChunkInfoCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *GetChunkInfoCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)

	config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsChunkIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsChunkServerAddressSliceRequiredFlag(cCmd.Cmd)
}

func GetChunkInfo(caller *cobra.Command) (*map[string]*chunk.GetChunkInfoResponse, *cmderror.CmdError) {
	sCmd := NewGetChunkInfoCommand()
	config.AlignFlagsValue(caller, sCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_LOGIC_POOL_ID, config.CURVEBS_COPYSET_ID, config.CURVEBS_CHUNK_ID, config.CURVEBS_CHUNKSERVER_ADDRESS,
	})
	sCmd.Cmd.SilenceErrors = true
	sCmd.Cmd.SilenceUsage = true
	sCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := sCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetChunkInfo()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return sCmd.addr2Chunk, cmderror.Success()
}
