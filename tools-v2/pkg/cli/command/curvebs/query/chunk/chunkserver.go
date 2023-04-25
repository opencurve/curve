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
* Project: curve
* Created Date: 2023-04-12
* Author: chengyi01
 */

package chunk

import (
	"context"
	"fmt"
	"strconv"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetChunkServerListRpc struct {
	Info      *basecmd.Rpc
	Request   *topology.GetChunkServerListInCopySetsRequest
	mdsClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*GetChunkServerListRpc)(nil) // check interface

func (gRpc *GetChunkServerListRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = topology.NewTopologyServiceClient(cc)
}

func (gRpc *GetChunkServerListRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetChunkServerListInCopySets(ctx, gRpc.Request)
}

type ChunkServerListInCoysetCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *GetChunkServerListRpc
	Response *topology.GetChunkServerListInCopySetsResponse
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkServerListInCoysetCommand)(nil) // check interface

func NewQueryChunkServerListCommand() *ChunkServerListInCoysetCommand {
	chunkCmd := &ChunkServerListInCoysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&chunkCmd.FinalCurveCmd, chunkCmd)
	return chunkCmd
}

func NewChunkServerListCommand() *cobra.Command {
	return NewQueryChunkServerListCommand().Cmd
}

func (cCmd *ChunkServerListInCoysetCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddBSLogicalPoolIdRequiredFlag(cCmd.Cmd)
	config.AddBSCopysetIdSliceRequiredFlag(cCmd.Cmd)
}

func (cCmd *ChunkServerListInCoysetCommand) Init(cmd *cobra.Command, args []string) error {
	logicalpool := config.GetBsFlagUint32(cCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	copysetidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_COPYSET_ID)
	copysetIds := make([]uint32, len(copysetidList))
	for i, s := range copysetidList {
		id, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return fmt.Errorf("converting %s to uint32 err: %s", s, err.Error())
		}
		copysetIds[i] = uint32(id)
	}
	request := topology.GetChunkServerListInCopySetsRequest{
		LogicalPoolId: &logicalpool,
		CopysetId:     copysetIds,
	}

	mdsAddrs, err := config.GetBsMdsAddrSlice(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)
	cCmd.Rpc = &GetChunkServerListRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetFileInfo"),
		Request: &request,
	}
	return nil
}

func (cCmd *ChunkServerListInCoysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *ChunkServerListInCoysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(cCmd.Rpc.Info, cCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	cCmd.Response = result.(*topology.GetChunkServerListInCopySetsResponse)
	if cCmd.Response.GetStatusCode() != int32(statuscode.TopoStatusCode_Success) {
		retErr := cmderror.ErrGetChunkServerListInCopySets(statuscode.TopoStatusCode(cCmd.Response.GetStatusCode()),
			cCmd.Rpc.Request.GetLogicalPoolId(), cCmd.Rpc.Request.GetCopysetId())
		return retErr.ToError()
	}
	return nil
}

func (cCmd *ChunkServerListInCoysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func GetChunkServerListInCopySets(caller *cobra.Command, logicalpoolId uint32, copysetId uint32) (*topology.GetChunkServerListInCopySetsResponse, *cmderror.CmdError) {
	getCmd := NewQueryChunkServerListCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_LOGIC_POOL_ID, config.CURVEBS_COPYSET_ID,
	})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{
		"--format", config.FORMAT_NOOUT,
		fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID),
		fmt.Sprintf("%d", logicalpoolId),
		fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID),
		fmt.Sprintf("%d", copysetId),
	})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsChunkServerListInCopySets()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return getCmd.Response, cmderror.Success()
}
