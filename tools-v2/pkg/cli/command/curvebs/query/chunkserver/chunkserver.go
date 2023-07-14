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

package chunkserver

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
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
	Rpc          []*GetChunkServerListRpc
	key2Location map[uint64][]*common.ChunkServerLocation
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
	config.AddBsLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
}

func (cCmd *ChunkServerListInCoysetCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)
	logicalpoolidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	copysetidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_COPYSET_ID)
	if len(logicalpoolidList) != len(copysetidList) {
		return fmt.Errorf("logicalpoolidList and copysetidList length not equal")
	}
	logicalpoolIds, errParse := cobrautil.StringList2Uint64List(logicalpoolidList)
	if errParse != nil {
		return fmt.Errorf("Parse logicalpoolid", logicalpoolidList, " fail!")
	}
	copysetIds, errParse := cobrautil.StringList2Uint64List(copysetidList)
	if errParse != nil {
		return fmt.Errorf("Parse copysetid", copysetidList, " fail!")
	}
	logicalpool2copysets := make(map[uint32][]uint32)
	for i := 0; i < len(logicalpoolidList); i++ {
		lpid := logicalpoolIds[i]
		cpid := copysetIds[i]
		logicalpool2copysets[uint32(lpid)] = append(logicalpool2copysets[uint32(lpid)], uint32(cpid))
	}

	for logicalpoolId, copysetIds := range logicalpool2copysets {
		// for get pointer
		logicalpoolId := logicalpoolId
		cCmd.Rpc = append(cCmd.Rpc, &GetChunkServerListRpc{
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetFileInfo"),
			Request: &topology.GetChunkServerListInCopySetsRequest{
				LogicalPoolId: &logicalpoolId,
				CopysetId:     copysetIds,
			},
		})
	}
	return nil
}

func (cCmd *ChunkServerListInCoysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *ChunkServerListInCoysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
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

	key2Location := make(map[uint64][]*common.ChunkServerLocation)
	cCmd.key2Location = key2Location
	for i, result := range results {
		logicalpoolid := cCmd.Rpc[i].Request.GetLogicalPoolId()
		copysetids := cCmd.Rpc[i].Request.GetCopysetId()
		response := result.(*topology.GetChunkServerListInCopySetsResponse)
		if response.GetStatusCode() != int32(statuscode.TopoStatusCode_Success) {
			err := cmderror.ErrGetChunkServerListInCopySets(statuscode.TopoStatusCode(response.GetStatusCode()),
				logicalpoolid, copysetids)
			errs = append(errs, err)
			continue
		}
		for _, info := range response.CsInfo {
			key := cobrautil.GetCopysetKey(uint64(logicalpoolid), uint64(*info.CopysetId))
			cCmd.key2Location[key] = info.CsLocs
		}
	}
	errRet := cmderror.MergeCmdErrorExceptSuccess(errs)
	cCmd.Error = errRet
	if errRet.TypeCode() != cmderror.CODE_SUCCESS {
		return errRet.ToError()
	}
	return nil
}

func (cCmd *ChunkServerListInCoysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func GetChunkServerListInCopySets(caller *cobra.Command) (map[uint64][]*common.ChunkServerLocation, *cmderror.CmdError) {
	getCmd := NewQueryChunkServerListCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_LOGIC_POOL_ID, config.CURVEBS_COPYSET_ID,
	})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsChunkServerListInCopySets()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return getCmd.key2Location, cmderror.Success()
}
