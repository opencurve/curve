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
* Created Date: 2023-04-26
* Author: chengyi01
 */

package copyset

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetCoysetRpc struct {
	Info      *basecmd.Rpc
	Request   *topology.GetCopysetRequest
	mdsClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*GetCoysetRpc)(nil) // check interface

func (gRpc *GetCoysetRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = topology.NewTopologyServiceClient(cc)
}

func (gRpc *GetCoysetRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetCopyset(ctx, gRpc.Request)
}

type CopysetCommand struct {
	CopysetInfoList []*common.CopysetInfo
	Rpc             []*GetCoysetRpc
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

func NewQueryCopysetCommand() *CopysetCommand {
	copysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func NewCopysetCommand() *cobra.Command {
	return NewQueryCopysetCommand().Cmd
}

func (cCmd *CopysetCommand) AddFlags() {
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	logicalpoolidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	copysetidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_COPYSET_ID)
	if len(logicalpoolidList) != len(copysetidList) {
		return fmt.Errorf("logicalpoolidList and copysetidList length not equal")
	}
	logicalpoolIds, errParse := curveutil.StringList2Uint64List(logicalpoolidList)
	if errParse != nil {
		return fmt.Errorf("parse logicalpoolid%v fail", logicalpoolidList)
	}
	copysetIds, errParse := curveutil.StringList2Uint64List(copysetidList)
	if errParse != nil {
		return fmt.Errorf("parse copysetid%v fail", copysetidList)
	}
	mdsAddrs, err := config.GetBsMdsAddrSlice(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)
	for i, logicalPool := range logicalpoolIds {
		lpId := uint32(logicalPool)
		cpId := uint32(copysetIds[i])
		cCmd.Rpc = append(cCmd.Rpc, &GetCoysetRpc{
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetCopyset"),
			Request: &topology.GetCopysetRequest{
				LogicalPoolId: &lpId,
				CopysetId:     &cpId,
			},
		})
	}

	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
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
	for i, result := range results {
		if result != nil {
			continue
		}
		res := result.(*topology.GetCopysetResponse)
		request := cCmd.Rpc[i].Request
		err := cmderror.ErrBsGetCopyset(statuscode.TopoStatusCode(res.GetStatusCode()), request.GetLogicalPoolId(), request.GetCopysetId())
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			errs = append(errs, err)
		} else {
			cCmd.CopysetInfoList = append(cCmd.CopysetInfoList, res.GetCopysetInfo())
		}
	}
	cCmd.Result = cCmd.CopysetInfoList
	cCmd.Error = cmderror.MergeCmdErrorExceptSuccess(errs)
	return nil
}

func (cCmd *CopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func GetCopyset(caller *cobra.Command) ([]*common.CopysetInfo, *cmderror.CmdError) {
	getCmd := NewQueryCopysetCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_LOGIC_POOL_ID, config.CURVEBS_COPYSET_ID,
	})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsCreateFileOrDirectoryType()
		retErr.Format(err.Error())
		return getCmd.CopysetInfoList, retErr
	}
	return getCmd.CopysetInfoList, cmderror.Success()
}
