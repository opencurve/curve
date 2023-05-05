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
 * Created Date: 2023-03-27
 * Author: Sindweller
 */

package chunkserver

import (
	"context"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	common "github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"strconv"
)

type GetCopySetsInChunkServerRpc struct {
	Info                  *basecmd.Rpc
	Request               *topology.GetCopySetsInChunkServerRequest
	topologyServiceClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*GetCopySetsInChunkServerRpc)(nil) // check interface

type CopySetCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *GetCopySetsInChunkServerRpc
	Response *topology.GetCopySetsInChunkServerResponse
	CopySets []*common.CopysetInfo
}

var _ basecmd.FinalCurveCmdFunc = (*CopySetCommand)(nil) // check interface

func (lRpc *GetCopySetsInChunkServerRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.topologyServiceClient = topology.NewTopologyServiceClient(cc)
}

func (lRpc *GetCopySetsInChunkServerRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.topologyServiceClient.GetCopySetsInChunkServer(ctx, lRpc.Request)
}

func NewCopySetsInChunkServerCommand() *cobra.Command {
	return NewGetCopySetsInCopySetCommand().Cmd
}

func NewGetCopySetsInCopySetCommand() *CopySetCommand {
	lsCmd := &CopySetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&lsCmd.FinalCurveCmd, lsCmd)
	return lsCmd
}

// AddFlags implements basecmd.FinalCurveCmdFunc
func (pCmd *CopySetCommand) AddFlags() {
	config.AddBsMdsFlagOption(pCmd.Cmd)
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
	config.AddBsUserOptionFlag(pCmd.Cmd)
	config.AddBsPasswordOptionFlag(pCmd.Cmd)
	config.AddBSChunkServerIdFlag(pCmd.Cmd)
}

// Init implements basecmd.FinalCurveCmdFunc
func (pCmd *CopySetCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	timeout := config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)
	strid, e := strconv.Atoi(config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_CHUNKSERVER_ID))
	if e != nil {
		return e
	}
	id := uint32(strid)
	pCmd.Rpc = &GetCopySetsInChunkServerRpc{
		Request: &topology.GetCopySetsInChunkServerRequest{
			ChunkServerID: &id,
		},
		Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetCopySetsInChunkServer"),
	}

	header := []string{
		cobrautil.ROW_ID,
		cobrautil.ROW_TYPE,
		cobrautil.ROW_IP,
		cobrautil.ROW_PORT,
		cobrautil.ROW_RW_STATUS,
		cobrautil.ROW_DISK_STATE,
		cobrautil.ROW_COPYSET_NUM,
		cobrautil.ROW_MOUNTPOINT,
		cobrautil.ROW_DISK_CAPACITY,
		cobrautil.ROW_DISK_USED,
		cobrautil.ROW_UNHEALTHY_COPYSET,
		cobrautil.ROW_EXT_ADDR,
	}
	pCmd.SetHeader(header)
	pCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		pCmd.Header, []string{cobrautil.ROW_TYPE, cobrautil.ROW_IP, cobrautil.ROW_DISK_STATE},
	))
	return nil
}

// Print implements basecmd.FinalCurveCmdFunc
func (pCmd *CopySetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

// RunCommand implements basecmd.FinalCurveCmdFunc
func (pCmd *CopySetCommand) RunCommand(cmd *cobra.Command, args []string) error {

	result, err := basecmd.GetRpcResponse(pCmd.Rpc.Info, pCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		pCmd.Error = err
		pCmd.Result = result
		return err.ToError()
	}

	pCmd.Response = result.(*topology.GetCopySetsInChunkServerResponse)
	infos := pCmd.Response.GetCopysetInfos()
	rows := make([]map[string]string, 0)
	var errors []*cmderror.CmdError
	pCmd.CopySets = append(pCmd.CopySets, infos...)

	list := cobrautil.ListMap2ListSortByKeys(rows, pCmd.Header, []string{cobrautil.ROW_TYPE, cobrautil.ROW_RW_STATUS, cobrautil.ROW_MOUNTPOINT})
	pCmd.TableNew.AppendBulk(list)
	errRet := cmderror.MergeCmdError(errors)
	pCmd.Error = errRet
	pCmd.Result = result
	return nil
}

// ResultPlainOutput implements basecmd.FinalCurveCmdFunc
func (pCmd *CopySetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}

func GetCopySetsInChunkServer(caller *cobra.Command) ([]*common.CopysetInfo, *cmderror.CmdError) {
	getCopySetsCmd := NewGetCopySetsInCopySetCommand()
	config.AlignFlagsValue(caller, getCopySetsCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_CHUNKSERVER_ID,
	})
	getCopySetsCmd.Cmd.SilenceErrors = true
	getCopySetsCmd.Cmd.SilenceUsage = true
	getCopySetsCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := getCopySetsCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetChunkCopyset()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return getCopySetsCmd.CopySets, cmderror.ErrSuccess()
}
