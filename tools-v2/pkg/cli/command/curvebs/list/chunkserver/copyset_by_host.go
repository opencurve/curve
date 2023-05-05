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
 * Created Date: 2023-04-28
 * Author: baytan0720
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

type GetCopySetsInChunkServerByHostRpc struct {
	Info      *basecmd.Rpc
	Request   *topology.GetCopySetsInChunkServerRequest
	mdsClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*GetCopySetsInChunkServerByHostRpc)(nil) // check interface

func (gRpc *GetCopySetsInChunkServerByHostRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = topology.NewTopologyServiceClient(cc)
}

func (gRpc *GetCopySetsInChunkServerByHostRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetCopySetsInChunkServer(ctx, gRpc.Request)
}

type CopySetsInChunkServerCommand struct {
	basecmd.FinalCurveCmd
	Rpc           []*GetCopySetsInChunkServerByHostRpc
	addr2Copysets *map[string][]*common.CopysetInfo
}

var _ basecmd.FinalCurveCmdFunc = (*CopySetsInChunkServerCommand)(nil) // check interface

func NewCopySetsInChunkServerByHostCommand() *cobra.Command {
	return NewGetCopySetsInChunkServerCommand().Cmd
}

func NewGetCopySetsInChunkServerCommand() *CopySetsInChunkServerCommand {
	copysetCmd := &CopySetsInChunkServerCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func (cCmd *CopySetsInChunkServerCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)
	chunkserverAddrs := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_CHUNKSERVER_ADDRESS)
	for i := 0; i < len(chunkserverAddrs); i++ {
		hostIp, port, addrErr := cobrautil.Addr2IpPort(chunkserverAddrs[i])
		if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
			return addrErr.ToError()
		}
		rpc := &GetCopySetsInChunkServerByHostRpc{
			Request: &topology.GetCopySetsInChunkServerRequest{
				HostIp: &hostIp,
				Port:   &port,
			},
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetCopySetsInChunkServer"),
		}
		cCmd.Rpc = append(cCmd.Rpc, rpc)
	}
	return nil
}

func (cCmd *CopySetsInChunkServerCommand) RunCommand(cmd *cobra.Command, args []string) error {
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
	cCmd.addr2Copysets = &map[string][]*common.CopysetInfo{}
	for i, result := range results {
		resp := result.(*topology.GetCopySetsInChunkServerResponse)
		if resp.GetStatusCode() != int32(statuscode.TopoStatusCode_Success) {
			err := cmderror.ErrBsGetCopysetInChunkServerRpc(
				statuscode.TopoStatusCode(resp.GetStatusCode()),
			)
			errs = append(errs, err)
			continue
		}
		addr := fmt.Sprintf("%s:%d", *cCmd.Rpc[i].Request.HostIp, *cCmd.Rpc[i].Request.Port)
		(*cCmd.addr2Copysets)[addr] = resp.CopysetInfos
	}
	errRet := cmderror.MergeCmdError(errs)
	cCmd.Error = errRet
	return nil
}

func (cCmd *CopySetsInChunkServerCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopySetsInChunkServerCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *CopySetsInChunkServerCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)

	config.AddBsChunkServerAddressSliceRequiredFlag(cCmd.Cmd)
}

func GetCopySetsInChunkServerByHost(caller *cobra.Command) (*map[string][]*common.CopysetInfo, *cmderror.CmdError) {
	gCmd := NewGetCopySetsInChunkServerCommand()
	config.AlignFlagsValue(caller, gCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
	})
	gCmd.Cmd.SilenceErrors = true
	gCmd.Cmd.SilenceUsage = true
	gCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := gCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetCopysetInChunkServer()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return gCmd.addr2Copysets, cmderror.Success()
}
