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
* Project: CurveCli
* Created Date: 2023-06-02
* Author: montaguelhz
 */
package chunkserver

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type GetChunkServerInfoRpc struct {
	Info      *basecmd.Rpc
	Request   *topology.GetChunkServerInfoRequest
	mdsClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*GetChunkServerInfoRpc)(nil)

func (gRpc *GetChunkServerInfoRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = topology.NewTopologyServiceClient(cc)
}

func (gRpc *GetChunkServerInfoRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetChunkServer(ctx, gRpc.Request)
}

type ChunkServerInfoCommand struct {
	basecmd.FinalCurveCmd
	Rpc   []*GetChunkServerInfoRpc
	infos []*topology.ChunkServerInfo
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkServerInfoCommand)(nil)

func (csiCmd *ChunkServerInfoCommand) AddFlags() {
	config.AddBsMdsFlagOption(csiCmd.Cmd)
	config.AddRpcRetryTimesFlag(csiCmd.Cmd)
	config.AddRpcTimeoutFlag(csiCmd.Cmd)

	config.AddBsChunkServerIdOptionFlag(csiCmd.Cmd)
	config.AddBsChunkServerAddrOptionFlag(csiCmd.Cmd)
}

func NewGetChunkServerInfoCommand() *ChunkServerInfoCommand {
	csiCmd := &ChunkServerInfoCommand{FinalCurveCmd: basecmd.FinalCurveCmd{}}
	basecmd.NewFinalCurveCli(&csiCmd.FinalCurveCmd, csiCmd)
	return csiCmd
}

func NewChunkServerInfoCommand() *cobra.Command {
	return NewGetChunkServerInfoCommand().Cmd
}

func (csiCmd *ChunkServerInfoCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(csiCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(csiCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(csiCmd.Cmd, config.RPCRETRYTIMES)

	var ids []uint32
	var addrs []string
	if viper.IsSet(config.VIPER_CURVEBS_CHUNKSERVER_ADDR) && !viper.IsSet(config.VIPER_CURVEBS_CHUNKSERVER_ID) {
		// chunkserveraddr is set, but chunkserverid is not set
		addrs, err = config.GetBsChunkserverAddrSlice(csiCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return fmt.Errorf(err.Message)
		}
	} else {
		ids = config.GetBsChunkServerId(csiCmd.Cmd)
	}
	if len(ids) == 0 && len(addrs) == 0 {
		return fmt.Errorf("%s or %s is required", config.CURVEBS_CHUNKSERVER_ADDR, config.CURVEBS_CHUNKSERVER_ID)
	}

	for i := range addrs {

		// The address has been checked for legitimacy
		addr := strings.Split(addrs[i], ":")
		port, _ := strconv.ParseUint(addrs[1], 10, 32)
		port32 := uint32(port)

		request := &topology.GetChunkServerInfoRequest{
			HostIp: &addr[0],
			Port:   &port32,
		}
		rpc := &GetChunkServerInfoRpc{
			Request: request,
			Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetChunkServerInfo"),
		}
		csiCmd.Rpc = append(csiCmd.Rpc, rpc)
	}

	for _, id := range ids {
		request := &topology.GetChunkServerInfoRequest{
			ChunkServerID: &id,
		}
		rpc := &GetChunkServerInfoRpc{
			Request: request,
			Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetChunkServerInfo"),
		}
		csiCmd.Rpc = append(csiCmd.Rpc, rpc)
	}

	return nil
}

func (csiCmd *ChunkServerInfoCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var rpcs []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range csiCmd.Rpc {
		rpcs = append(rpcs, rpc.Info)
		funcs = append(funcs, rpc)
	}

	results, errs := basecmd.GetRpcListResponse(rpcs, funcs)
	if len(errs) == len(rpcs) {
		mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
		return mergeErr.ToError()
	}
	for _, result := range results {
		response := result.(*topology.GetChunkServerInfoResponse)
		if response.GetStatusCode() != int32(statuscode.TopoStatusCode_Success) {
			code := response.GetStatusCode()
			err := cmderror.ErrGetChunkserverInfo(int(code))
			err.Format(statuscode.TopoStatusCode_name[int32(response.GetStatusCode())])
			errs = append(errs, err)
			continue
		}
		csiCmd.infos = append(csiCmd.infos, response.GetChunkServerInfo())
	}
	csiCmd.Result = results
	csiCmd.Error = cmderror.MostImportantCmdError(errs)
	return nil
}

func (csiCmd *ChunkServerInfoCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&csiCmd.FinalCurveCmd, csiCmd)
}

func (csiCmd *ChunkServerInfoCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&csiCmd.FinalCurveCmd)
}

func GetChunkServerInfo(caller *cobra.Command) ([]*topology.ChunkServerInfo, *cmderror.CmdError) {
	csiCmd := NewGetChunkServerInfoCommand()
	config.AlignFlagsValue(caller, csiCmd.Cmd, []string{config.CURVEBS_CHUNKSERVER_ADDR, config.CURVEBS_CHUNKSERVER_ID})
	csiCmd.Cmd.SilenceErrors = true
	csiCmd.Cmd.SilenceUsage = true
	csiCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := csiCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetChunkserverInfo()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return csiCmd.infos, cmderror.Success()
}
