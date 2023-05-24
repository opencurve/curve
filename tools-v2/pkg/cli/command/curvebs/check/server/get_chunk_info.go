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
 * Created Date: 2023-05-07
 * Author: pengpengSir
 */
package server

import (
	"context"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetChunkServerInfosRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListChunkServerRequest
	topologyClient topology.TopologyServiceClient
}

type GetChunkInfosCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *GetChunkServerInfosRpc
	Response *topology.ListChunkServerResponse
}

var _ basecmd.FinalCurveCmdFunc = (*GetChunkInfosCommand)(nil)

func NewChunkInfosCommand() *cobra.Command {
	return NewGetChunkServerInfosCommand().Cmd
}

func NewGetChunkServerInfosCommand() *GetChunkInfosCommand {
	gCmd := &GetChunkInfosCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}
	basecmd.NewFinalCurveCli(&gCmd.FinalCurveCmd, gCmd)
	return gCmd
}

func (gcRpc *GetChunkServerInfosRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gcRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (gcRpc *GetChunkServerInfosRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gcRpc.topologyClient.ListChunkServer(ctx, gcRpc.Request)
}

func (gCmd *GetChunkInfosCommand) AddFlags() {
	config.AddBsMdsFlagOption(gCmd.Cmd)
	config.AddRpcRetryTimesFlag(gCmd.Cmd)
	config.AddRpcTimeoutFlag(gCmd.Cmd)
	config.AddBsServerIdOptionFlag(gCmd.Cmd)
	config.AddBsIpOptionFlag(gCmd.Cmd)
	config.AddBsPortOptionFlag(gCmd.Cmd)
}

func (gCmd *GetChunkInfosCommand) Init(cmd *cobra.Command, args []string) error {
	timeout := config.GetFlagDuration(gCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(gCmd.Cmd, config.RPCRETRYTIMES)
	mdsAddrs, _ := config.GetBsMdsAddrSlice(gCmd.Cmd)
	ip := config.GetFlagString(gCmd.Cmd, config.CURVEBS_IP)
	port := config.GetFlagUint32(gCmd.Cmd, config.CURVEBS_PORT)
	serverid := config.GetFlagUint32(gCmd.Cmd, config.CURVEBS_SERVER_ID)

	if ip != "" {
		rpc := &GetChunkServerInfosRpc{
			Request: &topology.ListChunkServerRequest{
				Ip:   &ip,
				Port: &port,
			},
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListChunkServer"),
		}
		gCmd.Rpc = rpc
	} else {
		rpc := &GetChunkServerInfosRpc{
			Request: &topology.ListChunkServerRequest{
				ServerID: &serverid,
			},
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListChunkServer"),
		}
		gCmd.Rpc = rpc
	}
	return nil
}

func (gCmd *GetChunkInfosCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&gCmd.FinalCurveCmd, gCmd)
}

func (gCmd *GetChunkInfosCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&gCmd.FinalCurveCmd)
}

func (gCmd *GetChunkInfosCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(gCmd.Rpc.Info, gCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	gCmd.Response = result.(*topology.ListChunkServerResponse)
	return nil
}

func GetChunkserverInfos(caller *cobra.Command) ([]*topology.ChunkServerInfo, *cmderror.CmdError) {
	gCmd := NewGetChunkServerInfosCommand()
	config.AlignFlagsValue(caller, gCmd.Cmd, []string{
		config.CURVEBS_MDSADDR, config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_SERVER_ID, config.CURVEBS_IP, config.CURVEBS_PORT,
	})

	gCmd.Cmd.SilenceErrors = true
	gCmd.Cmd.SilenceUsage = true
	gCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := gCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetChunkServerInfos()
		retErr.Format(err.Error())
		return nil, retErr
	}

	return gCmd.Response.ChunkServerInfos, cmderror.Success()
}
