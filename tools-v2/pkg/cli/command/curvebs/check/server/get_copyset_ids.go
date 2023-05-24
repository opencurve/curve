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
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetChunkServerCopysetsRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.GetCopySetsInChunkServerRequest
	topologyClient topology.TopologyServiceClient
}

type CopysetidsCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *GetChunkServerCopysetsRpc
	Response *topology.GetCopySetsInChunkServerResponse
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetidsCommand)(nil)

func NewCopysetCommand() *cobra.Command {
	return NewCopysetidsCommand().Cmd
}

func NewCopysetidsCommand() *CopysetidsCommand {
	cCmd := &CopysetidsCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}
	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd
}

func (tRpc *GetChunkServerCopysetsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	tRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (tRpc *GetChunkServerCopysetsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return tRpc.topologyClient.GetCopySetsInChunkServer(ctx, tRpc.Request)
}

func (cCmd *CopysetidsCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddBsChunkServerIDOptionFlag(cCmd.Cmd)
	config.AddBsIpOptionFlag(cCmd.Cmd)
	config.AddBsPortOptionFlag(cCmd.Cmd)
}

func (cCmd *CopysetidsCommand) Init(cmd *cobra.Command, args []string) error {
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)
	mdsAddrs, _ := config.GetBsMdsAddrSlice(cCmd.Cmd)

	chunkserverId := config.GetBsFlagUint32(cCmd.Cmd, config.CURVEBS_CHUNKSERVER_ID)
	hostip := config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_IP)
	port := config.GetBsFlagUint32(cCmd.Cmd, config.CURVEBS_PORT)

	rpc := &GetChunkServerCopysetsRpc{
		Request: &topology.GetCopySetsInChunkServerRequest{
			ChunkServerID: &chunkserverId,
			HostIp:        &hostip,
			Port:          &port,
		},
		Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetCopysetInChunkServer"),
	}
	cCmd.Rpc = rpc
	return nil
}

func (cCmd *CopysetidsCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetidsCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *CopysetidsCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(cCmd.Rpc.Info, cCmd.Rpc)

	if err.Message != "success" {
		fmt.Print(err.Message)
		return err.ToError()
	}

	cCmd.Response = result.(*topology.GetCopySetsInChunkServerResponse)
	return nil
}

func GetCopysetids(caller *cobra.Command) (*map[uint32]uint32, *cmderror.CmdError) {
	cCmd := NewCopysetidsCommand()
	config.AlignFlagsValue(caller, cCmd.Cmd, []string{
		config.CURVEBS_MDSADDR, config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_CHUNKSERVER_ID,
		config.CURVEBS_IP, config.CURVEBS_PORT,
	})
	cCmd.Cmd.SilenceErrors = true
	cCmd.Cmd.SilenceUsage = true
	cCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := cCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetCopysetStatus()
		retErr.Format(err.Error())
		return nil, retErr
	}

	copysetID2PoolId := make(map[uint32]uint32)
	for _, copysetInfo := range cCmd.Response.GetCopysetInfos() {
		copysetID2PoolId[copysetInfo.GetCopysetId()] = copysetInfo.GetLogicalPoolId()
	}

	return &copysetID2PoolId, cmderror.Success()
}
