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
 * Created Date: 2023-05-07
 * Author: pengpengSir
 */
package server

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"

	copysetbs "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/copyset"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	serverExample = `$ curve bs check server`
)

type CheckServerRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListChunkServerRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*CheckServerRpc)(nil)

type ServerCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *CheckServerRpc
	Response *topology.ListChunkServerResponse
	ServerID uint32
	ServerIP string
	Port     uint32
}

var _ basecmd.FinalCurveCmdFunc = (*ServerCommand)(nil)

func (cRpc *CheckServerRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	cRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (cRpc *CheckServerRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return cRpc.topologyClient.ListChunkServer(ctx, cRpc.Request)
}

func NewServerCommand() *cobra.Command {
	return NewCheckServerCommand().Cmd
}

func (pCmd *ServerCommand) AddFlags() {
	config.AddBsMdsFlagOption(pCmd.Cmd)
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
	config.AddBsServerIdFlag(pCmd.Cmd)
	config.AddBsServerIpFlag(pCmd.Cmd)
	config.AddBsPortFlag(pCmd.Cmd)
}

func NewCheckServerCommand() *ServerCommand {
	ckCmd := &ServerCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "server",
			Short:   "check all copysets infomation on server",
			Example: serverExample,
		},
	}

	basecmd.NewFinalCurveCli(&ckCmd.FinalCurveCmd, ckCmd)
	return ckCmd
}

func (pCmd *ServerCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	timeout := config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)

	// 解析出serverID或者serverIP
	serverID, _ := config.GetBsFlagUint32(pCmd.Cmd, config.CURVEBS_SERVER_ID)
	serverIP := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_SERVER_IP)
	port, _ := config.GetBsFlagUint32(pCmd.Cmd, config.CURVEBS_PORT)
	pCmd.ServerID = serverID
	pCmd.ServerIP = serverIP
	pCmd.Port = port

	if serverIP != "" {
		rpc := &CheckServerRpc{
			Request: &topology.ListChunkServerRequest{
				Ip:   &serverIP,
				Port: &port,
			},
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListChunkServer"),
		}
		pCmd.Rpc = rpc
	} else {
		rpc := &CheckServerRpc{
			Request: &topology.ListChunkServerRequest{
				ServerID: &serverID,
			},
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListChunkServer"),
		}
		pCmd.Rpc = rpc
	}

	// 创建表的header
	header := []string{cobrautil.ROW_SERVER, cobrautil.ROW_IP, cobrautil.ROW_TOTAL,
		cobrautil.ROW_UNHEALTHY_COPYSET_COUNT, cobrautil.ROW_UNHEALTHY_COPYSET_RATIO,
	}
	pCmd.SetHeader(header)
	return nil
}

func (pCmd *ServerCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *ServerCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}

func (pCmd *ServerCommand) RunCommand(cmd *cobra.Command, args []string) error {
	// 1 send RPCs to get all ChunkServerInfo on server
	response, errCmd := basecmd.GetRpcResponse(pCmd.Rpc.Info, pCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return errCmd.ToError()
	}
	timeout := config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)

	pCmd.Response = response.(*topology.ListChunkServerResponse)
	chunkServerInfos := pCmd.Response.ChunkServerInfos
	ip_out := pCmd.ServerIP
	if len(chunkServerInfos) != 0 {
		ip_out = chunkServerInfos[0].GetHostIp()
	}

	mdsAddrs := config.GetBsFlagString(cmd, config.CURVEBS_MDSADDR)

	copysetid2Status := make(map[uint32]*copyset.COPYSET_OP_STATUS)

	total := 0
	healthy := 0
	unhelthy := 0

	for _, item := range chunkServerInfos {
		// get copysetids from chunkserver
		chunkServerID := item.GetChunkServerID()
		hostip := item.GetHostIp()
		port := item.GetPort()
		pCmd.Cmd.ResetFlags()
		config.AddBsMdsFlagOption(pCmd.Cmd)
		config.AddRpcRetryTimesFlag(pCmd.Cmd)
		config.AddRpcTimeoutFlag(pCmd.Cmd)
		config.AddBsChunkServerIDFlag(pCmd.Cmd)
		config.AddBsHostIpFlag(pCmd.Cmd)
		config.AddBsPortFlag(pCmd.Cmd)

		pCmd.Cmd.ParseFlags([]string{
			fmt.Sprintf("--%s", config.CURVEBS_MDSADDR), mdsAddrs,
			fmt.Sprintf("--%s", config.RPCRETRYTIMES), fmt.Sprintf("%d", retrytimes),
			fmt.Sprintf("--%s", config.RPCTIMEOUT), timeout.String(),
			fmt.Sprintf("--%s", config.CURVEBS_CHUNK_SERVER_ID), fmt.Sprintf("%d", chunkServerID),
			fmt.Sprintf("--%s", config.CURVEBS_HOST_IP), hostip,
			fmt.Sprintf("--%s", config.CURVEBS_PORT), fmt.Sprintf("%d", port),
		})

		copysetids2poolids, err := GetCopysetids(pCmd.Cmd)
		if err.Message != "success" {
			return err.ToError()
		}

		// get copyset status
		pCmd.Cmd.ResetFlags()
		config.AddBsMdsFlagOption(pCmd.Cmd)
		config.AddRpcRetryTimesFlag(pCmd.Cmd)
		config.AddRpcTimeoutFlag(pCmd.Cmd)
		config.AddBSCopysetIdRequiredFlag(pCmd.Cmd)
		config.AddBSLogicalPoolIdRequiredFlag(pCmd.Cmd)
		config.AddBSPeersConfFlag(pCmd.Cmd)

		peerAddr := fmt.Sprintf("%s:%d", hostip, port)
		for copysetid, poolid := range *copysetids2poolids {
			pCmd.Cmd.ParseFlags([]string{
				fmt.Sprintf("--%s", config.CURVEBS_MDSADDR), mdsAddrs,
				fmt.Sprintf("--%s", config.RPCRETRYTIMES), fmt.Sprintf("%d", retrytimes),
				fmt.Sprintf("--%s", config.RPCTIMEOUT), timeout.String(),
				fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), fmt.Sprintf("%d", copysetid),
				fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), fmt.Sprintf("%d", poolid),
				fmt.Sprintf("--%s", config.CURVEBS_PEERS_ADDRESS), peerAddr,
			})

			result, err := copysetbs.GetCopysetStatus(pCmd.Cmd)
			if err.Message != "success" {
				return err.ToError()
			}
			for _, staRes := range result {
				copysetid2Status[copysetid] = staRes.Status
			}
		}

		for _, status := range copysetid2Status {
			total++
			if status.String() == "COPYSET_OP_STATUS_SUCCESS" {
				healthy++
			} else {
				unhelthy++
			}
		}

	}
	row := make(map[string]string)
	row[cobrautil.ROW_SERVER] = fmt.Sprintf("%d", pCmd.ServerID)
	row[cobrautil.ROW_TOTAL] = fmt.Sprintf("%d", total)
	row[cobrautil.ROW_IP] = fmt.Sprintf("%s", ip_out)
	row[cobrautil.ROW_UNHEALTHY_COPYSET_COUNT] = fmt.Sprintf("%d", unhelthy)
	row[cobrautil.ROW_UNHEALTHY_COPYSET_RATIO] = fmt.Sprintf("%v", unhelthy/total)

	list := cobrautil.Map2List(row, pCmd.Header)
	pCmd.TableNew.Append(list)
	config.AddFormatFlag(pCmd.Cmd)
	return nil
}
