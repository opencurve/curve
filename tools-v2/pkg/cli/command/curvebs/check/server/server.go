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
	"fmt"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"

	copysetbs "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/copyset"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
)

const (
	serverExample = `$ curve bs check server`
)

type ServerCommand struct {
	basecmd.FinalCurveCmd
	ServerID uint32
	ServerIP string
	Port     uint32
}

var _ basecmd.FinalCurveCmdFunc = (*ServerCommand)(nil)

func NewServerCommand() *cobra.Command {
	return NewCheckServerCommand().Cmd
}

func (sCmd *ServerCommand) AddFlags() {
	config.AddBsMdsFlagOption(sCmd.Cmd)
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)
	config.AddBsServerIdOptionFlag(sCmd.Cmd)
	config.AddBsIpOptionFlag(sCmd.Cmd)
	config.AddBsPortOptionFlag(sCmd.Cmd)
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

func (sCmd *ServerCommand) Init(cmd *cobra.Command, args []string) error {

	serverID := config.GetBsFlagUint32(sCmd.Cmd, config.CURVEBS_SERVER_ID)
	serverIP := config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_IP)
	port := config.GetBsFlagUint32(sCmd.Cmd, config.CURVEBS_PORT)
	sCmd.ServerID = serverID
	sCmd.ServerIP = serverIP
	sCmd.Port = port

	header := []string{cobrautil.ROW_SERVER, cobrautil.ROW_IP, cobrautil.ROW_TOTAL,
		cobrautil.ROW_UNHEALTHY_COPYSET,
	}
	sCmd.SetHeader(header)
	return nil
}

func (sCmd *ServerCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *ServerCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func (sCmd *ServerCommand) RunCommand(cmd *cobra.Command, args []string) error {
	// 1 send RPCs to get all ChunkServerInfo on server
	timeout := config.GetFlagDuration(sCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(sCmd.Cmd, config.RPCRETRYTIMES)
	mdsAddr := config.GetBsFlagString(cmd, config.CURVEBS_MDSADDR)
	copysetid2Status := make(map[uint32]*copyset.COPYSET_OP_STATUS)
	ip_out := sCmd.ServerIP

	total := 0
	healthy := 0
	unhelthy := 0

	chunkServerInfos, err := GetChunkserverInfos(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	if len(chunkServerInfos) != 0 {
		ip_out = chunkServerInfos[0].GetHostIp()
	}

	for _, item := range chunkServerInfos {
		err := sCmd.GetStatus(item, &timeout, uint32(retrytimes), mdsAddr, &copysetid2Status)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			sCmd.Error = err
			return err.ToError()
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
	row[cobrautil.ROW_SERVER] = fmt.Sprintf("%d", sCmd.ServerID)
	row[cobrautil.ROW_TOTAL] = fmt.Sprintf("%d", total)
	row[cobrautil.ROW_IP] = ip_out
	if total == 0 {
		row[cobrautil.ROW_UNHEALTHY_COPYSET] = fmt.Sprintf("%d(%d%%)", 0, 0)
	} else {
		row[cobrautil.ROW_UNHEALTHY_COPYSET] = fmt.Sprintf("%d(%v%%)", unhelthy, (unhelthy/total)*100)
	}

	list := cobrautil.Map2List(row, sCmd.Header)
	sCmd.TableNew.Append(list)
	return nil
}

func (sCmd *ServerCommand) GetStatus(item *topology.ChunkServerInfo, timeout *time.Duration,
	retrytimes uint32, mdsAddrs string, copysetid2Status *map[uint32]*copyset.COPYSET_OP_STATUS) *cmderror.CmdError {
	chunkServerID := item.GetChunkServerID()

	hostip := item.GetHostIp()
	port := item.GetPort()
	sCmd.Cmd.ResetFlags()
	config.AddBsMdsFlagOption(sCmd.Cmd)
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)
	config.AddBsChunkServerIDOptionFlag(sCmd.Cmd)
	config.AddBsIpOptionFlag(sCmd.Cmd)
	config.AddBsPortOptionFlag(sCmd.Cmd)

	sCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_MDSADDR), mdsAddrs,
		fmt.Sprintf("--%s", config.RPCRETRYTIMES), fmt.Sprintf("%d", retrytimes),
		fmt.Sprintf("--%s", config.RPCTIMEOUT), timeout.String(),
		fmt.Sprintf("--%s", config.CURVEBS_CHUNKSERVER_ID), fmt.Sprintf("%d", chunkServerID),
		fmt.Sprintf("--%s", config.VIPER_CURVEBS_SERVER_ID), hostip,
		fmt.Sprintf("--%s", config.CURVEBS_PORT), fmt.Sprintf("%d", port),
	})

	copysetids2poolids, err := GetCopysetids(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}

	// get copyset status
	peerAddr := fmt.Sprintf("%s:%d", hostip, port)

	for copysetid, poolid := range *copysetids2poolids {
		sCmd.Cmd.ResetFlags()
		config.AddBsMdsFlagOption(sCmd.Cmd)
		config.AddRpcRetryTimesFlag(sCmd.Cmd)
		config.AddRpcTimeoutFlag(sCmd.Cmd)
		config.AddBSCopysetIdRequiredFlag(sCmd.Cmd)
		config.AddBSLogicalPoolIdRequiredFlag(sCmd.Cmd)
		config.AddBSPeersConfFlag(sCmd.Cmd)
		sCmd.Cmd.ParseFlags([]string{
			fmt.Sprintf("--%s", config.CURVEBS_MDSADDR), mdsAddrs,
			fmt.Sprintf("--%s", config.RPCRETRYTIMES), fmt.Sprintf("%d", retrytimes),
			fmt.Sprintf("--%s", config.RPCTIMEOUT), timeout.String(),
			fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), fmt.Sprintf("%d", copysetid),
			fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), fmt.Sprintf("%d", poolid),
			fmt.Sprintf("--%s", config.CURVEBS_PEERS_ADDRESS), peerAddr,
		})

		result, err := copysetbs.GetCopysetStatus(sCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		for _, staRes := range *result {
			(*copysetid2Status)[copysetid] = staRes.Status
		}
	}
	return cmderror.Success()
}
