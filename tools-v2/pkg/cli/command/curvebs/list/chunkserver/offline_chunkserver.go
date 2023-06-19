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
 * Created Date: 2023-06-07
 * Author: baytan0720
 */

package chunkserver

import (
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/chunk"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
)

type OfflineChunkServerCommand struct {
	basecmd.FinalCurveCmd
	csInfos []*topology.ChunkServerInfo
}

var _ basecmd.FinalCurveCmdFunc = (*OfflineChunkServerCommand)(nil) // check interface

func NewOfflineChunkServerCommand() *cobra.Command {
	return NewListOfflineChunkServerCommand().Cmd
}

func NewListOfflineChunkServerCommand() *OfflineChunkServerCommand {
	oCmd := &OfflineChunkServerCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}
	basecmd.NewFinalCurveCli(&oCmd.FinalCurveCmd, oCmd)
	return oCmd
}

func (oCmd *OfflineChunkServerCommand) AddFlags() {
	config.AddBsMdsFlagOption(oCmd.Cmd)
	config.AddRpcRetryTimesFlag(oCmd.Cmd)
	config.AddRpcTimeoutFlag(oCmd.Cmd)
}

func (oCmd *OfflineChunkServerCommand) Init(cmd *cobra.Command, args []string) error {
	// list chunkserver
	chunkserverInfos, errCmd := GetChunkServerInCluster(oCmd.Cmd)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return errCmd.ToError()
	}
	addr2CsInfo := make(map[string]*topology.ChunkServerInfo)

	config.AddBsCopysetIdSliceRequiredFlag(oCmd.Cmd)
	config.AddBsLogicalPoolIdSliceRequiredFlag(oCmd.Cmd)
	config.AddBsChunkIdSliceRequiredFlag(oCmd.Cmd)
	config.AddBsChunkServerAddressSliceRequiredFlag(oCmd.Cmd)
	for i, info := range chunkserverInfos {
		address := fmt.Sprintf("%s:%d", *info.HostIp, *info.Port)
		addr2CsInfo[fmt.Sprintf("%s:%d", *info.HostIp, *info.Port)] = chunkserverInfos[i]
		oCmd.Cmd.ParseFlags([]string{
			fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), "1",
			fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), "1",
			fmt.Sprintf("--%s", config.CURVEBS_CHUNK_ID), "1",
			fmt.Sprintf("--%s", config.CURVEBS_CHUNKSERVER_ADDRESS), address,
		})
	}
	addr2Chunk, errCmd := chunk.GetChunkInfo(oCmd.Cmd)
	for addr, info := range addr2Chunk {
		if info == nil {
			oCmd.csInfos = append(oCmd.csInfos, addr2CsInfo[addr])
		}
	}
	return nil
}

func (oCmd *OfflineChunkServerCommand) RunCommand(cmd *cobra.Command, args []string) error {
	return nil
}

func (oCmd *OfflineChunkServerCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&oCmd.FinalCurveCmd, oCmd)
}

func (oCmd *OfflineChunkServerCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&oCmd.FinalCurveCmd)
}

func ListOfflineChunkServer(caller *cobra.Command) ([]*topology.ChunkServerInfo, *cmderror.CmdError) {
	cCmd := NewListOfflineChunkServerCommand()
	cCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	config.AlignFlagsValue(caller, cCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
	})
	cCmd.Cmd.SilenceErrors = true
	cCmd.Cmd.SilenceUsage = true
	err := cCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsListOfflineChunkServer()
		retErr.Format(err.Error())
		return cCmd.csInfos, retErr
	}
	return cCmd.csInfos, cmderror.Success()
}
