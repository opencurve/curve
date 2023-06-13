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
* Created Date: 2023-05-11
* Author: chengyi01
 */
package chunkserver

import (
	"fmt"

	"github.com/olekukonko/tablewriter"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	listchunkserver "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
)

type ChunkServerCommand struct {
	basecmd.FinalCurveCmd

	ChunkServerInfos []*topology.ChunkServerInfo
	RecoverStatusMap map[uint32]bool
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkServerCommand)(nil)

func (csCmd *ChunkServerCommand) Init(cmd *cobra.Command, args []string) error {
	recoverMap, err := chunkserver.GetQueryChunkServerRecoverStatus(csCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	csCmd.RecoverStatusMap = recoverMap

	csInfos, err := listchunkserver.GetChunkServerInCluster(csCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	csCmd.ChunkServerInfos = csInfos
	header := []string{
		cobrautil.ROW_EXTERNAL_ADDR, cobrautil.ROW_INTERNAL_ADDR, cobrautil.ROW_VERSION,
		cobrautil.ROW_STATUS, cobrautil.ROW_RECOVERING,
	}
	csCmd.SetHeader(header)
	csCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		csCmd.Header, []string{cobrautil.ROW_STATUS, cobrautil.ROW_RECOVERING, cobrautil.ROW_VERSION},
	))
	return nil
}

func (csCmd *ChunkServerCommand) RunCommand(cmd *cobra.Command, args []string) error {
	rows := make([]map[string]string, 0)
	for _, csInfo := range csCmd.ChunkServerInfos {
		row := make(map[string]string)
		row[cobrautil.ROW_EXTERNAL_ADDR] = fmt.Sprintf("%s:%d", csInfo.GetExternalIp(), csInfo.GetPort())
		row[cobrautil.ROW_INTERNAL_ADDR] = fmt.Sprintf("%s:%d", csInfo.GetHostIp(), csInfo.GetPort())
		row[cobrautil.ROW_VERSION] = csInfo.GetVersion()
		state := csInfo.GetOnlineState()
		if state == topology.OnlineState_ONLINE {
			row[cobrautil.ROW_STATUS] = cobrautil.ROW_VALUE_ONLINE
		} else {
			row[cobrautil.ROW_STATUS] = cobrautil.ROW_VALUE_OFFLINE
		}
		row[cobrautil.ROW_RECOVERING] = fmt.Sprintf("%v", csCmd.RecoverStatusMap[csInfo.GetChunkServerID()])
		rows = append(rows, row)
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, csCmd.Header, []string{
		cobrautil.ROW_STATUS, cobrautil.ROW_RECOVERING, cobrautil.ROW_VERSION,
	})
	csCmd.TableNew.AppendBulk(list)
	csCmd.Result = rows
	csCmd.Error = cmderror.Success()
	return nil
}

func (csCmd *ChunkServerCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&csCmd.FinalCurveCmd, csCmd)
}

func (csCmd *ChunkServerCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&csCmd.FinalCurveCmd)
}

func (csCmd *ChunkServerCommand) AddFlags() {
	config.AddRpcTimeoutFlag(csCmd.Cmd)
	config.AddRpcRetryTimesFlag(csCmd.Cmd)

	config.AddBsMdsFlagOption(csCmd.Cmd)
}

func NewStatusChunkServerCommand() *ChunkServerCommand {
	csCmd := &ChunkServerCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "chunkserver",
			Short: "get status of chunkserver",
		},
	}
	basecmd.NewFinalCurveCli(&csCmd.FinalCurveCmd, csCmd)
	return csCmd
}

func NewChunkServerCommand() *cobra.Command {
	return NewStatusChunkServerCommand().Cmd
}

func GetChunkserverStatus(caller *cobra.Command) (*interface{}, *tablewriter.Table, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS) {
	csCmd := NewStatusChunkServerCommand()
	csCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, csCmd.Cmd, []string{config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR})
	csCmd.Cmd.SilenceErrors = true
	err := csCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetChunkServerInCluster()
		retErr.Format(err.Error())
		return nil, nil, retErr, cobrautil.HEALTH_ERROR
	}
	return &csCmd.Result, csCmd.TableNew, cmderror.Success(), cobrautil.HEALTH_OK
}
