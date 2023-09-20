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
	"strconv"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	checkcopyset "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/config"

	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	serverExample = `$ curve bs check server \
$ curve bs check server --serverid=1 \
$ curve bs check server --ip 127.0.0.1 --port 8200`
)

type ServerCommand struct {
	basecmd.FinalCurveCmd

	ServerID   uint32
	ServerIP   string
	ServerPort uint32

	rows []map[string]string
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
	config.AddBsServerIpOptionFlag(sCmd.Cmd)
	config.AddBsServerPortOptionFlag(sCmd.Cmd)
}

func NewCheckServerCommand() *ServerCommand {
	ckCmd := &ServerCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "server",
			Short:   "check all copysets information on server",
			Example: serverExample,
		},
	}

	basecmd.NewFinalCurveCli(&ckCmd.FinalCurveCmd, ckCmd)
	return ckCmd
}

func (sCmd *ServerCommand) Init(cmd *cobra.Command, args []string) error {
	sCmd.ServerID = config.GetBsFlagUint32(sCmd.Cmd, config.CURVEBS_SERVER_ID)
	sCmd.ServerIP = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_SERVER_IP)
	sCmd.ServerPort = config.GetBsFlagUint32(sCmd.Cmd, config.CURVEBS_SERVER_PORT)

	header := []string{cobrautil.ROW_SERVER, cobrautil.ROW_IP, cobrautil.ROW_TOTAL,
		cobrautil.ROW_UNHEALTHY_COPYSET, cobrautil.ROW_UNHEALTHY_COPYSET_RATIO,
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
	chunkServerInfos, err := chunkserver.ListChunkServerInfos(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		sCmd.Error = err
		return err.ToError()
	}
	if len(chunkServerInfos) == 0 {
		return nil
	}

	config.AddBsChunkServerIdFlag(sCmd.Cmd)
	config.AddBsLogicalPoolIdRequiredFlag(sCmd.Cmd)
	config.AddBsCopysetIdRequiredFlag(sCmd.Cmd)
	for _, item := range chunkServerInfos {
		row := make(map[string]string)
		chunkId := item.GetChunkServerID()
		sCmd.Cmd.Flags().Set(config.CURVEBS_CHUNKSERVER_ID, strconv.FormatUint(uint64(chunkId), 10))
		copySets, err := chunkserver.GetCopySetsInChunkServer(sCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}

		total := len(copySets)
		var unhealthy int
		for _, copyset := range copySets {
			logicalPoolId := copyset.GetLogicalPoolId()
			copysetId := copyset.GetCopysetId()

			copysetKey := cobrautil.GetCopysetKey(uint64(logicalPoolId), uint64(copysetId))
			sCmd.Cmd.Flags().Set(config.CURVEBS_COPYSET_ID, strconv.FormatUint(uint64(copysetId), 10))
			sCmd.Cmd.Flags().Set(config.CURVEBS_LOGIC_POOL_ID, strconv.FormatUint(uint64(logicalPoolId), 10))
			copysetKey2Status, err := checkcopyset.CheckCopysets(sCmd.Cmd)
			if err.TypeCode() != cmderror.CODE_SUCCESS {
				return err.ToError()
			}

			if copysetKey2Status[copysetKey] != cobrautil.HEALTH_OK {
				unhealthy++
			}
		}
		row[cobrautil.ROW_SERVER] = strconv.FormatUint(uint64(chunkId), 10)
		row[cobrautil.ROW_IP] = item.GetExternalIp()
		row[cobrautil.ROW_TOTAL] = strconv.FormatUint(uint64(total), 10)
		row[cobrautil.ROW_UNHEALTHY_COPYSET] = strconv.FormatUint(uint64(unhealthy), 10)
		row[cobrautil.ROW_UNHEALTHY_COPYSET_RATIO] = fmt.Sprintf("%v%%", (unhealthy/total)*100)
		sCmd.rows = append(sCmd.rows, row)
		sCmd.Error = err
	}

	list := cobrautil.ListMap2ListSortByKeys(sCmd.rows, sCmd.Header, []string{
		cobrautil.ROW_SERVER,
	})
	sCmd.TableNew.AppendBulk(list)
	sCmd.Result = sCmd.rows
	return nil
}
