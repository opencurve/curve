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
 * Created Date: 2023-07-19
 * Author: lng2020
 */

package chunkserver

import (
	"fmt"
	"strconv"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check/copyset"
	listchunkserver "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	chunkserverExample = `$ curve bs check chunkserver --chunkserverid=1`
)

type ChunkserverCommand struct {
	basecmd.FinalCurveCmd
	chunkserverid uint32
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkserverCommand)(nil) // check interface

func NewChunkserverCommand() *cobra.Command {
	return NewCheckChunkserverCommand().Cmd
}

func NewCheckChunkserverCommand() *ChunkserverCommand {
	chunkserverCmd := &ChunkserverCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "chunkserver",
			Short:   "check chunkserver health in curvebs",
			Example: chunkserverExample,
		},
	}
	basecmd.NewFinalCurveCli(&chunkserverCmd.FinalCurveCmd, chunkserverCmd)
	return chunkserverCmd
}

func (cCmd *ChunkserverCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddBsUserOptionFlag(cCmd.Cmd)
	config.AddBsPasswordOptionFlag(cCmd.Cmd)

	config.AddBsMarginOptionFlag(cCmd.Cmd)
	config.AddBsChunkServerIdFlag(cCmd.Cmd)
}

func (cCmd *ChunkserverCommand) Init(cmd *cobra.Command, args []string) error {
	strid, e := strconv.Atoi(config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_CHUNKSERVER_ID))
	if e != nil {
		return e
	}
	cCmd.chunkserverid = uint32(strid)
	header := []string{cobrautil.ROW_CHUNKSERVER, cobrautil.ROW_HEALTHY_COUNT, cobrautil.ROW_UNHEALTHY_COUNT, cobrautil.ROW_UNHEALTHY_RATIO}
	cCmd.SetHeader(header)
	return nil
}

func (cCmd *ChunkserverCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *ChunkserverCommand) RunCommand(cmd *cobra.Command, args []string) error {
	row := make(map[string]string, 0)
	var err *cmderror.CmdError
	healthyCount := 0
	unhealthyCount := 0
	// get copysetid and logicalpoolid

	var copysetidList []string
	var logicalpoolidList []string
	resp, err := listchunkserver.GetCopySetsInChunkServer(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		cCmd.Error = err
		cCmd.Result = cobrautil.ROW_VALUE_FAILED
		return err.ToError()
	}
	for _, copyset := range resp {
		copysetidList = append(copysetidList, strconv.FormatUint(uint64(copyset.GetCopysetId()), 10))
		logicalpoolidList = append(logicalpoolidList, strconv.FormatUint(uint64(copyset.GetLogicalPoolId()), 10))
	}

	// check copyset
	config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
	cCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), strings.Join(copysetidList, ","),
		fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), strings.Join(logicalpoolidList, ","),
	})
	response, err := copyset.CheckCopysets(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		cCmd.Error = err
		cCmd.Result = cobrautil.ROW_VALUE_FAILED
		return err.ToError()
	}
	for _, health := range response {
		if health == cobrautil.HEALTH_OK {
			healthyCount++
		} else {
			unhealthyCount++
		}
	}
	row[cobrautil.ROW_CHUNKSERVER] = fmt.Sprint(cCmd.chunkserverid)
	row[cobrautil.ROW_HEALTHY_COUNT] = fmt.Sprint(healthyCount)
	row[cobrautil.ROW_UNHEALTHY_COUNT] = fmt.Sprint(unhealthyCount)
	row[cobrautil.ROW_UNHEALTHY_RATIO] = fmt.Sprintf("%.2f%%", float64(unhealthyCount)/float64(healthyCount+unhealthyCount)*100)

	cCmd.Result = row
	cCmd.Error = cmderror.ErrSuccess()
	list := cobrautil.Map2List(row, []string{cobrautil.ROW_CHUNKSERVER})
	cCmd.TableNew.Append(list)
	return nil
}

func (cCmd *ChunkserverCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}
