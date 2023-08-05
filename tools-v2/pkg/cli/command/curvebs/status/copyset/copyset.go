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
* Created Date: 2023-05-18
* Author: chengyi01
 */

package copyset

import (
	"fmt"
	"sort"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	checkcopyset "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check/copyset"
	clustercopyset "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

type CopysetCommand struct {
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

func NewCopysetCommand() *cobra.Command {
	return NewStatusCopysetCommand().Cmd
}

func (cCmd *CopysetCommand) AddFlags() {
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddBsMarginOptionFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	header := []string{cobrautil.ROW_COPYSET_KEY, cobrautil.ROW_COPYSET_ID,
		cobrautil.ROW_POOL_ID, cobrautil.ROW_STATUS, cobrautil.ROW_LOG_GAP,
		cobrautil.ROW_EXPLAIN,
	}
	cCmd.SetHeader(header)
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		cCmd.Header, []string{cobrautil.ROW_COPYSET_ID, cobrautil.ROW_POOL_ID,
			cobrautil.ROW_STATUS,
		},
	))

	config.AddBsFilterOptionFlag(cCmd.Cmd)
	cCmd.Cmd.ParseFlags([]string{fmt.Sprintf("--%s=%s", config.CURVEBS_FIlTER, cobrautil.FALSE_STRING)})
	copysetsInfo, err := clustercopyset.GetCopySetsInCluster(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	var copysetIds []string
	var poolIds []string
	for _, info := range copysetsInfo {
		copysetIds = append(copysetIds, fmt.Sprintf("%d", info.GetCopysetId()))
		poolIds = append(poolIds, fmt.Sprintf("%d", info.GetLogicalPoolId()))
	}
	config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
	cCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), strings.Join(copysetIds, ","),
		fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), strings.Join(poolIds, ","),
	})

	results, err := checkcopyset.GetCopysetsStatus(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	cCmd.Error = err
	if results != nil && *results != nil {
		cCmd.Result = *results
		rows := (*results).([]map[string]string)
		sort.Slice(rows, func(i, j int) bool {
			return rows[i][cobrautil.ROW_COPYSET_KEY] < rows[j][cobrautil.ROW_COPYSET_KEY]
		})
		list := cobrautil.ListMap2ListSortByKeys(rows, cCmd.Header, []string{
			cobrautil.ROW_POOL_ID, cobrautil.ROW_STATUS, cobrautil.ROW_COPYSET_ID,
		})
		cCmd.TableNew.AppendBulk(list)
	}
	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	return nil
}

func (cCmd *CopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func NewStatusCopysetCommand() *CopysetCommand {
	copysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "copyset",
			Short: "get status of copysets in cluster",
		},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func GetCopysetsStatus(caller *cobra.Command) (*interface{}, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS) {
	cCmd := NewStatusCopysetCommand()
	cCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, cCmd.Cmd, []string{config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR, config.CURVEBS_MARGIN})
	cCmd.Cmd.SilenceErrors = true
	err := cCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetCopysetStatus()
		retErr.Format(err.Error())
		return nil, retErr, cobrautil.HEALTH_ERROR
	}
	return &cCmd.Result, cmderror.Success(), cobrautil.HEALTH_OK
}
