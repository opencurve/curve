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
 * Created Date: 2022-06-22
 * Author: chengyi (Cyber-SiKu)
 */

package copyset

import (
	"fmt"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/query/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	ROW_COPYSETKEY = "copyset key"
	ROW_STATUS     = "status"
	ROW_EXPLAIN    = "explain"
)

type CopysetCommand struct {
	basecmd.FinalCurveCmd
	key2Copyset *map[uint64]*cobrautil.CopysetInfoStatus
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

func NewCopysetCommand() *cobra.Command {
	copysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "copyset",
			Short: "check copysets health in curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd.Cmd
}

func (cCmd *CopysetCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddFsMdsAddrFlag(cCmd.Cmd)
	config.AddCopysetidSliceRequiredFlag(cCmd.Cmd)
	config.AddPoolidSliceRequiredFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	var queryCopysetErr *cmderror.CmdError
	cCmd.key2Copyset, queryCopysetErr = copyset.QueryCopysetInfoStatus(cCmd.Cmd)
	if queryCopysetErr.TypeCode() != cmderror.CODE_SUCCESS {
		return queryCopysetErr.ToError()
	}
	cCmd.Error = queryCopysetErr
	table, err := gotable.Create(ROW_COPYSETKEY, ROW_STATUS, ROW_EXPLAIN)
	if err != nil {
		return err
	}
	cCmd.Table = table
	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	rows := make([]map[string]string, 0)
	var errs []*cmderror.CmdError
	for k, v := range *cCmd.key2Copyset {
		row := make(map[string]string)
		row[ROW_COPYSETKEY] = fmt.Sprintf("%d", k)
		if v == nil {
			row[ROW_STATUS] = cobrautil.CopysetHealthStatus_Str[int32(cobrautil.COPYSET_NOTEXIST)]
		} else {
			status, errs := cobrautil.CheckCopySetHealth(v)
			row[ROW_STATUS] = cobrautil.CopysetHealthStatus_Str[int32(status)]
			if status != cobrautil.COPYSET_OK {
				explain := "|"
				for _, e := range errs {
					explain += fmt.Sprintf("%s|", e.Message)
				}
				row[ROW_EXPLAIN] = explain

			}
		}
		rows = append(rows, row)
	}
	retErr := cmderror.MergeCmdError(errs)
	cCmd.Error = &retErr
	cCmd.Table.AddRows(rows)
	var err error
	cCmd.Result, err = cobrautil.TableToResult(cCmd.Table)
	return err
}

func (cCmd *CopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd, cCmd)
}
