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
 * Created Date: 2023-04-28
 * Author: Xinlong-Chen
 */

package copyset

import (
	"fmt"

	"github.com/spf13/cobra"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
)

const (
	updateExample = `$ curve bs snapshot copyset 127.0.0.0:8200:0 --logicalpoolid=1 --copysetid=1
$ curve bs snapshot copyset --all`
)

type SnapshotCopysetCommand struct {
	basecmd.FinalCurveCmd

	needAll bool
}

var _ basecmd.FinalCurveCmdFunc = (*SnapshotCopysetCommand)(nil) // check interface

// NewCommand ...
func NewSnapshotCopysetCommand() *cobra.Command {
	sCmd := &SnapshotCopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "copyset",
			Short:   "take snapshot for copyset",
			Example: updateExample,
		},
	}
	basecmd.NewFinalCurveCli(&sCmd.FinalCurveCmd, sCmd)
	return sCmd.Cmd
}

func (sCmd *SnapshotCopysetCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)

	config.AddBsLogicalPoolIdOptionFlag(sCmd.Cmd)
	config.AddBsCopysetIdOptionFlag(sCmd.Cmd)
	config.AddBsAllOptionFlag(sCmd.Cmd)
}

func (sCmd *SnapshotCopysetCommand) Init(cmd *cobra.Command, args []string) error {
	sCmd.needAll = config.GetBsFlagBool(sCmd.Cmd, config.CURVEBS_ALL)

	if !sCmd.needAll {
		isLogicalPoolIDChanged := config.GetBsFlagChanged(sCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
		isCopysetIDChanged := config.GetBsFlagChanged(sCmd.Cmd, config.CURVEBS_COPYSET_ID)
		if !isLogicalPoolIDChanged || !isCopysetIDChanged {
			return fmt.Errorf("all or logicalpoolid and copysetid is required")
		}

		sCmd.SetHeader([]string{cobrautil.ROW_PEER, cobrautil.ROW_COPYSET, cobrautil.ROW_RESULT})
		sCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
			sCmd.Header, []string{},
		))
	} else {
		sCmd.SetHeader([]string{cobrautil.ROW_CHUNKSERVER, cobrautil.ROW_RESULT})
		sCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
			sCmd.Header, []string{cobrautil.ROW_RESULT},
		))
	}

	return nil
}

func (sCmd *SnapshotCopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SnapshotCopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	if sCmd.needAll {
		csAddrs, res, err := GetAllSnapshotResult(sCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}

		rows := make([]map[string]string, len(csAddrs))
		for i := 0; i < len(csAddrs); i++ {
			rows[i] = make(map[string]string)
			rows[i][cobrautil.ROW_CHUNKSERVER] = csAddrs[i]
			if res[i] {
				rows[i][cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS
			} else {
				rows[i][cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_FAILED
			}
		}

		list := cobrautil.ListMap2ListSortByKeys(rows, sCmd.Header, []string{cobrautil.ROW_RESULT})
		sCmd.TableNew.AppendBulk(list)
		sCmd.Result = rows
	} else {
		peer, copyset, err := GetOneSnapshotResult(sCmd.Cmd, args)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}

		row := make(map[string]string)
		row[cobrautil.ROW_PEER] = peer
		row[cobrautil.ROW_COPYSET] = copyset
		row[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS

		list := cobrautil.Map2List(row, sCmd.Header)
		sCmd.TableNew.Append(list)
		sCmd.Result = row
	}

	return nil
}

func (sCmd *SnapshotCopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}
