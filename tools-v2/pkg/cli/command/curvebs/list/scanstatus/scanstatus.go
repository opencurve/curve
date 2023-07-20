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
* Project: curve
* Created Date: 2023-05-18
* Author: lianzhanbiao
 */

package scanstatus

import (
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/spf13/cobra"
)

const (
	copysetExample = `$ curve bs list scan-status`
)

type ScanStatusCommand struct {
	CopysetInfoList []*common.CopysetInfo
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*ScanStatusCommand)(nil) // check interface

func NewListScanStatusCommand() *ScanStatusCommand {
	scanStatusCommand := &ScanStatusCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "scan-status",
			Short:   "list curvebs all copyset that scanning is false",
			Example: copysetExample,
		},
	}

	basecmd.NewFinalCurveCli(&scanStatusCommand.FinalCurveCmd, scanStatusCommand)
	return scanStatusCommand
}

func NewScanStatusCommand() *cobra.Command {
	return NewListScanStatusCommand().Cmd
}

func (sCmd *ScanStatusCommand) AddFlags() {
	config.AddRpcTimeoutFlag(sCmd.Cmd)
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddBsMdsFlagOption(sCmd.Cmd)
}

func (sCmd *ScanStatusCommand) Init(cmd *cobra.Command, args []string) error {
	header := []string{
		cobrautil.ROW_LOGICALPOOL,
		cobrautil.ROW_COPYSET_ID,
	}
	sCmd.SetHeader(header)
	sCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		sCmd.Header, []string{cobrautil.ROW_COPYSET_ID, cobrautil.ROW_POOL_ID},
	))
	return nil
}

func (sCmd *ScanStatusCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *ScanStatusCommand) RunCommand(cmd *cobra.Command, args []string) error {
	config.AddBsFilterOptionFlag(sCmd.Cmd)
	sCmd.Cmd.ParseFlags([]string{fmt.Sprintf("--%s=%s", config.CURVEBS_FIlTER, cobrautil.FALSE_STRING)})
	copysetInfoList, err := copyset.GetCopySetsInCluster(cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	sCmd.CopysetInfoList = copysetInfoList
	rows := make([]map[string]string, 0)
	for _, info := range sCmd.CopysetInfoList {
		row := make(map[string]string)
		row[cobrautil.ROW_LOGICALPOOL] = fmt.Sprintf("%d", info.GetLogicalPoolId())
		row[cobrautil.ROW_COPYSET_ID] = fmt.Sprintf("%d", info.GetCopysetId())
		rows = append(rows, row)
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, sCmd.Header, []string{
		cobrautil.ROW_LOGICALPOOL,
		cobrautil.ROW_COPYSET_ID,
	})
	sCmd.TableNew.AppendBulk(list)
	sCmd.Error = cmderror.ErrSuccess()
	return nil
}

func (sCmd *ScanStatusCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}
