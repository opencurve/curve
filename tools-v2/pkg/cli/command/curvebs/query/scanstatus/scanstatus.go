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
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/spf13/cobra"
)

const (
	scanStatusExample = `$ curve bs query scan-status --copysetid 1 --logicalpoolid 1`
)

type ScanStatusCommand struct {
	CopysetInfoList []*common.CopysetInfo
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*ScanStatusCommand)(nil) // check interface

func NewQueryScanStatusCommand() *ScanStatusCommand {
	scanStatusCmd := &ScanStatusCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "scan-status",
			Short:   "query ScanStatus info in bs",
			Example: scanStatusExample,
		},
	}

	basecmd.NewFinalCurveCli(&scanStatusCmd.FinalCurveCmd, scanStatusCmd)
	return scanStatusCmd
}

func NewScanStatusCommand() *cobra.Command {
	return NewQueryScanStatusCommand().Cmd
}

func (sCmd *ScanStatusCommand) AddFlags() {
	config.AddBsMdsFlagOption(sCmd.Cmd)
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)
	config.AddBsLogicalPoolIdSliceRequiredFlag(sCmd.Cmd)
	config.AddBsCopysetIdSliceRequiredFlag(sCmd.Cmd)
}

func (sCmd *ScanStatusCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs := config.GetBsFlagString(cmd, config.CURVEBS_MDSADDR)
	retrytime := config.GetBsFlagInt32(cmd, config.RPCRETRYTIMES)
	timeout := config.GetFlagDuration(sCmd.Cmd, config.RPCTIMEOUT)
	logicalpoolidList := config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	copysetidList := config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_COPYSET_ID)

	sCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_MDSADDR), mdsAddrs,
		fmt.Sprintf("--%s", config.RPCRETRYTIMES), fmt.Sprintf("%d", retrytime),
		fmt.Sprintf("--%s", config.RPCTIMEOUT), fmt.Sprintf("%d", timeout),
		fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), logicalpoolidList,
		fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), copysetidList,
	})

	header := []string{
		cobrautil.ROW_LOGICALPOOL,
		cobrautil.ROW_COPYSET_ID,
		cobrautil.ROW_SCAN,
		cobrautil.ROW_LASTSCAN,
		cobrautil.ROW_LAST_SCAN_CONSISTENT,
	}
	sCmd.SetHeader(header)
	sCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		sCmd.Header, []string{cobrautil.ROW_COPYSET_ID, cobrautil.ROW_POOL_ID,
			cobrautil.ROW_SCAN,
		},
	))

	return nil
}

func (sCmd *ScanStatusCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *ScanStatusCommand) RunCommand(cmd *cobra.Command, args []string) error {
	copysetInfoList, err := copyset.GetCopyset(cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	sCmd.CopysetInfoList = copysetInfoList
	rows := make([]map[string]string, 0)
	for _, info := range sCmd.CopysetInfoList {
		row := make(map[string]string)
		row[cobrautil.ROW_LOGICALPOOL] = fmt.Sprintf("%d", info.GetLogicalPoolId())
		row[cobrautil.ROW_COPYSET_ID] = fmt.Sprintf("%d", info.GetCopysetId())
		row[cobrautil.ROW_SCAN] = fmt.Sprintf("%v", info.GetScaning())
		row[cobrautil.ROW_LASTSCAN] = fmt.Sprintf("%v", time.Unix(int64(info.GetLastScanSec()), 0))
		row[cobrautil.ROW_LAST_SCAN_CONSISTENT] = fmt.Sprintf("%v", info.GetLastScanConsistent())
		rows = append(rows, row)
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, sCmd.Header, []string{
		cobrautil.ROW_LOGICALPOOL,
		cobrautil.ROW_COPYSET_ID,
	})
	sCmd.TableNew.AppendBulk(list)
	sCmd.Error = cmderror.Success()
	return nil
}

func (sCmd *ScanStatusCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}
