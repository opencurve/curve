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
 * Created Date: 2022-06-25
 * Author: chengyi (Cyber-SiKu)
 */

package copyset

import (
	"fmt"
	"strings"

	"github.com/olekukonko/tablewriter"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	checkCopyset "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/check/copyset"
	listCopyset "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/list/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

type CopysetCommand struct {
	basecmd.FinalCurveCmd
	health curveutil.ClUSTER_HEALTH_STATUS
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

const (
	copysetExample = `$ curve fs status copyset`
)

func NewCopysetCommand() *cobra.Command {
	return NewStatusCopysetCommand().Cmd
}

func (cCmd *CopysetCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddFsMdsAddrFlag(cCmd.Cmd)
	config.AddMarginOptionFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	cCmd.health = curveutil.HEALTH_ERROR
	response, err := listCopyset.GetCopysetsInfos(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		cCmd.Error = err
		return fmt.Errorf(err.Message)
	}
	var copysetIdVec []string
	var poolIdVec []string
	for _, v := range response.GetCopysetValues() {
		info := v.GetCopysetInfo()
		copysetIdVec = append(copysetIdVec, fmt.Sprintf("%d", info.GetCopysetId()))
		poolIdVec = append(poolIdVec, fmt.Sprintf("%d", info.GetPoolId()))
	}
	if len(copysetIdVec) == 0 {
		var err error
		cCmd.Error = cmderror.ErrSuccess()
		cCmd.Result = "No copyset found"
		cCmd.health = curveutil.HEALTH_OK
		return err
	}
	copysetIds := strings.Join(copysetIdVec, ",")
	poolIds := strings.Join(poolIdVec, ",")
	result, table, errCheck, health := checkCopyset.GetCopysetsStatus(cCmd.Cmd, copysetIds, poolIds)
	cCmd.Result = result
	cCmd.TableNew = table
	cCmd.Error = errCheck
	cCmd.health = health
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
			Use:     "copyset",
			Short:   "status all copyset of the curvefs",
			Example: copysetExample,
		},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func GetCopysetStatus(caller *cobra.Command) (*interface{}, *tablewriter.Table, *cmderror.CmdError, curveutil.ClUSTER_HEALTH_STATUS) {
	copysetCmd := NewStatusCopysetCommand()
	copysetCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, copysetCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEFS_MDSADDR,
	})
	copysetCmd.Cmd.SilenceErrors = true
	copysetCmd.Cmd.Execute()
	return &copysetCmd.Result, copysetCmd.TableNew, copysetCmd.Error, copysetCmd.health
}
