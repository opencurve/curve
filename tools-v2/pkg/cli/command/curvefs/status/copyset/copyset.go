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

	"github.com/liushuochen/gotable/table"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	checkCopyset "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/check/copyset"
	listCopyset "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/list/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

type CopysetCommand struct {
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

func NewCopysetCommand() *cobra.Command {
	cCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "copyset",
			Short: "status all copyset of the curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd.Cmd
}

func (cCmd *CopysetCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddFsMdsAddrFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	response, err := listCopyset.GetCopysetsInfos(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(err.Message)
	}
	var copysetIdVec []string
	var poolIdVec []string
	for _, v := range response.GetCopysetValues() {
		info := v.GetCopysetInfo()
		copysetIdVec = append(copysetIdVec, fmt.Sprintf("%d", info.GetCopysetId()))
		poolIdVec = append(poolIdVec, fmt.Sprintf("%d", info.GetPoolId()))
	}
	copysetIds := strings.Join(copysetIdVec, ",")
	poolIds := strings.Join(poolIdVec, ",")
	result, table, errCheck := checkCopyset.GetCopysetsStatus(cCmd.Cmd, copysetIds, poolIds)
	cCmd.Result = result
	cCmd.Table = table
	cCmd.Error = errCheck
	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	return nil
}

func (cCmd *CopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd, cCmd)
}

func NewStatusCopysetCommand() *CopysetCommand {
	copysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "etcd",
			Short: "get the etcd status of curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func GetCopysetStatus(caller *cobra.Command) (*interface{}, *table.Table, *cmderror.CmdError) {
	copysetCmd := NewStatusCopysetCommand()
	copysetCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	cobrautil.AlignFlags(caller, copysetCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEFS_MDSADDR,
	})
	copysetCmd.Cmd.SilenceUsage = true
	copysetCmd.Cmd.Execute()
	return &copysetCmd.Result, copysetCmd.Table, copysetCmd.Error
}
