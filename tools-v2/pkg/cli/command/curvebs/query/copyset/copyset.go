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
* Created Date: 2023-04-26
* Author: chengyi01
 */

package copyset

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

type CopysetCommand struct {
	CopysetInfoList []*common.CopysetInfo
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

func NewQueryCopysetCommand() *CopysetCommand {
	copysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func NewCopysetCommand() *cobra.Command {
	return NewQueryCopysetCommand().Cmd
}

func (cCmd *CopysetCommand) AddFlags() {
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	logicalpoolidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	copysetidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_COPYSET_ID)
	if len(logicalpoolidList) != len(copysetidList) {
		return fmt.Errorf("logicalpoolidList and copysetidList length not equal")
	}
	logicalpoolIds, errParse := cobrautil.StringList2Uint64List(logicalpoolidList)
	if errParse != nil {
		return fmt.Errorf("parse logicalpoolid%v fail", logicalpoolidList)
	}
	copysetIds, errParse := cobrautil.StringList2Uint64List(copysetidList)
	if errParse != nil {
		return fmt.Errorf("parse copysetid%v fail", copysetidList)
	}

	copysetInfoList, err := copyset.GetCopySetsInCluster(cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	copysetMap := make(map[string]*common.CopysetInfo)
	for _, copysetInfo := range copysetInfoList {
		copysetIdentity := formatCopysetId(*copysetInfo.LogicalPoolId, *copysetInfo.CopysetId)
		copysetMap[copysetIdentity] = copysetInfo
	}

	for i, logicalPool := range logicalpoolIds {
		lpId := uint32(logicalPool)
		cpId := uint32(copysetIds[i])
		copysetIdentity := formatCopysetId(lpId, cpId)
		if _, ok := copysetMap[copysetIdentity]; !ok {
			return fmt.Errorf("get copyset(id: %d,logicalPoolid: %d) info from cluster fail", cpId, lpId)
		}
		cCmd.CopysetInfoList = append(cCmd.CopysetInfoList, copysetMap[copysetIdentity])
	}
	cCmd.Result = cCmd.CopysetInfoList
	cCmd.Error = cmderror.Success()

	return nil
}

func formatCopysetId(logicalPoolId uint32, copysetId uint32) string {
	return fmt.Sprintf("%d:%d", logicalPoolId, copysetId)
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

func GetCopyset(caller *cobra.Command) ([]*common.CopysetInfo, *cmderror.CmdError) {
	getCmd := NewQueryCopysetCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.CURVEBS_MDSADDR, config.RPCRETRYTIMES, config.RPCTIMEOUT,
		config.CURVEBS_LOGIC_POOL_ID, config.CURVEBS_COPYSET_ID,
	})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetScanStatus()
		retErr.Format(err.Error())
		return getCmd.CopysetInfoList, retErr
	}
	return getCmd.CopysetInfoList, cmderror.Success()
}
