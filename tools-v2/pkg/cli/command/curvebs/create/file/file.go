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
* Created Date: 2023-04-14
* Author: chengyi01
 */
package file

import (
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	dirExample = `$ curve bs create file --path /test`
	TYOE_DIR
)

type fileCommand struct {
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*fileCommand)(nil)


func (fCmd *fileCommand) Init(cmd *cobra.Command, args []string) error {
	config.AddBsFileTypeRequiredFlag(fCmd.Cmd)
	fCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_TYPE),
		fmt.Sprint(cobrautil.TYPE_FILE),
	})
	fCmd.SetHeader([]string{cobrautil.ROW_RESULT})
	return nil
}

func (fCmd *fileCommand) RunCommand(cmd *cobra.Command, args []string) error {
	fCmd.Result, fCmd.Error = CreateFileOrDirectory(fCmd.Cmd)
	if fCmd.Error.TypeCode() != cmderror.CODE_SUCCESS {
		return fCmd.Error.ToError()
	}
	fCmd.TableNew.Append([]string{fCmd.Error.Message})
	return nil
}

func (fCmd *fileCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *fileCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}

func (fCmd *fileCommand) AddFlags() {
	config.AddBsMdsFlagOption(fCmd.Cmd)
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddBsPathRequiredFlag(fCmd.Cmd)
	config.AddBsUserOptionFlag(fCmd.Cmd)
	config.AddBsPasswordOptionFlag(fCmd.Cmd)
	config.AddBsSizeRequiredFlag(fCmd.Cmd)
	config.AddBsStripeUnitOptionFlag(fCmd.Cmd)
	config.AddBsStripeCountOptionFlag(fCmd.Cmd)
}

func NewCreateFileCommand() *fileCommand {
	fCmd := &fileCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "file",
			Short:   "create page file in curvebs cluster",
			Example: dirExample,
		},
	}
	basecmd.NewFinalCurveCli(&fCmd.FinalCurveCmd, fCmd)
	return fCmd
}

func NewFileCommand() *cobra.Command {
	return NewCreateFileCommand().Cmd
}
