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
package dir

import (
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/create/file"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	dirExample = `$ curve bs create dir --path /test`
)

type DirectoryCommand struct {
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*DirectoryCommand)(nil)


func (dCmd *DirectoryCommand) Init(cmd *cobra.Command, args []string) error {
	config.AddBsFileTypeRequiredFlag(dCmd.Cmd)
	dCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_TYPE),
		fmt.Sprint(cobrautil.TYPE_DIR),
	})
	dCmd.SetHeader([]string{cobrautil.ROW_RESULT})
	return nil
}

func (dCmd *DirectoryCommand) RunCommand(cmd *cobra.Command, args []string) error {
	dCmd.Result, dCmd.Error = file.CreateFileOrDirectory(dCmd.Cmd)
	if dCmd.Error.TypeCode() != cmderror.CODE_SUCCESS {
		return dCmd.Error.ToError()
	}
	dCmd.TableNew.Append([]string{dCmd.Error.Message})
	return nil
}

func (dCmd *DirectoryCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&dCmd.FinalCurveCmd, dCmd)
}

func (dCmd *DirectoryCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&dCmd.FinalCurveCmd)
}

func (dCmd *DirectoryCommand) AddFlags() {
	config.AddBsMdsFlagOption(dCmd.Cmd)
	config.AddRpcTimeoutFlag(dCmd.Cmd)
	config.AddRpcRetryTimesFlag(dCmd.Cmd)
	config.AddBsUserOptionFlag(dCmd.Cmd)
	config.AddBsPasswordOptionFlag(dCmd.Cmd)
	config.AddBsPathRequiredFlag(dCmd.Cmd)
}

func NewCreateDirectoryCommand() *DirectoryCommand {
	dCmd := &DirectoryCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "dir",
			Short:   "create directory in curvebs cluster",
			Example: dirExample,
		},
	}
	basecmd.NewFinalCurveCli(&dCmd.FinalCurveCmd, dCmd)
	return dCmd
}

func NewDirectoryCommand() *cobra.Command {
	return NewCreateDirectoryCommand().Cmd
}
