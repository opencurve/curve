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
 * Created Date: 2022-05-09
 * Author: chengyi (Cyber-SiKu)
 */

package basecmd

import (
	"os"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	"github.com/spf13/cobra"
)

// FinalCurveCmd is the final executable command,
// it has no subcommands.
// The execution process is Init->RunCommand->Print.
// Error Use to indicate whether the command is wrong
// and the reason for the execution error
type FinalCurveCmd struct {
	Use     string
	Short   string
	Long    string
	Example string // how to use cmd
	Error   cmderror.CmdError
	Result  string // cmd result detail(json string)
	Cmd     *cobra.Command
}

// FinalCurveCmdFunc is the function type for final command
type FinalCurveCmdFunc interface {
	Init(cmd *cobra.Command, args []string) error
	RunCommand(cmd *cobra.Command, args []string) error
	Print(cmd *cobra.Command, args []string) error
	// result in plain format string
	ResultPlainString() (string, error)
}

// MidCurveCmd is the middle command and has subcommands.
// If you execute this command
// you will be prompted which subcommands are included
type MidCurveCmd struct {
	Use   string
	Short string
	Cmd   *cobra.Command
}

// Add subcommand for MidCurveCmd
type MidCurveCmdFunc interface {
	AddSubCommands()
}

func NewFinalCurveCli(cli *FinalCurveCmd, funcs FinalCurveCmdFunc) *cobra.Command {
	return &cobra.Command{
		Use:          cli.Use,
		Short:        cli.Short,
		Long:         cli.Long,
		PreRunE:      funcs.Init,
		RunE:         funcs.RunCommand,
		PostRunE:     funcs.Print,
		SilenceUsage: true,
	}
}

func NewMidCurveCli(cli *MidCurveCmd, add MidCurveCmdFunc) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cli.Use,
		Short: cli.Short,
		Args:  cobrautil.NoArgs,
		RunE:  cobrautil.ShowHelp(os.Stderr),
	}
	add.AddSubCommands()
	return cmd
}
