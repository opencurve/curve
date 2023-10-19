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

package update

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/update/fs"
	"github.com/spf13/cobra"
)

type UpdateCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*UpdateCommand)(nil) // check interface

func (updateCmd *UpdateCommand) AddSubCommands() {
	updateCmd.Cmd.AddCommand(
		fs.NewFsCommand(),
	)
}

func NewUpdateCommand() *cobra.Command {
	updateCmd := &UpdateCommand{
		basecmd.MidCurveCmd{
			Use:   "update",
			Short: "update resources in the curvefs",
		},
	}
	return basecmd.NewMidCurveCli(&updateCmd.MidCurveCmd, updateCmd)
}
