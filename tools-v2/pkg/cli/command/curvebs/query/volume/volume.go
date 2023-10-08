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
* Created Date: 2023-10-08
* Author: baytan0720
 */

package volume

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	clone_recover "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/volume/clone-recover"
	"github.com/spf13/cobra"
)

type RecoverCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*RecoverCommand)(nil) // check interface

func (dCmd *RecoverCommand) AddSubCommands() {
	dCmd.Cmd.AddCommand(
		clone_recover.NewCommand(),
	)
}

func NewVolumeCommand() *cobra.Command {
	dCmd := &RecoverCommand{
		basecmd.MidCurveCmd{
			Use:   "volume",
			Short: "query resources in the curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&dCmd.MidCurveCmd, dCmd)
}
