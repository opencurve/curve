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
* Project: CurveCli
* Created Date: 2023-09-16
* Author: baytan0720
 */

package volume

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/update/volume/flatten"
	"github.com/spf13/cobra"
)

type VolumeCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*VolumeCommand)(nil) // check interface

func (volumeCmd *VolumeCommand) AddSubCommands() {
	volumeCmd.Cmd.AddCommand(
		flatten.NewCommand(),
	)
}

func NewVolumeCommand() *cobra.Command {
	volumeCmd := &VolumeCommand{
		basecmd.MidCurveCmd{
			Use:   "volume",
			Short: "update volume resources in the curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&volumeCmd.MidCurveCmd, volumeCmd)
}
