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
 * Created Date: 2023-11-15
 * Author: CrystalAnalyst
 */
package recover

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	volume "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/recover/volume"
	"github.com/spf13/cobra"
)

type RecoverCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*RecoverCommand)(nil)

func (rCmd *RecoverCommand) AddSubCommands() {
	rCmd.Cmd.AddCommand(
		volume.NewVolumeCommand(),
	)
}

func NewRecoverCommand() *cobra.Command {
	rCmd := &RecoverCommand{
		basecmd.MidCurveCmd{
			Use:   "recover",
			Short: "recover resources in curvebs cluster",
		},
	}
	return basecmd.NewMidCurveCli(&rCmd.MidCurveCmd, rCmd)
}
