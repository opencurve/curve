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
* Created Date: 2023-12-11
* Author: wsehjk
 */

package export

import (
 	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
 	target "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/export/target"
 	"github.com/spf13/cobra"
)
type ExportCommand struct {
	basecmd.MidCurveCmd
}
 
var _ basecmd.MidCurveCmdFunc = (*ExportCommand)(nil) // check interface
 
func (eCmd *ExportCommand) AddSubCommands() {
	eCmd.Cmd.AddCommand(
		target.NewTargetCommand(),
	)
}
func NewExportCommand() *cobra.Command{
	eCmd := &ExportCommand {
		basecmd.MidCurveCmd{
			Use: "export",
			Short: "export the target in curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&eCmd.MidCurveCmd, eCmd)
}