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
 * Created Date: 2023-11-02
 * Author: ZackSoul
 */
package stop

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	snapshot "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/stop/snapshot"
	"github.com/spf13/cobra"
)

type StopCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*StopCommand)(nil)

func (sCmd *StopCommand) AddSubCommands() {
	sCmd.Cmd.AddCommand(
		snapshot.NewSnapShotCommand(),
	)
}

func NewStopCommand() *cobra.Command {
	sCmd := &StopCommand{
		basecmd.MidCurveCmd{
			Use:   "stop",
			Short: "stop resources in the curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&sCmd.MidCurveCmd, sCmd)
}
