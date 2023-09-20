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
 * Created Date: 2023-04-24
 * Author: baytan0720
 */

package check

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check/operator"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check/server"
	"github.com/spf13/cobra"
)

type CheckCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*CheckCommand)(nil) // check interface

func (checkCmd *CheckCommand) AddSubCommands() {
	checkCmd.Cmd.AddCommand(
		copyset.NewCopysetCommand(),
		operator.NewOperatorCommand(),
		server.NewServerCommand(),
		chunkserver.NewChunkserverCommand(),
	)
}

func NewCheckCommand() *cobra.Command {
	checkCmd := &CheckCommand{
		basecmd.MidCurveCmd{
			Use:   "check",
			Short: "checkout the health of resources in curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&checkCmd.MidCurveCmd, checkCmd)
}
