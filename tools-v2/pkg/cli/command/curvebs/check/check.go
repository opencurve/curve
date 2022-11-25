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
 * Created Date: 2022-11-04
 * Author: ShiYue
 */

package check

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check/consistency"
	"github.com/spf13/cobra"
)

type CheckCmd struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*CheckCmd)(nil) // check interface

func (checkCmd *CheckCmd) AddSubCommands() {
	checkCmd.Cmd.AddCommand(
		consistency.NewConsistencyCmd(),
	)
}

func NewCheckCmd() *cobra.Command {
	checkCmd := &CheckCmd{
		basecmd.MidCurveCmd{
			Use:   "check",
			Short: "check resources in curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&checkCmd.MidCurveCmd, checkCmd)
}
