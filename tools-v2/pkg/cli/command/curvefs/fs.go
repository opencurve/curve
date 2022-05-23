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
 * Created Date: 2022-05-10
 * Author: chengyi (Cyber-SiKu)
 */

package curvefs

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/create"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/delete"
	list "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/list"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/query"
	status "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/status"
	umount "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/umount"
	usage "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/usage"
	"github.com/spf13/cobra"
)

type CurveFsCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*CurveFsCommand)(nil) // check interface

func (fsCmd *CurveFsCommand) AddSubCommands() {
	fsCmd.Cmd.AddCommand(
		usage.NewUsageCommand(),
		list.NewListCommand(),
		status.NewStatusCommand(),
		umount.NewUmountCommand(),
		query.NewQueryCommand(),
		delete.NewDeleteCommand(),
		create.NewCreateCommand(),
	)
}

func NewCurveFsCommand() *cobra.Command {
	fsCmd := &CurveFsCommand{
		basecmd.MidCurveCmd{
			Use:   "fs",
			Short: "Manage curvefs cluster",
		},
	}
	return basecmd.NewMidCurveCli(&fsCmd.MidCurveCmd, fsCmd)
}
