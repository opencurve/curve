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
 * Created Date: 2022-08-24
 * Author: chengyi (Cyber-SiKu)
 */

package curvebs

import (
	"github.com/spf13/cobra"

	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/clean_recycle"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/create"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/delete"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/snapshot"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/update"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/clone"
    "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/flatten"
)

type CurveBsCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*CurveBsCommand)(nil) // check interface

func (bsCmd *CurveBsCommand) AddSubCommands() {
	bsCmd.Cmd.AddCommand(
		list.NewListCommand(),
		query.NewQueryCommand(),
		status.NewStatusCommand(),
		delete.NewDeleteCommand(),
		create.NewCreateCmd(),
		update.NewUpdateCommand(),
		clean_recycle.NewCleanRecycleCommand(),
		check.NewCheckCommand(),
		snapshot.NewSnapshotCommand(),
        clone.NewCloneCommand(),
        flatten.NewFlattenCommand(),
	)
}

func NewCurveBsCommand() *cobra.Command {
	bsCmd := &CurveBsCommand{
		basecmd.MidCurveCmd{
			Use:   "bs",
			Short: "Manage curvebs cluster",
		},
	}
	return basecmd.NewMidCurveCli(&bsCmd.MidCurveCmd, bsCmd)
}
