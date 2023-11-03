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
 * Created Date: 2022-10-25
 * Author: ShiYue
 */

package create

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/create/cluster"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/create/dir"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/create/file"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/create/snapshot"
	"github.com/spf13/cobra"
)

type CreateCmd struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*CreateCmd)(nil) // check interface

func (createCmd *CreateCmd) AddSubCommands() {
	createCmd.Cmd.AddCommand(
		cluster.NewClusterTopoCmd(),
		dir.NewDirectoryCommand(),
		file.NewFileCommand(),
    snapshot.NewSnapshotCommand(),
	)
}

func NewCreateCmd() *cobra.Command {
	createCmd := &CreateCmd{
		basecmd.MidCurveCmd{
			Use:   "create",
			Short: "create resources in curvebs cluster",
		},
	}
	return basecmd.NewMidCurveCli(&createCmd.MidCurveCmd, createCmd)
}
