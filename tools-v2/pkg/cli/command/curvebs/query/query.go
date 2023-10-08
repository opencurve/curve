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
 * Created Date: 2022-08-25
 * Author: chengyi (Cyber-SiKu)
 */

package query

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/chunk"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/file"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/scanstatus"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/seginfo"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/volume"
	"github.com/spf13/cobra"
)

type QueryCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*QueryCommand)(nil) // check interface

func (queryCmd *QueryCommand) AddSubCommands() {
	queryCmd.Cmd.AddCommand(
		file.NewFileCommand(),
		seginfo.NewSeginfoCommand(),
		chunk.NewChunkCommand(),
		scanstatus.NewScanStatusCommand(),
		volume.NewVolumeCommand(),
	)
}

func NewQueryCommand() *cobra.Command {
	queryCmd := &QueryCommand{
		basecmd.MidCurveCmd{
			Use:   "query",
			Short: "query resources in the curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&queryCmd.MidCurveCmd, queryCmd)
}
