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
 * Created Date: 2022-06-15
 * Author: chengyi (Cyber-SiKu)
 */

package query

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/query/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/query/fs"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/query/inode"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/query/metaserver"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/query/partition"
	"github.com/spf13/cobra"
)

type QueryCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*QueryCommand)(nil) // check interface

func (queryCmd *QueryCommand) AddSubCommands() {
	queryCmd.Cmd.AddCommand(
		fs.NewFsCommand(),
		metaserver.NewMetaserverCommand(),
		partition.NewPartitionCommand(),
		copyset.NewCopysetCommand(),
		inode.NewInodeCommand(),
	)
}

func NewQueryCommand() *cobra.Command {
	queryCmd := &QueryCommand{
		basecmd.MidCurveCmd{
			Use:   "query",
			Short: "query resources in the curvefs",
		},
	}
	return basecmd.NewMidCurveCli(&queryCmd.MidCurveCmd, queryCmd)
}
