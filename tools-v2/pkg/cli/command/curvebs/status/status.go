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
 * Created Date: 2022-09-08
 * Author: chengyi (Cyber-SiKu)
 */

package status

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/client"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/cluster"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/etcd"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/mds"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/snapshot"
	"github.com/spf13/cobra"
)

type StatusCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*StatusCommand)(nil) // check interface

func (statusCmd *StatusCommand) AddSubCommands() {
	statusCmd.Cmd.AddCommand(
		etcd.NewEtcdCommand(),
		mds.NewMdsCommand(),
		client.NewClientCommand(),
		snapshot.NewSnapshotCommand(),
		chunkserver.NewChunkServerCommand(),
		copyset.NewCopysetCommand(),
		cluster.NewClusterCommand(),
	)
}

func NewStatusCommand() *cobra.Command {
	statusCmd := &StatusCommand{
		basecmd.MidCurveCmd{
			Use:   "status",
			Short: "get the status of curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&statusCmd.MidCurveCmd, statusCmd)
}
