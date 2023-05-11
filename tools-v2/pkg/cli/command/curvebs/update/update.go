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
 * Created Date: 2023-02-27
 * Author: zweix
 */

package update

import (
	"github.com/spf13/cobra"

	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/update/file"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/update/leader"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/update/peer"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/update/scan_state"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/update/throttle"
)

type UpdateCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*UpdateCommand)(nil)

func (updateCmd *UpdateCommand) AddSubCommands() {
	updateCmd.Cmd.AddCommand(
		peer.NewPeerCommand(),
		file.NewFileCommand(),
		throttle.NewThrottleCommand(),
		leader.NewleaderCommand(),
		scan_state.NewScanStateCommand(),
	)
}

func NewUpdateCommand() *cobra.Command {
	updateCmd := &UpdateCommand{
		basecmd.MidCurveCmd{
			Use:   "update",
			Short: "update resources in the curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&updateCmd.MidCurveCmd, updateCmd)
}
