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
* Project: curve
* Created Date: 2023-08-23
* Author: lng2020
 */

package volume

import (
	"time"

	"github.com/spf13/cobra"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
)

const (
	cloneExample = `$ curve bs create volume clone`
)

type CloneCmd struct {
	basecmd.FinalCurveCmd
	snapshotAddrs []string
	timeout       time.Duration

	user string
	src  string
	dest string
	lazy bool
}

var _ basecmd.FinalCurveCmdFunc = (*CloneCmd)(nil)

func (cCmd *CloneCmd) Init(cmd *cobra.Command, args []string) error {
	snapshotAddrs, err := config.GetBsSnapshotAddrSlice(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS || len(snapshotAddrs) == 0 {
		return err.ToError()
	}
	cCmd.snapshotAddrs = snapshotAddrs
	cCmd.timeout = config.GetFlagDuration(cCmd.Cmd, config.HTTPTIMEOUT)
	cCmd.user = config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_USER)
	cCmd.src = config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_SRC)
	cCmd.dest = config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_DEST)
	cCmd.lazy = config.GetBsFlagBool(cCmd.Cmd, config.CURVEBS_LAZY)
	return nil
}

func (cCmd *CloneCmd) RunCommand(cmd *cobra.Command, args []string) error {
	c := newCloneOrRecover(cCmd.snapshotAddrs, cCmd.timeout, cCmd.user, cCmd.src, cCmd.dest, cCmd.lazy)
	if err := c.CreateClone(); err != nil {
		return err
	}
	return nil
}

func (cCmd *CloneCmd) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CloneCmd) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *CloneCmd) AddFlags() {
	config.AddBsSnapshotCloneFlagOption(cCmd.Cmd)
	config.AddHttpTimeoutFlag(cCmd.Cmd)
	config.AddBsUserOptionFlag(cCmd.Cmd)
	config.AddBsSrcOptionFlag(cCmd.Cmd)
	config.AddBsDestOptionFlag(cCmd.Cmd)
	config.AddBsLazyOptionFlag(cCmd.Cmd)
}

func NewCreateVolumeCloneCommand() *CloneCmd {
	cCmd := &CloneCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "clone",
			Short:   "create volume clone tasks in curvebs cluster",
			Example: cloneExample,
		},
	}
	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd
}

func NewCloneCommand() *cobra.Command {
	return NewCreateVolumeCloneCommand().Cmd
}
