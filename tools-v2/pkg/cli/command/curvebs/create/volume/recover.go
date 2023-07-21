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
	recoverExample = `$ curve bs create volume recover`
)

type RecoverCmd struct {
	basecmd.FinalCurveCmd
	snapshotAddrs []string
	timeout       time.Duration

	user string
	src  string
	dest string
	lazy bool
}

var _ basecmd.FinalCurveCmdFunc = (*RecoverCmd)(nil)

func (rCmd *RecoverCmd) Init(cmd *cobra.Command, args []string) error {
	snapshotAddrs, err := config.GetBsSnapshotAddrSlice(rCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS || len(snapshotAddrs) == 0 {
		return err.ToError()
	}
	rCmd.snapshotAddrs = snapshotAddrs
	rCmd.timeout = config.GetFlagDuration(rCmd.Cmd, config.HTTPTIMEOUT)
	rCmd.user = config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_USER)
	rCmd.src = config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_SRC)
	rCmd.dest = config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_DEST)
	rCmd.lazy = config.GetBsFlagBool(rCmd.Cmd, config.CURVEBS_LAZY)
	return nil
}

func (rCmd *RecoverCmd) RunCommand(cmd *cobra.Command, args []string) error {
	c := newCloneOrRecover(rCmd.snapshotAddrs, rCmd.timeout, rCmd.user, rCmd.src, rCmd.dest, rCmd.lazy)
	if err := c.CreateRecover(); err != nil {
		return err
	}
	return nil
}

func (rCmd *RecoverCmd) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&rCmd.FinalCurveCmd, rCmd)
}

func (rCmd *RecoverCmd) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&rCmd.FinalCurveCmd)
}

func (rCmd *RecoverCmd) AddFlags() {
	config.AddBsSnapshotCloneFlagOption(rCmd.Cmd)
	config.AddHttpTimeoutFlag(rCmd.Cmd)
	config.AddBsUserOptionFlag(rCmd.Cmd)
	config.AddBsSrcOptionFlag(rCmd.Cmd)
	config.AddBsDestOptionFlag(rCmd.Cmd)
	config.AddBsLazyOptionFlag(rCmd.Cmd)
}

func NewCreateVolumeRecoverCommand() *RecoverCmd {
	rCmd := &RecoverCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "recover",
			Short:   "create volume recover tasks in curvebs cluster",
			Example: recoverExample,
		},
	}
	basecmd.NewFinalCurveCli(&rCmd.FinalCurveCmd, rCmd)
	return rCmd
}

func NewRecoverCommand() *cobra.Command {
	return NewCreateVolumeRecoverCommand().Cmd
}
