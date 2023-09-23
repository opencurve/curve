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
* Project: CurveCli
* Created Date: 2023-09-23
* Author: baytan0720
 */

package snapshot

import (
	"encoding/json"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	snapshotExample = `$ curve bs create volume snapshot --user root --filename test --snapshotname snap-test`
)

type SnapshotCmd struct {
	basecmd.FinalCurveCmd
	snapshotAddrs []string
	timeout       time.Duration

	user         string
	fileName     string
	snapshotName string
}

var _ basecmd.FinalCurveCmdFunc = (*SnapshotCmd)(nil)

func NewCommand() *cobra.Command {
	return NewSnapshotCmd().Cmd
}

func NewSnapshotCmd() *SnapshotCmd {
	sCmd := &SnapshotCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "snapshot",
			Short:   "create volume snapshot in curvebs cluster",
			Example: snapshotExample,
		},
	}
	basecmd.NewFinalCurveCli(&sCmd.FinalCurveCmd, sCmd)
	return sCmd
}

func (sCmd *SnapshotCmd) AddFlags() {
	config.AddBsSnapshotCloneFlagOption(sCmd.Cmd)
	config.AddHttpTimeoutFlag(sCmd.Cmd)
	config.AddBsUserRequiredFlag(sCmd.Cmd)
	config.AddBsFileNameRequiredFlag(sCmd.Cmd)
	config.AddBsSnapshotNameRequiredFlag(sCmd.Cmd)
}

func (sCmd *SnapshotCmd) Init(cmd *cobra.Command, args []string) error {
	snapshotAddrs, err := config.GetBsSnapshotAddrSlice(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS || len(snapshotAddrs) == 0 {
		return err.ToError()
	}
	sCmd.snapshotAddrs = snapshotAddrs
	sCmd.timeout = config.GetFlagDuration(sCmd.Cmd, config.HTTPTIMEOUT)
	sCmd.user = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_USER)
	sCmd.fileName = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_FILENAME)
	sCmd.snapshotName = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_SNAPSHOTNAME)
	sCmd.SetHeader([]string{cobrautil.ROW_USER, cobrautil.ROW_FILE_NAME, cobrautil.ROW_SNAPSHOTNAME, cobrautil.ROW_RESULT})
	return nil
}

func (sCmd *SnapshotCmd) RunCommand(cmd *cobra.Command, args []string) error {
	params := map[string]any{
		cobrautil.QueryAction: cobrautil.ActionCreateSnapshot,
		cobrautil.QueryUser:   sCmd.user,
		cobrautil.QueryFile:   sCmd.fileName,
		cobrautil.QueryName:   sCmd.snapshotName,
	}
	subUri := cobrautil.NewSnapshotQuerySubUri(params)
	metric := basecmd.NewMetric(sCmd.snapshotAddrs, subUri, sCmd.timeout)
	result, err := basecmd.QueryMetric(metric)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(result), &payload); err != nil {
		return err
	}
	row := make(map[string]string)
	row[cobrautil.ROW_USER] = sCmd.user
	row[cobrautil.ROW_FILE_NAME] = sCmd.fileName
	row[cobrautil.ROW_SNAPSHOTNAME] = sCmd.snapshotName

	if payload["Code"] != "0" {
		row[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_FAILED
	} else {
		row[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS
	}

	sCmd.Result = row
	return nil
}

func (sCmd *SnapshotCmd) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SnapshotCmd) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}
