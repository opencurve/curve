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
* Created Date: 2023-09-16
* Author: baytan0720
 */

package flatten

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
	flattenExample = `$ curve bs update volume flatten --user root --taskid d26e27a8-fcbd-4f7a-adf8-53795217cbb0`
)

type FlattenCmd struct {
	basecmd.FinalCurveCmd
	snapshotAddrs []string
	timeout       time.Duration

	user   string
	taskID string
}

var _ basecmd.FinalCurveCmdFunc = (*FlattenCmd)(nil)

func NewCommand() *cobra.Command {
	return NewFlattenCmd().Cmd
}

func NewFlattenCmd() *FlattenCmd {
	fCmd := &FlattenCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "flatten",
			Short:   "update volume flatten in curvebs cluster",
			Example: flattenExample,
		},
	}
	basecmd.NewFinalCurveCli(&fCmd.FinalCurveCmd, fCmd)
	return fCmd
}

func (fCmd *FlattenCmd) AddFlags() {
	config.AddBsSnapshotCloneFlagOption(fCmd.Cmd)
	config.AddHttpTimeoutFlag(fCmd.Cmd)
	config.AddBsUserRequireFlag(fCmd.Cmd)
	config.AddBsTaskIDRequireFlag(fCmd.Cmd)
}

func (fCmd *FlattenCmd) Init(cmd *cobra.Command, args []string) error {
	snapshotAddrs, err := config.GetBsSnapshotAddrSlice(fCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS || len(snapshotAddrs) == 0 {
		return err.ToError()
	}
	fCmd.snapshotAddrs = snapshotAddrs
	fCmd.timeout = config.GetFlagDuration(fCmd.Cmd, config.HTTPTIMEOUT)
	fCmd.user = config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_USER)
	fCmd.taskID = config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_TASKID)
	fCmd.SetHeader([]string{cobrautil.ROW_USER, cobrautil.ROW_TASK_ID, cobrautil.ROW_RESULT})
	return nil
}

func (fCmd *FlattenCmd) RunCommand(cmd *cobra.Command, args []string) error {
	params := map[string]any{
		cobrautil.QueryAction: cobrautil.ActionFlatten,
		cobrautil.QueryUser:   fCmd.user,
		cobrautil.QueryUUID:   fCmd.taskID,
	}
	subUri := cobrautil.NewSnapshotQuerySubUri(params)
	metric := basecmd.NewMetric(fCmd.snapshotAddrs, subUri, fCmd.timeout)
	result, err := basecmd.QueryMetric(metric)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(result), &payload); err != nil {
		return err
	}
	row := make(map[string]string)
	row[cobrautil.ROW_USER] = fCmd.user
	row[cobrautil.ROW_TASK_ID] = fCmd.taskID

	if payload[cobrautil.ResultCode] != cobrautil.ResultSuccess {
		row[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_FAILED
	} else {
		row[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS
	}

	fCmd.Result = row
	return nil
}

func (fCmd *FlattenCmd) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *FlattenCmd) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}
