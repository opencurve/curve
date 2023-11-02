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
 * Created Date: 2023-11-02
 * Author: ZackSoul
 */
package snapshot

import (
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	snapshotutil "github.com/opencurve/curve/tools-v2/internal/utils/snapshot"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	listSnapshot "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/snapshot"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	snapshotExample = `$ curve bs stop snapshot`
)

type SnapShotCommand struct {
	basecmd.FinalCurveCmd
	snapshotAddrs []string
	timeout       time.Duration

	user string
	file string
	uuid string
}

var _ basecmd.FinalCurveCmdFunc = (*SnapShotCommand)(nil)

func NewSnapShotCommand() *cobra.Command {
	return NewStopSnapShotCommand().Cmd
}

func NewStopSnapShotCommand() *SnapShotCommand {
	snapShotCommand := &SnapShotCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "snapshot",
			Short:   "stop snapshot in curvebs",
			Example: snapshotExample,
		},
	}

	basecmd.NewFinalCurveCli(&snapShotCommand.FinalCurveCmd, snapShotCommand)
	return snapShotCommand
}

func (sCmd *SnapShotCommand) AddFlags() {
	config.AddBsSnapshotCloneFlagOption(sCmd.Cmd)
	config.AddHttpTimeoutFlag(sCmd.Cmd)
	config.AddBsUserOptionFlag(sCmd.Cmd)
	config.AddBsSnapshotIDOptionFlag(sCmd.Cmd)
	config.AddBsPathOptionFlag(sCmd.Cmd)
}

func (sCmd *SnapShotCommand) Init(cmd *cobra.Command, args []string) error {
	snapshotAddrs, err := config.GetBsSnapshotAddrSlice(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS || len(snapshotAddrs) == 0 {
		return err.ToError()
	}
	sCmd.snapshotAddrs = snapshotAddrs
	sCmd.timeout = config.GetFlagDuration(sCmd.Cmd, config.HTTPTIMEOUT)
	sCmd.user = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_USER)
	sCmd.file = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_PATH)
	sCmd.uuid = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_SNAPSHOT_ID)
	header := []string{
		cobrautil.ROW_SNAPSHOT_ID,
		cobrautil.ROW_SNAPSHOT_NAME,
		cobrautil.ROW_RESULT,
	}
	sCmd.SetHeader(header)
	return nil
}

func (sCmd *SnapShotCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SnapShotCommand) RunCommand(cmd *cobra.Command, args []string) error {
	params := map[string]any{
		snapshotutil.QueryAction: snapshotutil.ActionGetFileSnapshotList,
		snapshotutil.QueryUser:   sCmd.user,
		snapshotutil.QueryFile:   sCmd.file,
		snapshotutil.QueryLimit:  snapshotutil.Limit,
		snapshotutil.QueryOffset: snapshotutil.Offset,
	}
	if sCmd.uuid != "*" {
		params[snapshotutil.QueryUUID] = sCmd.uuid
	}
	snapshotsList, err := listSnapshot.ListSnapShot(sCmd.snapshotAddrs, sCmd.timeout, params)
	if err != nil {
		sCmd.Error = err
		return sCmd.Error.ToError()
	}
	rows := make([]map[string]string, 0)
	for _, snapshot := range snapshotsList {
		row := make(map[string]string)
		err := StopSnapShot(sCmd.snapshotAddrs, sCmd.timeout, snapshot)
		row[cobrautil.ROW_SNAPSHOT_ID] = snapshot.UUID
		row[cobrautil.ROW_SNAPSHOT_NAME] = snapshot.Name
		if err.TypeCode() == cmderror.CODE_SUCCESS {
			row[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS
		} else {
			row[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_FAILED
		}
		rows = append(rows, row)
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, sCmd.Header, []string{cobrautil.ROW_SNAPSHOT_NAME, cobrautil.ROW_SNAPSHOT_ID})
	sCmd.TableNew.AppendBulk(list)
	sCmd.Result = rows
	sCmd.Error = cmderror.Success()
	return nil
}

func (sCmd *SnapShotCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func StopSnapShot(addrs []string, timeout time.Duration, snapshot *snapshotutil.SnapshotInfo) *cmderror.CmdError {
	params := map[string]any{
		snapshotutil.QueryAction: snapshotutil.ActionCancelSnapshot,
		snapshotutil.QueryUser:   snapshot.User,
		snapshotutil.QueryUUID:   snapshot.UUID,
		snapshotutil.QueryFile:   snapshot.File,
	}
	subUri := snapshotutil.NewQuerySubUri(params)
	metric := basecmd.NewMetric(addrs, subUri, timeout)
	_, err := basecmd.QueryMetric(metric)
	return err
}
