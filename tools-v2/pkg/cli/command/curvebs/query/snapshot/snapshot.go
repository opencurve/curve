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
 * Created Date: 2023-12-04
 * Author: ZackSoul
 */
package snapshot

import (
	"strconv"
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
	snapshotExample = `$ curve bs query snapshot --path /test/test111 --snapshotstatus done # other status: "in-progess" "deleting" "errorDeleteing" "canceling" "error"`
)

type SnapShotCommand struct {
	basecmd.FinalCurveCmd
	snapshotAddrs []string
	timeout       time.Duration

	user   string
	file   string
	uuid   string
	status string
}

var _ basecmd.FinalCurveCmdFunc = (*SnapShotCommand)(nil)

func NewQuerySnapshotCommand() *SnapShotCommand {
	sCmd := &SnapShotCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "snapshot",
			Short:   "query snapshot in curve bs",
			Example: snapshotExample,
		},
	}

	basecmd.NewFinalCurveCli(&sCmd.FinalCurveCmd, sCmd)
	return sCmd
}

func NewSnapshotCommand() *cobra.Command {
	return NewQuerySnapshotCommand().Cmd
}

func (sCmd *SnapShotCommand) AddFlags() {
	config.AddBsSnapshotCloneFlagOption(sCmd.Cmd)
	config.AddHttpTimeoutFlag(sCmd.Cmd)
	config.AddBsUserOptionFlag(sCmd.Cmd)
	config.AddBsSnapshotIDOptionFlag(sCmd.Cmd)
	config.AddBsPathOptionFlag(sCmd.Cmd)
	config.AddSnapshotStatusOptionalFlag(sCmd.Cmd)
}

func (sCmd *SnapShotCommand) Init(cmd *cobra.Command, args []string) error {
	snapshotAddrs, err := config.GetBsSnapshotAddrSlice(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		sCmd.Error = err
		return err.ToError()
	}
	sCmd.snapshotAddrs = snapshotAddrs
	sCmd.timeout = config.GetFlagDuration(sCmd.Cmd, config.HTTPTIMEOUT)
	sCmd.user = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_USER)
	sCmd.file = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_PATH)
	sCmd.uuid = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_SNAPSHOT_ID)
	sCmd.status = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_SNAPSHOT_STATUS)
	header := []string{
		cobrautil.ROW_SNAPSHOT_ID,
		cobrautil.ROW_SNAPSHOT_NAME,
		cobrautil.ROW_USER,
		cobrautil.ROW_STATUS,
		cobrautil.ROW_SNAPSHOT_SEQNUM,
		cobrautil.ROW_FILE_LENGTH,
		cobrautil.ROW_PROGRESS,
		cobrautil.ROW_CREATE_TIME,
		cobrautil.ROW_FILE,
	}
	sCmd.SetHeader(header)
	sCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		sCmd.Header, []string{cobrautil.ROW_FILE},
	))
	return nil
}

func (sCmd *SnapShotCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SnapShotCommand) RunCommand(cmd *cobra.Command, args []string) error {
	params := map[string]any{
		snapshotutil.QueryAction:  snapshotutil.ActionGetFileSnapshotList,
		snapshotutil.QueryVersion: snapshotutil.Version,
		snapshotutil.QueryUser:    sCmd.user,
		snapshotutil.QueryFile:    sCmd.file,
		snapshotutil.QueryLimit:   snapshotutil.Limit,
		snapshotutil.QueryOffset:  snapshotutil.Offset,
	}
	if sCmd.uuid != snapshotutil.DefaultSnapID {
		params[snapshotutil.QueryUUID] = sCmd.uuid
	}
	for i, status := range snapshotutil.SnapshotStatus {
		if status == sCmd.status {
			params[snapshotutil.QueryStatus] = i
		}
	}
	snapshotsInfo, err := listSnapshot.ListSnapShot(sCmd.snapshotAddrs, sCmd.timeout, params)
	if err != nil {
		sCmd.Error = err
		return sCmd.Error.ToError()
	}
	rows := make([]map[string]string, 0)
	if len(snapshotsInfo) == 0 {
		rows = append(rows, listSnapshot.EmptyOutput())
	}
	for _, item := range snapshotsInfo {
		row := make(map[string]string)
		row[cobrautil.ROW_SNAPSHOT_ID] = item.UUID
		row[cobrautil.ROW_SNAPSHOT_NAME] = item.Name
		row[cobrautil.ROW_USER] = item.User
		row[cobrautil.ROW_FILE] = item.File
		row[cobrautil.ROW_STATUS] = strconv.Itoa(item.Status)
		row[cobrautil.ROW_SNAPSHOT_SEQNUM] = strconv.Itoa(item.SeqNum)
		row[cobrautil.ROW_FILE_LENGTH] = strconv.Itoa(item.FileLength)
		row[cobrautil.ROW_PROGRESS] = strconv.FormatFloat(item.Progress, 'f', 2, 64)
		row[cobrautil.ROW_CREATE_TIME] = time.Unix(int64(item.Time/1000000), 0).Format("2006-01-02 15:04:05")
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
