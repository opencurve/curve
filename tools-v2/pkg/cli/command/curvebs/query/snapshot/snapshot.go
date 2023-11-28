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
 * Created Date: 2023-11-03
 * Author: ZackSoul
 */
package snapshot

import (
	"fmt"
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
	snapshotExample = `$ curve bs query snapshot`
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
	config.AddBsSnapshotIDRequiredFlag(sCmd.Cmd)
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
		cobrautil.ROW_USER,
		cobrautil.ROW_STATUS,
		cobrautil.ROW_SNAPSHOT_SEQNUM,
		cobrautil.ROW_FILE_LENGTH,
		cobrautil.ROW_PROGRESS,
		cobrautil.ROW_CREATE_TIME,
		cobrautil.ROW_FILE,
	}
	sCmd.SetHeader(header)
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
		snapshotutil.QueryUUID:    sCmd.uuid,
		snapshotutil.QueryLimit:   snapshotutil.Limit,
		snapshotutil.QueryOffset:  snapshotutil.Offset,
	}
	snapshots, err := listSnapshot.ListSnapShot(sCmd.snapshotAddrs, sCmd.timeout, params)
	if err != nil {
		sCmd.Error = err
		return sCmd.Error.ToError()
	}
	row := make(map[string]string)
	if snapshots != nil {
		row[cobrautil.ROW_SNAPSHOT_ID] = snapshots[0].UUID
		row[cobrautil.ROW_SNAPSHOT_NAME] = snapshots[0].Name
		row[cobrautil.ROW_USER] = snapshots[0].User
		row[cobrautil.ROW_FILE] = snapshots[0].File
		row[cobrautil.ROW_STATUS] = fmt.Sprintf("%d", snapshots[0].Status)
		row[cobrautil.ROW_SNAPSHOT_SEQNUM] = fmt.Sprintf("%d", snapshots[0].SeqNum)
		row[cobrautil.ROW_FILE_LENGTH] = fmt.Sprintf("%d", snapshots[0].FileLength)
		row[cobrautil.ROW_PROGRESS] = fmt.Sprintf("%v", snapshots[0].Progress)
		row[cobrautil.ROW_CREATE_TIME] = time.Unix(int64(snapshots[0].Time/1000000), 0).Format("2006-01-02 15:04:05")
	}
	list := cobrautil.Map2List(row, sCmd.Header)
	sCmd.TableNew.Append(list)
	return nil
}

func (sCmd *SnapShotCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func QuerySnapShot(addrs []string, timeout time.Duration, params map[string]any) (*snapshotutil.SnapshotInfo, *cmderror.CmdError) {
	snapshotsInfo, err := listSnapshot.ListSnapShot(addrs, timeout, params)
	if err != nil {
		return nil, err
	}
	if snapshotsInfo == nil || len(snapshotsInfo) == 0 {
		return nil, err
	}
	return snapshotsInfo[0], err
}
