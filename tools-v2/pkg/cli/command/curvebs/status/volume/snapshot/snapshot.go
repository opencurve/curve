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
 * Created Date: 2023-09-13
 * Author: saicaca
 */

package snapshot

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	snapshotutil "github.com/opencurve/curve/tools-v2/internal/utils/snapshot"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	LENGTH_EMPTY = 0
)

type SnapshotCommand struct {
	basecmd.FinalCurveCmd

	snapshotAddrs []string
	timeout       time.Duration
	user          string
	filename      string

	errCode int
	health  cobrautil.ClUSTER_HEALTH_STATUS
}

var _ basecmd.FinalCurveCmdFunc = (*SnapshotCommand)(nil) // check interface

func NewSnapshotCommand() *cobra.Command {
	return NewVolumeSnapshotCommand().Cmd
}

func NewVolumeSnapshotCommand() *SnapshotCommand {
	vsCmd := &SnapshotCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "snapshot",
			Short: "show the snapshot status",
		},
	}
	basecmd.NewFinalCurveCli(&vsCmd.FinalCurveCmd, vsCmd)
	return vsCmd
}

func (sCmd *SnapshotCommand) Init(cmd *cobra.Command, args []string) error {
	snapshotAddrs, err := config.GetBsSnapshotAddrSlice(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS || len(snapshotAddrs) == 0 {
		return err.ToError()
	}
	sCmd.snapshotAddrs = snapshotAddrs
	sCmd.filename = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_FILENAME)
	sCmd.user = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_USER)
	sCmd.timeout = config.GetFlagDuration(sCmd.Cmd, config.HTTPTIMEOUT)

	header := []string{cobrautil.ROW_STATUS, cobrautil.ROW_COUNT}
	sCmd.SetHeader(header)

	return nil
}

func (sCmd *SnapshotCommand) RunCommand(cmd *cobra.Command, args []string) error {
	params := map[string]any{
		snapshotutil.QueryAction: snapshotutil.ActionGetFileSnapshotList,
		snapshotutil.QueryUser:   sCmd.user,
		snapshotutil.QueryFile:   sCmd.filename,
		snapshotutil.QueryLimit:  snapshotutil.Limit,
		snapshotutil.QueryOffset: snapshotutil.Offset,
	}
	count := make(map[int]int)
	for {
		subUri := snapshotutil.NewQuerySubUri(params)

		metric := basecmd.NewMetric(sCmd.snapshotAddrs, subUri, sCmd.timeout)
		result, err := basecmd.QueryMetric(metric)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			sCmd.Error = cmderror.ErrBsGetSnapshotStatus()
			sCmd.Error.Format(fmt.Sprintf("code: %d, msg: %s", err.TypeCode(), err.ToError().Error()))
			return sCmd.Error.ToError()
		}

		var resp struct {
			snapshotutil.Response
			TotalCount int                          `json:"TotalCount"`
			SnapShots  []*snapshotutil.SnapshotInfo `json:"SnapShots"`
		}
		if err := json.Unmarshal([]byte(result), &resp); err != nil {
			sCmd.Error = cmderror.ErrUnmarshalJson()
			sCmd.Error.Format(err.Error())
			return sCmd.Error.ToError()
		}
		if resp.Code != snapshotutil.ResultSuccess {
			sCmd.Error = cmderror.ErrBsGetSnapshotStatus()
			sCmd.Error.Format(fmt.Sprintf("code: %s, msg: %s", resp.Code, resp.Message))
			return sCmd.Error.ToError()
		}
		if len(resp.SnapShots) == LENGTH_EMPTY {
			break
		}
		for _, s := range resp.SnapShots {
			count[s.Status] += 1
		}

		params[snapshotutil.QueryOffset] = params[snapshotutil.QueryOffset].(int) + params[snapshotutil.QueryLimit].(int)
	}

	rows := make([]map[string]string, 0)
	for i, s := range snapshotutil.SnapshotStatus {
		rows = append(rows, map[string]string{
			cobrautil.ROW_STATUS: s,
			cobrautil.ROW_COUNT:  strconv.Itoa(count[i]),
		})
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, sCmd.Header, []string{})
	sCmd.TableNew.AppendBulk(list)
	sCmd.Result = rows
	sCmd.Error = cmderror.ErrSuccess()

	return nil
}

func (sCmd *SnapshotCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SnapshotCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func (sCmd *SnapshotCommand) AddFlags() {
	config.AddBsUserOptionFlag(sCmd.Cmd)
	config.AddBsSnapshotFilenameFlag(sCmd.Cmd)
	config.AddBsSnapshotCloneFlagOption(sCmd.Cmd)
	config.AddHttpTimeoutFlag(sCmd.Cmd)
}
