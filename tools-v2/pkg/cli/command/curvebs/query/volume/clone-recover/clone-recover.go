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
* Created Date: 2023-10-08
* Author: baytan0720
 */

package clone_recover

import (
	"encoding/json"
	"fmt"
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
	cloneRecoverExample = `$ curve bs query volume clone-recover`
)

type CloneCmd struct {
	basecmd.FinalCurveCmd
	snapshotAddrs []string
	timeout       time.Duration

	user     string
	src      string
	dest     string
	failed   bool
	status   string
	taskID   string
	taskType string
	records  snapshotutil.TaskInfos
}

var _ basecmd.FinalCurveCmdFunc = (*CloneCmd)(nil)

func (rCmd *CloneCmd) Init(cmd *cobra.Command, args []string) error {
	snapshotAddrs, err := config.GetBsSnapshotAddrSlice(rCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS || len(snapshotAddrs) == 0 {
		return err.ToError()
	}
	rCmd.snapshotAddrs = snapshotAddrs
	rCmd.timeout = config.GetFlagDuration(rCmd.Cmd, config.HTTPTIMEOUT)
	rCmd.user = config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_USER)
	rCmd.src = config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_SRC)
	rCmd.dest = config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_DEST)
	rCmd.taskID = config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_TASKID)
	rCmd.failed = config.GetBsFlagBool(rCmd.Cmd, config.CURVEBS_FAILED)
	if rCmd.failed {
		rCmd.status = "5"
	}
	taskType := config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_TYPE)
	switch taskType {
	case "clone":
		rCmd.taskType = snapshotutil.TypeCloneTask
	case "recover":
		rCmd.taskType = snapshotutil.TypeRecoverTask
	case "":
	default:
		return fmt.Errorf("invalid task type: %s, only support clone and recover", taskType)
	}

	rCmd.SetHeader([]string{cobrautil.ROW_USER, cobrautil.ROW_SRC, cobrautil.ROW_TASK_ID, cobrautil.ROW_TASKTYPE, cobrautil.ROW_FILE, cobrautil.ROW_FILE_TYPE, cobrautil.ROW_ISLAZY, cobrautil.ROW_NEXTSTEP, cobrautil.ROW_STATUS, cobrautil.ROW_PROGRESS, cobrautil.ROW_TIME})
	return nil
}

func (rCmd *CloneCmd) RunCommand(cmd *cobra.Command, args []string) error {
	params := map[string]any{
		snapshotutil.QueryAction:      snapshotutil.ActionGetCloneTaskList,
		snapshotutil.QueryType:        rCmd.taskType,
		snapshotutil.QueryUser:        rCmd.user,
		snapshotutil.QueryUUID:        rCmd.taskID,
		snapshotutil.QuerySource:      rCmd.src,
		snapshotutil.QueryDestination: rCmd.dest,
		snapshotutil.QueryStatus:      rCmd.status,
		snapshotutil.QueryLimit:       100,
		snapshotutil.QueryOffset:      0,
	}
	records := make(snapshotutil.TaskInfos, 0)
	for {
		subUri := snapshotutil.NewQuerySubUri(params)
		metric := basecmd.NewMetric(rCmd.snapshotAddrs, subUri, rCmd.timeout)
		result, err := basecmd.QueryMetric(metric)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}

		var resp struct {
			snapshotutil.Response
			TaskInfos snapshotutil.TaskInfos `json:"TaskInfos"`
		}
		if err := json.Unmarshal([]byte(result), &resp); err != nil {
			return err
		}
		if resp.Code != snapshotutil.ResultSuccess {
			return fmt.Errorf("get clone-recover list fail, requestId: %s,code: %s, error: %s", resp.RequestId, resp.Code, resp.Message)
		}
		if len(resp.TaskInfos) == 0 {
			break
		} else {
			records = append(records, resp.TaskInfos...)
			params[snapshotutil.QueryOffset] = params[snapshotutil.QueryOffset].(int) + params[snapshotutil.QueryLimit].(int)
		}
	}

	rows := make([][]string, len(records))
	for _, record := range records {
		rows = append(rows, []string{
			record.User,
			record.Src,
			record.UUID,
			snapshotutil.TaskType[record.TaskType],
			record.File,
			snapshotutil.FileType[record.FileType],
			fmt.Sprintf("%t", record.IsLazy),
			snapshotutil.CloneStep[record.NextStep],
			snapshotutil.TaskStatus[record.TaskStatus],
			fmt.Sprintf("%f", record.Progress),
			time.Unix(record.Time, 0).Format("2006-01-02 15:04:05"),
		})
	}

	rCmd.TableNew.AppendBulk(rows)
	rCmd.records = records
	rCmd.Result = records
	rCmd.Error = cmderror.Success()
	return nil
}

func (rCmd *CloneCmd) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&rCmd.FinalCurveCmd, rCmd)
}

func (rCmd *CloneCmd) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&rCmd.FinalCurveCmd)
}

func (rCmd *CloneCmd) AddFlags() {
	config.AddBsSnapshotCloneFlagOption(rCmd.Cmd)
	config.AddHttpTimeoutFlag(rCmd.Cmd)
	config.AddBsUserOptionFlag(rCmd.Cmd)
	config.AddBsSrcOptionFlag(rCmd.Cmd)
	config.AddBsDestOptionFlag(rCmd.Cmd)
	config.AddBsTaskIDOptionFlag(rCmd.Cmd)
	config.AddBsAllOptionFlag(rCmd.Cmd)
	config.AddBsFailedOptionFlag(rCmd.Cmd)
	config.AddBsTaskTypeOptionFlag(rCmd.Cmd)
}

func NewQueryCloneRecoverCommand() *CloneCmd {
	rCmd := &CloneCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "clone-recover",
			Short:   "query volume clone and recover in curvebs cluster",
			Example: cloneRecoverExample,
		},
	}
	basecmd.NewFinalCurveCli(&rCmd.FinalCurveCmd, rCmd)
	return rCmd
}

func NewCommand() *cobra.Command {
	return NewQueryCloneRecoverCommand().Cmd
}

func GetCloneOrRecoverList(caller *cobra.Command) (snapshotutil.TaskInfos, *cmderror.CmdError) {
	getCmd := NewQueryCloneRecoverCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.CURVEBS_SNAPSHOTADDR, config.HTTPTIMEOUT, config.CURVEBS_USER,
		config.CURVEBS_SRC, config.CURVEBS_DEST, config.CURVEBS_TASKID,
		config.CURVEBS_ALL, config.CURVEBS_FAILED, config.CURVEBS_TYPE,
	})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetCloneRecover()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return getCmd.records, cmderror.Success()
}
