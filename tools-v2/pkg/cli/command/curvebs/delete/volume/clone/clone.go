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
* Created Date: 2023-08-21
* Author: setcy
 */

package clone

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/spf13/cobra"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	snapshotutil "github.com/opencurve/curve/tools-v2/internal/utils/snapshot"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
)

const (
	cloneExample = `$ curve bs delete volume clone`
)

type CloneCmd struct {
	basecmd.FinalCurveCmd
	snapshotAddrs []string
	timeout       time.Duration

	user   string
	src    string
	dest   string
	taskID string
	all    bool
	failed bool
	status string
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
	rCmd.all = config.GetBsFlagBool(rCmd.Cmd, config.CURVEBS_ALL)
	rCmd.failed = config.GetBsFlagBool(rCmd.Cmd, config.CURVEBS_FAILED)
	if rCmd.failed {
		rCmd.status = "5"
	}
	rCmd.SetHeader([]string{cobrautil.ROW_USER, cobrautil.ROW_SRC, cobrautil.ROW_TASK_ID, cobrautil.ROW_FILE, cobrautil.ROW_RESULT, cobrautil.ROW_REASON})
	return nil
}

func (rCmd *CloneCmd) RunCommand(cmd *cobra.Command, args []string) error {
	params := map[string]any{
		snapshotutil.QueryAction:      snapshotutil.ActionGetCloneTaskList,
		snapshotutil.QueryType:        snapshotutil.TypeCloneTask,
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

		var payload struct {
			snapshotutil.Response
			TaskInfos  snapshotutil.TaskInfos `json:"TaskInfos"`
			TotalCount int                    `json:"TotalCount"`
		}
		if err := json.Unmarshal([]byte(result), &payload); err != nil {
			return err
		}
		if payload.Code != snapshotutil.ResultSuccess {
			return fmt.Errorf("get clone list fail, requestId: %s, code: %s, message: %s", payload.RequestId, payload.Code, payload.Message)
		}
		if len(payload.TaskInfos) == 0 {
			break
		} else {
			records = append(records, payload.TaskInfos...)
			params[snapshotutil.QueryOffset] = params[snapshotutil.QueryOffset].(int) + params[snapshotutil.QueryLimit].(int)
		}
	}

	wg := sync.WaitGroup{}
	for _, item := range records {
		wg.Add(1)
		go func(clone snapshotutil.TaskInfo) {
			defer wg.Done()
			result := cobrautil.ROW_VALUE_SUCCESS
			reason := ""
			params := map[string]any{
				snapshotutil.QueryAction: snapshotutil.ActionCleanCloneTask,
				snapshotutil.QueryUser:   clone.User,
				snapshotutil.QueryUUID:   clone.UUID,
			}
			subUri := snapshotutil.NewQuerySubUri(params)
			metric := basecmd.NewMetric(rCmd.snapshotAddrs, subUri, rCmd.timeout)
			retStr, err := basecmd.QueryMetric(metric)
			if err.TypeCode() != cmderror.CODE_SUCCESS {
				result = cobrautil.ROW_VALUE_FAILED
				reason = err.ToError().Error()
			} else {
				payload := snapshotutil.Response{}
				if err := json.Unmarshal([]byte(retStr), &payload); err != nil {
					result = cobrautil.ROW_VALUE_FAILED
					reason = err.Error()
				} else {
					if payload.Code != snapshotutil.ResultSuccess {
						result = cobrautil.ROW_VALUE_FAILED
						reason = payload.Message
					}
				}
			}
			rCmd.TableNew.Append(cobrautil.Map2List(map[string]string{
				cobrautil.ROW_USER:    clone.User,
				cobrautil.ROW_SRC:     clone.Src,
				cobrautil.ROW_TASK_ID: clone.UUID,
				cobrautil.ROW_FILE:    clone.File,
				cobrautil.ROW_RESULT:  result,
				cobrautil.ROW_REASON:  reason,
			}, rCmd.Header))
		}(item)
	}
	wg.Wait()

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
}

func NewDeleteVolumeCloneCommand() *CloneCmd {
	rCmd := &CloneCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "clone",
			Short:   "delete volume clone tasks in curvebs cluster",
			Example: cloneExample,
		},
	}
	basecmd.NewFinalCurveCli(&rCmd.FinalCurveCmd, rCmd)
	return rCmd
}

func NewCloneCommand() *cobra.Command {
	return NewDeleteVolumeCloneCommand().Cmd
}
