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

package recover

import (
	"sync"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"

	. "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/volume/clone_recover"
)

const (
	recoverExample = `$ curve bs delete volume recover`
)

type RecoverCmd struct {
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
	rCmd.taskID = config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_TASKID)
	rCmd.all = config.GetBsFlagBool(rCmd.Cmd, config.CURVEBS_ALL)
	rCmd.failed = config.GetBsFlagBool(rCmd.Cmd, config.CURVEBS_FAILED)
	if rCmd.failed {
		rCmd.status = "5"
	}
	rCmd.SetHeader([]string{cobrautil.ROW_USER, cobrautil.ROW_SRC, cobrautil.ROW_TASK_ID, cobrautil.ROW_FILE, cobrautil.ROW_RESULT})
	return nil
}

func (rCmd *RecoverCmd) RunCommand(cmd *cobra.Command, args []string) error {
	cloneRecoverCmd := CloneRecoverCmd{
		FinalCurveCmd: rCmd.FinalCurveCmd,
		SnapshotAddrs: rCmd.snapshotAddrs,
		Timeout:       rCmd.timeout,
		User:          rCmd.user,
		Src:           rCmd.src,
		Dest:          rCmd.dest,
		TaskID:        rCmd.taskID,
		All:           rCmd.all,
		Failed:        rCmd.failed,
		Status:        rCmd.status,
	}

	records, err := QueryTaskList(&cloneRecoverCmd)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for _, item := range records {
		wg.Add(1)
		go func(item map[string]string) {
			params := map[string]any{
				cobrautil.QueryAction: cobrautil.ActionCleanCloneTask,
				cobrautil.QueryUser:   item["User"],
				cobrautil.QueryUUID:   item["UUID"],
			}
			subUri := cobrautil.NewSnapshotQuerySubUri(params)
			metric := basecmd.NewMetric(rCmd.snapshotAddrs, subUri, rCmd.timeout)
			_, err := basecmd.QueryMetric(metric)
			if err.TypeCode() != cmderror.CODE_SUCCESS {
				item["Result"] = "fail"
			} else {
				item["Result"] = "success"
			}
			rCmd.TableNew.Append([]string{item["User"], item["Src"], item["UUID"], item["File"], item["Result"]})
			wg.Done()
		}(item)
	}
	wg.Wait()
	rCmd.Result = records
	rCmd.Error = cmderror.Success()
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
	config.AddBsTaskIDOptionFlag(rCmd.Cmd)
	config.AddBsAllOptionFlag(rCmd.Cmd)
	config.AddBsFailedOptionFlag(rCmd.Cmd)
}

func NewDeleteVolumeRecoverCommand() *RecoverCmd {
	rCmd := &RecoverCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "recover",
			Short:   "delete volume recover tasks in curvebs cluster",
			Example: recoverExample,
		},
	}
	basecmd.NewFinalCurveCli(&rCmd.FinalCurveCmd, rCmd)
	return rCmd
}

func NewRecoverCommand() *cobra.Command {
	return NewDeleteVolumeRecoverCommand().Cmd
}
