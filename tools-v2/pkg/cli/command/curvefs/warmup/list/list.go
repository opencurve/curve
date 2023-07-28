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
 * Created Date: 2023-08-09
 * Author: Ken Han (ken90242)
 */

package list

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cilium/cilium/pkg/mountinfo"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/pkg/xattr"
	"github.com/spf13/cobra"
)

const (
	listExample = `$ curve fs warmup list `

	CURVEFS_WARMUP_OP_XATTR = "curvefs.warmup.op.list"
)

type ListCommand struct {
	basecmd.FinalCurveCmd
	MountpointInfo *mountinfo.MountInfo
	mountpoint     string
}

var _ basecmd.FinalCurveCmdFunc = (*ListCommand)(nil) // check interface

func NewListWarmupCommand() *ListCommand {
	lCmd := &ListCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "list",
			Short:   "list the ongoing warmup progress for all tasks that are currently in process",
			Example: listExample,
		},
	}
	basecmd.NewFinalCurveCli(&lCmd.FinalCurveCmd, lCmd)
	return lCmd
}

func NewListCommand() *cobra.Command {
	return NewListWarmupCommand().Cmd
}

func (lCmd *ListCommand) AddFlags() {
	config.AddIntervalOptionFlag(lCmd.Cmd)
}

func (lCmd *ListCommand) Init(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return errors.New("please provide the path where CurveFS is being mounted. i.e., curve fs warmup list <curvefs mounted path>")
	}

	mountpoints, err := cobrautil.GetCurveFSMountPoints()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	} else if len(mountpoints) == 0 {
		return errors.New("no curvefs mountpoint found")
	}

	absPath, _ := filepath.Abs(args[0])
	lCmd.mountpoint = absPath

	lCmd.MountpointInfo = nil
	for _, mountpointInfo := range mountpoints {
		rel, err := filepath.Rel(mountpointInfo.MountPoint, absPath)
		if err == nil && !strings.HasPrefix(rel, "..") {
			// found the mountpoint
			if lCmd.MountpointInfo == nil ||
				len(lCmd.MountpointInfo.MountPoint) < len(mountpointInfo.MountPoint) {
				// Prevent the curvefs directory from being mounted under the curvefs directory
				// /a/b/c:
				// test-1 mount in /a
				// test-1 mount in /a/b
				// warmup /a/b/c.
				lCmd.MountpointInfo = mountpointInfo
			}
		}
	}
	return nil
}

func (lCmd *ListCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&lCmd.FinalCurveCmd, lCmd)
}

func (lCmd *ListCommand) RunCommand(cmd *cobra.Command, args []string) error {

	resultBytes, err := xattr.Get(lCmd.mountpoint, CURVEFS_WARMUP_OP_XATTR)
	resultStr := string(resultBytes)

	if err != nil {
		return err
	}

	entries := strings.Split(resultStr, ";")

	tableRows := make([]map[string]string, 0)

	if resultStr == "finished" {
		row := make(map[string]string)
		lCmd.Header = append(lCmd.Header, cobrautil.ROW_RESULT)
		row[cobrautil.ROW_RESULT] = "No tasks in queue"
		tableRows = append(tableRows, row)
	} else {
		lCmd.Header = append(lCmd.Header, cobrautil.ROW_PATH)
		lCmd.Header = append(lCmd.Header, cobrautil.ROW_PROGRESS)
		rows := make([]map[string]string, 0)
		for _, entry := range entries {
			if entry == "" {
				continue
			}
	
			parts := strings.Split(entry, ":")
	
			if len(parts) != 2 {
				return fmt.Errorf("invalid entry: %s", entry)
			}

			row := make(map[string]string)

			curvefsFilePath := parts[0]
			clientFilePath := cobrautil.CurvefsPath2ClientPath(curvefsFilePath, lCmd.MountpointInfo)

			row[cobrautil.ROW_PATH] = fmt.Sprintf("%s", clientFilePath)

			progress := parts[1]
			row[cobrautil.ROW_PROGRESS] = fmt.Sprintf("%s", progress)

			rows = append(rows, row)
		}

		tableRows = append(tableRows, rows...)
	}

	lCmd.SetHeader(lCmd.Header)
	list := cobrautil.ListMap2ListSortByKeys(tableRows, lCmd.Header, []string{})
	lCmd.TableNew.AppendBulk(list)

	return nil
}

func (lCmd *ListCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&lCmd.FinalCurveCmd)
}
