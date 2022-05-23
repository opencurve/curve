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
 * Created Date: 2022-06-10
 * Author: chengyi (Cyber-SiKu)
 */

package mountpoint

import (
	"encoding/json"
	"fmt"

	"github.com/liushuochen/gotable"
	"github.com/liushuochen/gotable/table"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/list/fs"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	mds "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/mds"
	"github.com/spf13/cobra"
)

type MountpointCommand struct {
	basecmd.FinalCurveCmd
	fsInfo *mds.ListClusterFsInfoResponse
}

var _ basecmd.FinalCurveCmdFunc = (*MountpointCommand)(nil) // check interface

func NewMountpointCommand() *cobra.Command {
	mpCmd := &MountpointCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "mountpoint",
			Short: "list all mountpoint of the curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&mpCmd.FinalCurveCmd, mpCmd)
	return mpCmd.Cmd
}

func (mpCmd *MountpointCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(mpCmd.Cmd)
	config.AddRpcTimeoutFlag(mpCmd.Cmd)
	config.AddFsMdsAddrFlag(mpCmd.Cmd)
}

func (mpCmd *MountpointCommand) Init(cmd *cobra.Command, args []string) error {
	var fsInfoErr *cmderror.CmdError
	mpCmd.fsInfo, fsInfoErr = fs.GetClusterFsInfo()
	if fsInfoErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(fsInfoErr.Message)
	}
	mpCmd.Error = fsInfoErr

	table, err := gotable.Create("fs id", "fs name", "mount point")
	if err != nil {
		return err
	}
	mpCmd.Table = table
	return nil
}

func (mpCmd *MountpointCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&mpCmd.FinalCurveCmd, mpCmd)
}

func (mpCmd *MountpointCommand) RunCommand(cmd *cobra.Command, args []string) error {
	updateTable(mpCmd.Table, mpCmd.fsInfo)

	jsonResult, err := mpCmd.Table.JSON(0)
	if err != nil {
		return err
	}
	var m interface{}
	err = json.Unmarshal([]byte(jsonResult), &m)
	if err != nil {
		return err
	}
	mpCmd.Result = m
	return nil
}

func updateTable(table *table.Table, info *mds.ListClusterFsInfoResponse) {
	fssInfo := info.GetFsInfo()
	rows := make([]map[string]string, 0)
	for _, fsInfo := range fssInfo {
		if len(fsInfo.GetMountpoints()) == 0 {
			row := make(map[string]string)
			row["fs id"] = fmt.Sprintf("%d", fsInfo.GetFsId())
			row["fs name"] = fsInfo.GetFsName()
			row["mount point"] = ""
			rows = append(rows, row)
		}
		for _, mountpoint := range fsInfo.GetMountpoints() {
			row := make(map[string]string)
			row["fs id"] = fmt.Sprintf("%d", fsInfo.GetFsId())
			row["fs name"] = fsInfo.GetFsName()
			mountpointStr := fmt.Sprintf("%s:%d:%s", mountpoint.GetHostname(), mountpoint.GetPort(), mountpoint.GetPath())
			row["mount point"] = mountpointStr
			rows = append(rows, row)
		}
	}
	table.AddRows(rows)
}

func (mpCmd *MountpointCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&mpCmd.FinalCurveCmd, mpCmd)
}
