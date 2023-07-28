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
 * Created Date: 2023-08-10
 * Author: ken90242 (Ken Han)
 */

package cancel

import (
	"errors"
	"fmt"
	"os"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/warmup/query"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

const (
	cancelCommand = `$ curve fs warmup cancel --filelist /mnt/warmup/0809.list # cancel warmup task in related to the file(dir) saved in /mnt/warmup/0809.list
 $ curve fs warmup cancel /mnt/warmup # cancel the warmup tasks of all files in /mnt/warmup`
)

const (
	CURVEFS_WARMUP_OP_XATTR         = "curvefs.warmup.op"
	CURVEFS_WARMUP_OP_CANCEL_SINGLE = "cancel\nsingle"
	CURVEFS_WARMUP_OP_CANCEL_LIST   = "cancel\nlist"
)

var STORAGE_TYPE = map[string]string{
	"disk": "disk",
	"mem":  "kvclient",
}

type CancelCommand struct {
	basecmd.FinalCurveCmd
	Path   string // path in user system
	Single bool   // cancel the warmup of a single file or directory
}

var _ basecmd.FinalCurveCmdFunc = (*CancelCommand)(nil) // check interface

func NewCancelWarmupCommand() *CancelCommand {
	cCmd := &CancelCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "cancel",
			Short:   "tell client to cancel warmup files(directories) to local",
			Example: cancelCommand,
		},
	}
	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd
}

func NewCancelCommand() *cobra.Command {
	return NewCancelWarmupCommand().Cmd
}

func (cCmd *CancelCommand) AddFlags() {
	config.AddFileListOptionFlag(cCmd.Cmd)
	config.AddDaemonOptionPFlag(cCmd.Cmd)
	config.AddStorageOptionFlag(cCmd.Cmd)
}

func (cCmd *CancelCommand) Init(cmd *cobra.Command, args []string) error {
	// check has curvefs mountpoint
	mountpoints, err := cobrautil.GetCurveFSMountPoints()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	} else if len(mountpoints) == 0 {
		return errors.New("no curvefs mountpoint found")
	}

	// check args
	cCmd.Single = false
	fileList := config.GetFileListOptionFlag(cCmd.Cmd)
	if fileList == "" && len(args) == 0 {
		cmd.SilenceUsage = false
		return fmt.Errorf("no --filelist or file(dir) specified")
	} else if fileList != "" {
		cCmd.Path = fileList
	} else {
		cCmd.Path = args[0]
		cCmd.Single = true
	}

	// check file is exist
	info, errStat := os.Stat(cCmd.Path)
	if errStat != nil {
		if os.IsNotExist(errStat) {
			return fmt.Errorf("[%s]: no such file or directory", cCmd.Path)
		} else {
			return fmt.Errorf("stat [%s] fail: %s", cCmd.Path, errStat.Error())
		}
	} else if !cCmd.Single && info.IsDir() {
		// --filelist must be a file
		return fmt.Errorf("[%s]: must be a file", cCmd.Path)
	}

	return nil
}

func (cCmd *CancelCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CancelCommand) RunCommand(cmd *cobra.Command, args []string) error {
	xattr := CURVEFS_WARMUP_OP_CANCEL_SINGLE
	if !cCmd.Single {
		xattr = CURVEFS_WARMUP_OP_CANCEL_LIST
	}
	err := unix.Setxattr(cCmd.Path, CURVEFS_WARMUP_OP_XATTR, []byte(xattr), 0)
	if err == unix.ENOTSUP || err == unix.EOPNOTSUPP {
		return fmt.Errorf("filesystem does not support extended attributes")
	} else if err != nil {
		setErr := cmderror.ErrSetxattr()
		setErr.Format(CURVEFS_WARMUP_OP_XATTR, err.Error())
		return setErr.ToError()
	}
	if config.GetDaemonFlag(cCmd.Cmd) {
		query.GetWarmupProgress(cCmd.Cmd, cCmd.Path)
	}
	return nil
}

func (cCmd *CancelCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}
