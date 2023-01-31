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
 * Created Date: 2022-08-10
 * Author: chengyi (Cyber-SiKu)
 */

package query

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/pkg/xattr"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

const (
	queryExample = `$ curve fs warmup query /mnt/warmup `

	CURVEFS_WARMUP_OP_XATTR = "curvefs.warmup.op"
)

type QueryCommand struct {
	basecmd.FinalCurveCmd
	path     string
	interval time.Duration
}

var _ basecmd.FinalCurveCmdFunc = (*QueryCommand)(nil) // check interface

func NewQueryWarmupCommand() *QueryCommand {
	qCmd := &QueryCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "query",
			Short:   "query the warmup progress",
			Example: queryExample,
		},
	}
	basecmd.NewFinalCurveCli(&qCmd.FinalCurveCmd, qCmd)
	return qCmd
}

func NewQueryCommand() *cobra.Command {
	return NewQueryWarmupCommand().Cmd
}

func (qCmd *QueryCommand) AddFlags() {
	config.AddIntervalOptionFlag(qCmd.Cmd)
}

func (qCmd *QueryCommand) Init(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return errors.New("need a path")
	}
	qCmd.path = args[0]
	qCmd.interval = config.GetIntervalFlag(qCmd.Cmd)
	return nil
}

func (qCmd *QueryCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&qCmd.FinalCurveCmd, qCmd)
}

func (qCmd *QueryCommand) RunCommand(cmd *cobra.Command, args []string) error {
	filename := strings.Split(qCmd.path, "/")
	bar := progressbar.NewOptions64(1,
		progressbar.OptionSetDescription("[cyan]Warmup[reset] "+filename[len(filename)-1]+"..."),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
	for {
		time.Sleep(qCmd.interval)
		result, err := xattr.Get(qCmd.path, CURVEFS_WARMUP_OP_XATTR)
		if err != nil {
			return err
		}
		resultStr := string(result)
		if resultStr == "finished" {
			break
		}
		strs := strings.Split(resultStr, "/")
		if len(strs) != 2 {
			break
		}
		finished, err := strconv.ParseUint(strs[0], 10, 64)
		if err != nil {
			break
		}
		total, err := strconv.ParseUint(strs[1], 10, 64)
		if err != nil {
			break
		}
		bar.ChangeMax64(int64(total))
		bar.Set64(int64(finished))
	}
	bar.Finish()
	return nil
}

func (qCmd *QueryCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&qCmd.FinalCurveCmd)
}

func GetWarmupProgress(caller *cobra.Command, path string) *cmderror.CmdError {
	queryCmd := NewQueryWarmupCommand()
	queryCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT, path})
	config.AlignFlagsValue(caller, queryCmd.Cmd, []string{config.CURVEFS_INTERVAL})
	err := queryCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrQueryWarmup()
		retErr.Format(err.Error())
		return retErr
	}
	return cmderror.Success()
}
