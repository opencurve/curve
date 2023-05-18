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
* Created Date: 2023-05-05
* Author: chengyi01
 */

package operator

import (
	"strconv"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	operatorExample = `$ curve bs check operator --op=operator|change_peer|add_peer|remove_peer|transfer_leader`
)

const (
	CHECK_SLEEP_TIME = 1 * time.Second
)

type OperatorCommand struct {
	basecmd.FinalCurveCmd
	metric    *basecmd.Metric
	checkTime time.Duration
	row       map[string]string
}

var _ basecmd.FinalCurveCmdFunc = (*OperatorCommand)(nil) // check interface

func NewOperatorCommand() *cobra.Command {
	return NewCheckOperatorCommand().Cmd
}

func (oCmd *OperatorCommand) AddFlags() {
	config.AddHttpTimeoutFlag(oCmd.Cmd)
	config.AddBsMdsFlagOption(oCmd.Cmd)
	config.AddBsOpRequiredFlag(oCmd.Cmd)
	config.AddBsCheckTimeOptionFlag(oCmd.Cmd)
}

func (oCmd *OperatorCommand) Init(cmd *cobra.Command, args []string) error {
	opName := config.GetBsFlagString(cmd, config.CURVEBS_OP)
	opType, cmdErr := curveutil.SupportOpName(opName)
	if cmdErr.TypeCode() != cmderror.CODE_SUCCESS {
		return cmdErr.ToError()
	}
	if oCmd.Cmd.Flags().Changed(config.CURVEBS_CHECK_TIME) {
		oCmd.checkTime = config.GetBsFlagDuration(cmd, config.CURVEBS_CHECK_TIME)
	} else {
		oCmd.checkTime = curveutil.GetDefaultCheckTime(opType)
	}
	addrs, cmdErr := config.GetBsMdsAddrSlice(oCmd.Cmd)
	if cmdErr.TypeCode() != cmderror.CODE_SUCCESS {
		return cmdErr.ToError()
	}
	timeout := config.GetBsFlagDuration(oCmd.Cmd, config.HTTPTIMEOUT)
	subUri := curveutil.GetOpNumSubUri(opName)
	oCmd.metric = basecmd.NewMetric(addrs, subUri, timeout)

	oCmd.SetHeader([]string{curveutil.ROW_OPNAME, curveutil.ROW_NUM})
	oCmd.row = make(map[string]string)
	oCmd.row[curveutil.ROW_OPNAME] = opName

	return nil
}

func (oCmd *OperatorCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&oCmd.FinalCurveCmd, oCmd)
}

func (oCmd *OperatorCommand) RunCommand(cmd *cobra.Command, args []string) error {
	start := time.Now()
	for {
		res, metricErr := basecmd.QueryMetric(oCmd.metric)
		if metricErr.TypeCode() != cmderror.CODE_SUCCESS {
			return metricErr.ToError()
		}
		resValueStr, metricErr := basecmd.GetMetricValue(res)
		if metricErr.TypeCode() != cmderror.CODE_SUCCESS {
			return metricErr.ToError()
		}
		resValue, err := strconv.ParseUint(resValueStr, 10, 64)
		if err != nil {
			metricErr := cmderror.ErrParseMetric()
			metricErr.Format(resValueStr)
			return metricErr.ToError()
		}
		oCmd.row[curveutil.ROW_NUM] = resValueStr
		if resValue != 0 || time.Since(start) >= oCmd.checkTime {
			break
		}
		time.Sleep(CHECK_SLEEP_TIME)
	}
	list := curveutil.Map2List(oCmd.row, oCmd.Header)
	oCmd.TableNew.Append(list)
	return nil
}

func (oCmd *OperatorCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&oCmd.FinalCurveCmd)
}

func NewCheckOperatorCommand() *OperatorCommand {
	operatorCmd := &OperatorCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "operator",
			Short: "check the operators",
			// Example: operatorExample,
		}}
	basecmd.NewFinalCurveCli(&operatorCmd.FinalCurveCmd, operatorCmd)
	return operatorCmd
}
