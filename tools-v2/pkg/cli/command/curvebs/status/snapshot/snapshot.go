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
 * Created Date: 2023-04-25
 * Author: Xinlong-Chen
 */

package snapshot

import (
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	config "github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
)

const (
	snapshotExample = `$ curve bs status snapshotserver`
)

type SnapshotCommand struct {
	basecmd.FinalCurveCmd
	metrics []*basecmd.Metric
	rows    []map[string]string
	health  curveutil.ClUSTER_HEALTH_STATUS
}

const (
	STATUS_SUBURI  = "/vars/snapshotcloneserver_status"
	VERSION_SUBURI = "/vars/curve_version"
)

var (
	SnapshotCloneStatusMap = map[string]string{
		"active":  "leader",
		"standby": "follower",
	}
)

var _ basecmd.FinalCurveCmdFunc = (*SnapshotCommand)(nil) // check interface

func NewSnapshotCommand() *cobra.Command {
	return NewStatusSnapshotCommand().Cmd
}

func (sCmd *SnapshotCommand) AddFlags() {
	config.AddHttpTimeoutFlag(sCmd.Cmd)
	config.AddBsSnapshotCloneFlagOption(sCmd.Cmd)
	config.AddBsSnapshotCloneDummyFlagOption(sCmd.Cmd)
}

func (sCmd *SnapshotCommand) Init(cmd *cobra.Command, args []string) error {
	sCmd.health = curveutil.HEALTH_ERROR

	header := []string{curveutil.ROW_ADDR, curveutil.ROW_DUMMY_ADDR, curveutil.ROW_VERSION, curveutil.ROW_STATUS}
	sCmd.SetHeader(header)
	sCmd.TableNew.SetAutoMergeCellsByColumnIndex(curveutil.GetIndexSlice(
		sCmd.Header, []string{curveutil.ROW_STATUS, curveutil.ROW_VERSION},
	))

	// set main addr
	mainAddrs, addrErr := config.GetBsSnapshotAddrSlice(sCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	// set dummy addr
	dummyAddrs, addrErr := config.GetBsSnapshotDummyAddrSlice(sCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	for _, addr := range dummyAddrs {
		// Use the dummy port to access the metric service
		timeout := viper.GetDuration(config.VIPER_GLOBALE_HTTPTIMEOUT)

		addrs := []string{addr}
		statusMetric := basecmd.NewMetric(addrs, STATUS_SUBURI, timeout)
		sCmd.metrics = append(sCmd.metrics, statusMetric)
		versionMetric := basecmd.NewMetric(addrs, VERSION_SUBURI, timeout)
		sCmd.metrics = append(sCmd.metrics, versionMetric)
	}

	for i := range mainAddrs {
		row := make(map[string]string)
		row[curveutil.ROW_ADDR] = mainAddrs[i]
		row[curveutil.ROW_DUMMY_ADDR] = dummyAddrs[i]
		row[curveutil.ROW_STATUS] = curveutil.ROW_VALUE_OFFLINE
		row[curveutil.ROW_VERSION] = curveutil.ROW_VALUE_UNKNOWN
		sCmd.rows = append(sCmd.rows, row)
	}

	return nil
}

func (sCmd *SnapshotCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SnapshotCommand) RunCommand(cmd *cobra.Command, args []string) error {
	results := make(chan basecmd.MetricResult, config.MaxChannelSize())
	size := 0
	for _, metric := range sCmd.metrics {
		size++
		go func(m *basecmd.Metric) {
			result, err := basecmd.QueryMetric(m)

			var key string
			if m.SubUri == STATUS_SUBURI {
				key = "status"
			} else {
				key = "version"
			}

			var value string
			if err.TypeCode() == cmderror.CODE_SUCCESS {
				value, err = basecmd.GetMetricValue(result)
			}

			results <- basecmd.MetricResult{
				Addr:  m.Addrs[0],
				Key:   key,
				Value: value,
				Err:   err,
			}
		}(metric)
	}

	count := 0
	var errs []*cmderror.CmdError
	var recordAddrs []string
	for res := range results {
		for _, row := range sCmd.rows {
			if res.Err.TypeCode() == cmderror.CODE_SUCCESS && row[curveutil.ROW_DUMMY_ADDR] == res.Addr {
				if res.Key == "status" {
					row[res.Key] = SnapshotCloneStatusMap[res.Value]
				} else {
					row[res.Key] = res.Value
				}
			} else if res.Err.TypeCode() != cmderror.CODE_SUCCESS {
				index := slices.Index(recordAddrs, res.Addr)
				if index == -1 {
					errs = append(errs, res.Err)
					recordAddrs = append(recordAddrs, res.Addr)
				}
			}
		}
		count++
		if count >= size {
			break
		}
	}

	if len(errs) > 0 && len(errs) < len(sCmd.rows) {
		sCmd.health = curveutil.HEALTH_WARN
	} else if len(errs) == 0 {
		sCmd.health = curveutil.HEALTH_OK
	}

	mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	sCmd.Error = mergeErr
	list := curveutil.ListMap2ListSortByKeys(sCmd.rows, sCmd.Header, []string{
		curveutil.ROW_STATUS, curveutil.ROW_VERSION,
	})
	sCmd.TableNew.AppendBulk(list)
	sCmd.Result = sCmd.rows

	return nil
}

func (sCmd *SnapshotCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func NewStatusSnapshotCommand() *SnapshotCommand {
	snapshotCmd := &SnapshotCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "snapshotserver",
			Short:   "get the snapshot clone status of curvebs",
			Example: snapshotExample,
		},
	}
	basecmd.NewFinalCurveCli(&snapshotCmd.FinalCurveCmd, snapshotCmd)
	return snapshotCmd
}

func GetSnapshotStatus(caller *cobra.Command) (*interface{}, *cmderror.CmdError, curveutil.ClUSTER_HEALTH_STATUS) {
	snapshotCmd := NewStatusSnapshotCommand()
	snapshotCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, snapshotCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_SNAPSHOTADDR,
	})
	snapshotCmd.Cmd.SilenceErrors = true
	err := snapshotCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetSnapshotServerStatus()
		retErr.Format(err.Error())
		return nil, retErr, curveutil.HEALTH_ERROR
	}
	return &snapshotCmd.Result, cmderror.Success(), snapshotCmd.health
}
