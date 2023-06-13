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
 * Created Date: 2023-04-27
 * Author: Xinlong-Chen
 */

package client

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/client"
	config "github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
)

const (
	clientExample = `$ curve bs status client`
)

type ClientCommand struct {
	basecmd.FinalCurveCmd
	metrics []*basecmd.Metric
	rows    []map[string]string
}

const (
	PROCESS_CMD_SUBURI = "/vars/process_cmdline"
	VERSION_SUBURI     = "/vars/curve_version"
)

const (
	PROCESS_CMD_KEY = "process"
	VERSION_KEY     = "version"
	COUNT_KEY       = "count"
)

const (
	kProcessNebdServer string = "nebd-server"
	kProcessQemu       string = "qemu"
	kProcessPython     string = "python"
	kProcessOther      string = "other"
)

var _ basecmd.FinalCurveCmdFunc = (*ClientCommand)(nil) // check interface

func NewClientCommand() *cobra.Command {
	return NewStatusClientCommand().Cmd
}

func (cCmd *ClientCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddHttpTimeoutFlag(cCmd.Cmd)
}

func (cCmd *ClientCommand) Init(cmd *cobra.Command, args []string) error {
	header := []string{cobrautil.ROW_TYPE, cobrautil.ROW_VERSION, cobrautil.ROW_ADDR, cobrautil.ROW_NUM}
	cCmd.SetHeader(header)
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		cCmd.Header, []string{cobrautil.ROW_TYPE, cobrautil.ROW_VERSION, cobrautil.ROW_NUM},
	))

	// get client list
	results, err := client.GetClientList(cmd)
	if err.Code != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	if len((*results).([]map[string]string)) == 0 {
		return nil
	}

	clientAddr := make([]string, 0)
	for _, res := range (*results).([]map[string]string) {
		clientAddr = append(clientAddr, res[cobrautil.ROW_IP]+":"+res[cobrautil.ROW_PORT])
	}

	// Init RPC
	// Split client lists to different process
	// count version for each process
	for _, addr := range clientAddr {
		timeout := viper.GetDuration(config.VIPER_GLOBALE_HTTPTIMEOUT)

		addrs := []string{addr}
		statusMetric := basecmd.NewMetric(addrs, PROCESS_CMD_SUBURI, timeout)
		cCmd.metrics = append(cCmd.metrics, statusMetric)
		versionMetric := basecmd.NewMetric(addrs, VERSION_SUBURI, timeout)
		cCmd.metrics = append(cCmd.metrics, versionMetric)
	}

	return nil
}

func (cCmd *ClientCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *ClientCommand) RunCommand(cmd *cobra.Command, args []string) error {
	if len(cCmd.metrics) == 0 {
		return nil
	}

	// run metrics request
	results := make(chan basecmd.MetricResult, config.MaxChannelSize())
	size := 0
	for _, metric := range cCmd.metrics {
		size++
		go func(m *basecmd.Metric) {
			result, err := basecmd.QueryMetric(m)
			var key string
			if m.SubUri == PROCESS_CMD_SUBURI {
				key = PROCESS_CMD_KEY
			} else {
				key = VERSION_KEY
			}
			var value string
			if err.TypeCode() == cmderror.CODE_SUCCESS {
				value, err = basecmd.GetMetricValue(result)
				if m.SubUri == PROCESS_CMD_SUBURI {
					value = cCmd.GetProcessNameFromProcessResp(&value)
				}
			}
			results <- basecmd.MetricResult{
				Addr:  m.Addrs[0],
				Key:   key,
				Value: value,
				Err:   err,
			}
		}(metric)
	}

	clientsInfo := map[string]map[string]string{}
	count := 0
	var errs []*cmderror.CmdError
	var recordAddrs []string
	for res := range results {
		if res.Err.TypeCode() != cmderror.CODE_SUCCESS {
			index := slices.Index(recordAddrs, res.Addr)
			if index == -1 {
				errs = append(errs, res.Err)
				recordAddrs = append(recordAddrs, res.Addr)
			}
		} else if _, ok := clientsInfo[res.Addr]; ok {
			clientsInfo[res.Addr][res.Key] = res.Value
		} else {
			clientsInfo[res.Addr] = make(map[string]string)
			clientsInfo[res.Addr][res.Key] = res.Value
		}
		count++
		if count >= size {
			break
		}
	}

	// process type => version => addrs
	clientsAddrs := map[string]map[string][]string{}
	for addr, mp := range clientsInfo {
		if _, ok := clientsAddrs[mp[PROCESS_CMD_KEY]]; !ok {
			clientsAddrs[mp[PROCESS_CMD_KEY]] = make(map[string][]string)
		}
		if _, ok := clientsAddrs[mp[PROCESS_CMD_KEY]][mp[VERSION_KEY]]; !ok {
			clientsAddrs[mp[PROCESS_CMD_KEY]][mp[VERSION_KEY]] = make([]string, 0)
		}
		clientsAddrs[mp[PROCESS_CMD_KEY]][mp[VERSION_KEY]] = append(clientsAddrs[mp[PROCESS_CMD_KEY]][mp[VERSION_KEY]], addr)
	}

	for process_type, mp := range clientsAddrs {
		for version, addrs := range mp {
			for _, addr := range addrs {
				row := make(map[string]string)
				row[cobrautil.ROW_TYPE] = process_type
				row[cobrautil.ROW_VERSION] = version
				row[cobrautil.ROW_ADDR] = addr
				row[cobrautil.ROW_NUM] = strconv.Itoa(len(addrs))
				cCmd.rows = append(cCmd.rows, row)
			}
		}
	}

	mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	cCmd.Error = mergeErr
	list := cobrautil.ListMap2ListSortByKeys(cCmd.rows, cCmd.Header, []string{
		cobrautil.ROW_TYPE, cobrautil.ROW_VERSION, cobrautil.ROW_ADDR,
	})
	cCmd.TableNew.AppendBulk(list)
	cCmd.Result = cCmd.rows

	return nil
}

func (cCmd *ClientCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *ClientCommand) GetProcessNameFromProcessResp(process_resp *string) string {
	if find := strings.Contains(*process_resp, kProcessNebdServer); find {
		return kProcessNebdServer
	} else if find := strings.Contains(*process_resp, kProcessPython); find {
		return kProcessPython
	} else if find := strings.Contains(*process_resp, kProcessQemu); find {
		return kProcessQemu
	} else {
		return kProcessOther
	}
}

func NewStatusClientCommand() *ClientCommand {
	clientCmd := &ClientCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "client",
			Short:   "get status of client",
			Example: clientExample,
		},
	}
	basecmd.NewFinalCurveCli(&clientCmd.FinalCurveCmd, clientCmd)
	return clientCmd
}

func GetClientStatus(caller *cobra.Command) (*interface{}, *tablewriter.Table, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS) {
	cCmd := NewStatusClientCommand()
	cCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, cCmd.Cmd, []string{config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR})
	cCmd.Cmd.SilenceErrors = true
	err := cCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetClientStatus()
		retErr.Format(err.Error())
		return nil, nil, retErr, cobrautil.HEALTH_ERROR
	}
	return &cCmd.Result, cCmd.TableNew, cmderror.Success(), cobrautil.HEALTH_OK
}
