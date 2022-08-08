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
 * Created Date: 2022-06-09
 * Author: chengyi (Cyber-SiKu)
 */

package etcd

import (
	"fmt"

	"github.com/olekukonko/tablewriter"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	config "github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
)

type EtcdCommand struct {
	basecmd.FinalCurveCmd
	metrics []basecmd.Metric
	rows    []map[string]string
	health  cobrautil.ClUSTER_HEALTH_STATUS
}

const (
	STATUS_SUBURI      = "/v2/stats/self"
	STATUS_METRIC_KEY  = "state"
	VERSION_SUBURI     = "/version"
	VARSION_METRIC_KEY = "etcdserver"
)

var (
	EtcdStatusMap = map[string]string{
		"StateLeader":   "leader",
		"StateFollower": "follower",
	}
)

var _ basecmd.FinalCurveCmdFunc = (*EtcdCommand)(nil) // check interface

const (
	etcdExample = `$ curve fs status etcd`
)

func NewEtcdCommand() *cobra.Command {
	return NewStatusEtcdCommand().Cmd
}

func (eCmd *EtcdCommand) AddFlags() {
	config.AddEtcdAddrFlag(eCmd.Cmd)
	config.AddHttpTimeoutFlag(eCmd.Cmd)
}

func (eCmd *EtcdCommand) Init(cmd *cobra.Command, args []string) error {
	eCmd.health = cobrautil.HEALTH_ERROR
	header := []string{cobrautil.ROW_ADDR, cobrautil.ROW_VERSION, cobrautil.ROW_STATUS}
	eCmd.SetHeader(header)
	eCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		eCmd.Header, []string{cobrautil.ROW_STATUS, cobrautil.ROW_VERSION},
	))

	// set main addr
	etcdAddrs, addrErr := config.GetFsEtcdAddrSlice(eCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	for _, addr := range etcdAddrs {
		// set metric
		timeout := viper.GetDuration(config.VIPER_GLOBALE_HTTPTIMEOUT)
		addrs := []string{addr}
		statusMetric := basecmd.NewMetric(addrs, STATUS_SUBURI, timeout)
		eCmd.metrics = append(eCmd.metrics, *statusMetric)
		versionMetric := basecmd.NewMetric(addrs, VERSION_SUBURI, timeout)
		eCmd.metrics = append(eCmd.metrics, *versionMetric)

		// set rows
		row := make(map[string]string)
		row[cobrautil.ROW_ADDR] = addr
		row[cobrautil.ROW_STATUS] = cobrautil.ROW_VALUE_OFFLINE
		row[cobrautil.ROW_VERSION] = cobrautil.ROW_VALUE_UNKNOWN
		eCmd.rows = append(eCmd.rows, row)
	}

	return nil
}

func (eCmd *EtcdCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&eCmd.FinalCurveCmd, eCmd)
}

func (eCmd *EtcdCommand) RunCommand(cmd *cobra.Command, args []string) error {
	results := make(chan basecmd.MetricResult, config.MaxChannelSize())
	size := 0
	var errs []*cmderror.CmdError
	for _, metric := range eCmd.metrics {
		size++
		go func(m basecmd.Metric) {
			result, err := basecmd.QueryMetric(m)
			var key string
			var metricKey string
			if m.SubUri == STATUS_SUBURI {
				key = "status"
				metricKey = STATUS_METRIC_KEY
			} else {
				key = "version"
				metricKey = VARSION_METRIC_KEY
			}
			var value string
			if err.TypeCode() == cmderror.CODE_SUCCESS {
				value, err = basecmd.GetKeyValueFromJsonMetric(result, metricKey)
				if err.TypeCode() != cmderror.CODE_SUCCESS {
					errs = append(errs, err)
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

	count := 0
	var recordAddrs []string
	for res := range results {
		if res.Err.TypeCode() != cmderror.CODE_SUCCESS {
			index := slices.Index(recordAddrs, res.Addr)
			if index == -1 {
				errs = append(errs, res.Err)
				recordAddrs = append(recordAddrs, res.Addr)
			}
		}
		for _, row := range eCmd.rows {
			if res.Err.TypeCode() == cmderror.CODE_SUCCESS && row[cobrautil.ROW_ADDR] == res.Addr {
				if res.Key == "status" {
					row[res.Key] = EtcdStatusMap[res.Value]
				} else {
					row[res.Key] = res.Value
				}
			}
		}
		count++
		if count >= size {
			break
		}
	}
	mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	eCmd.Error = &mergeErr

	if len(errs) > 0 && len(errs) < len(eCmd.rows) {
		eCmd.health = cobrautil.HEALTH_WARN
	} else if len(errs) == 0 {
		eCmd.health = cobrautil.HEALTH_OK
	}

	list := cobrautil.ListMap2ListSortByKeys(eCmd.rows, eCmd.Header, []string{
		cobrautil.ROW_STATUS, cobrautil.ROW_VERSION,
	})
	eCmd.TableNew.AppendBulk(list)

	eCmd.Result = eCmd.rows
	return nil
}

func (eCmd *EtcdCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&eCmd.FinalCurveCmd)
}

func NewStatusEtcdCommand() *EtcdCommand {
	etcdCmd := &EtcdCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "etcd",
			Short:   "get the etcd status of curvefs",
			Example: etcdExample,
		},
	}
	basecmd.NewFinalCurveCli(&etcdCmd.FinalCurveCmd, etcdCmd)
	return etcdCmd
}

func GetEtcdStatus(caller *cobra.Command) (*interface{}, *tablewriter.Table, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS) {
	etcdCmd := NewStatusEtcdCommand()
	etcdCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	cobrautil.AlignFlagsValue(caller, etcdCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEFS_MDSADDR,
	})
	etcdCmd.Cmd.SilenceErrors = true
	etcdCmd.Cmd.Execute()
	return &etcdCmd.Result, etcdCmd.TableNew, etcdCmd.Error, etcdCmd.health
}
