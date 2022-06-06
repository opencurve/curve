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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	config "github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type EtcdCommand struct {
	basecmd.FinalCurveCmd
	metrics []basecmd.Metric
	rows    []map[string]string
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

func NewEtcdCommand() *cobra.Command {
	etcdCmd := &EtcdCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "etcd",
			Short: "get the etcd status of curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&etcdCmd.FinalCurveCmd, etcdCmd)
	return etcdCmd.Cmd
}

func (eCmd *EtcdCommand) AddFlags() {
	config.AddHttpTimeoutFlag(eCmd.Cmd)
}

func (eCmd *EtcdCommand) Init(cmd *cobra.Command, args []string) error {
	table, err := gotable.Create("addr", "version", "status")
	if err != nil {
		cobra.CheckErr(err)
	}
	eCmd.Table = table

	// set main addr
	addrsStr := viper.GetString(config.VIPER_CURVEFS_ETCDADDR)
	etcdAddrs := strings.Split(addrsStr, ",")
	for _, addr := range etcdAddrs {
		if !cobrautil.IsValidAddr(addr) {
			return fmt.Errorf("invalid etcd addr: %s", addr)
		}

		// set metric
		timeout := viper.GetDuration(config.VIPER_GLOBALE_HTTPTIMEOUT)
		addrs := []string{addr}
		statusMetric := basecmd.NewMetric(addrs, STATUS_SUBURI, timeout)
		eCmd.metrics = append(eCmd.metrics, *statusMetric)
		versionMetric := basecmd.NewMetric(addrs, VERSION_SUBURI, timeout)
		eCmd.metrics = append(eCmd.metrics, *versionMetric)

		// set rows
		row := make(map[string]string)
		row["addr"] = addr
		row["status"] = "offline"
		row["version"] = "unknown"
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
	for res := range results {
		for _, row := range eCmd.rows {
			if res.Err.TypeCode() == cmderror.CODE_SUCCESS && row["addr"] == res.Addr {
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
	eCmd.Table.AddRows(eCmd.rows)
	jsonResult, err := eCmd.Table.JSON(0)
	if err != nil {
		cobra.CheckErr(err)
	}
	var m interface{}
	err = json.Unmarshal([]byte(jsonResult), &m)
	if err != nil {
		cobra.CheckErr(err)
	}
	eCmd.Result = m
	return nil
}

func (eCmd *EtcdCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&eCmd.FinalCurveCmd, eCmd)
}
