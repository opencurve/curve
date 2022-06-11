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
 * Created Date: 2022-06-06
 * Author: chengyi (Cyber-SiKu)
 */

package mds

import (
	"encoding/json"
	"fmt"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	config "github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type MdsCommand struct {
	basecmd.FinalCurveCmd
	metrics    []basecmd.Metric
	rows       []map[string]string
}

const (
	STATUS_SUBURI  = "/vars/curvefs_mds_status"
	VERSION_SUBURI = "/vars/curve_version"
)

var _ basecmd.FinalCurveCmdFunc = (*MdsCommand)(nil) // check interface

func NewMdsCommand() *cobra.Command {
	mdsCmd := &MdsCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "mds",
			Short: "get the inode usage of curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&mdsCmd.FinalCurveCmd, mdsCmd)
	return mdsCmd.Cmd
}

func (mCmd *MdsCommand) AddFlags() {
	config.AddFsMdsAddrFlag(mCmd.Cmd)
	config.AddHttpTimeoutFlag(mCmd.Cmd)
	config.AddFsMdsDummyAddrFlag(mCmd.Cmd)
}

func (mCmd *MdsCommand) Init(cmd *cobra.Command, args []string) error {
	table, err := gotable.Create("addr", "dummyAddr", "version", "status")
	if err != nil {
		cobra.CheckErr(err)
	}
	mCmd.Table = table

	// set main addr
	mainAddrs := viper.GetStringSlice(config.VIPER_CURVEFS_MDSADDR)
	for _, addr := range mainAddrs {
		if !cobrautil.IsValidAddr(addr) {
			return fmt.Errorf("invalid addr: %s", addr)
		}
	}

	// set dummy addr
	dummyAddrs := viper.GetStringSlice(config.VIPER_CURVEFS_MDSDUMMYADDR)
	for _, addr := range dummyAddrs {
		if !cobrautil.IsValidAddr(addr) {
			return fmt.Errorf("invalid dummy addr: %s", addr)
		}
		// Use the dummy port to access the metric service
		timeout := viper.GetDuration(config.VIPER_GLOBALE_HTTPTIMEOUT)

		addrs := []string{addr}
		versionMetric := basecmd.NewMetric(addrs, STATUS_SUBURI, timeout)
		mCmd.metrics = append(mCmd.metrics, *versionMetric)
		statusMetric := basecmd.NewMetric(addrs, VERSION_SUBURI, timeout)
		mCmd.metrics = append(mCmd.metrics, *statusMetric)
	}

	for i := range mainAddrs {
		row := make(map[string]string)
		row["addr"] = mainAddrs[i]
		row["dummyAddr"] = dummyAddrs[i]
		row["status"] = "offline"
		row["version"] = "unknown"
		mCmd.rows = append(mCmd.rows, row)
	}

	return nil
}

func (mCmd *MdsCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&mCmd.FinalCurveCmd, mCmd)
}

func (mCmd *MdsCommand) RunCommand(cmd *cobra.Command, args []string) error {
	results := make(chan basecmd.MetricResult, config.MaxChannelSize())
	size := 0
	for _, metric := range mCmd.metrics {
		size++
		go func(m basecmd.Metric) {
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
	for res := range results {
		for _, row := range mCmd.rows {
			if res.Err.TypeCode() == cmderror.CODE_SUCCESS && row["dummyAddr"] == res.Addr {
				row[res.Key] = res.Value
			}
		}
		count++
		if count >= size {
			break
		}
	}
	mCmd.Table.AddRows(mCmd.rows)
	jsonResult, err := mCmd.Table.JSON(0)
	if err != nil {
		cobra.CheckErr(err)
	}
	var m interface{}
	err = json.Unmarshal([]byte(jsonResult), &m)
	if err != nil {
		cobra.CheckErr(err)
	}
	mCmd.Result = m
	return nil
}

func (mCmd *MdsCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&mCmd.FinalCurveCmd, mCmd)
}
