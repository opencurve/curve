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
 * Created Date: 2022-05-09
 * Author: chengyi (Cyber-SiKu)
 */
package config

import (
	"os"
	"time"

	"github.com/gookit/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	ConfPath string // config file path
)

func InitConfig() {
	if ConfPath != "" {
		viper.SetConfigFile(ConfPath)
	} else {
		// using home directory and /etc/curve as default configuration file path
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)
		viper.AddConfigPath(home + "/.curve")
		viper.AddConfigPath("/etc/curve")
		viper.SetConfigType("yaml")
		viper.SetConfigName("curve")
	}

	// viper.SetDefault("format", "plain")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		cobra.CheckErr(err)
	}
}

func AddHttpTimeoutFlag(cmd *cobra.Command) {
	cmd.Flags().Duration("httptimeout", 500*time.Millisecond, "http timeout")
	err := viper.BindPFlag("global.httpTimeout", cmd.Flags().Lookup("httptimeout"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddRpcTimeoutFlag(cmd *cobra.Command) {
	cmd.Flags().Duration("rpctimeout", 10000*time.Millisecond, "rpc timeout")
	err := viper.BindPFlag("global.rpcTimeout", cmd.Flags().Lookup("rpctimeout"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddRpcRetryTimesFlag(cmd *cobra.Command) {
	cmd.Flags().Int32("rpcretrtytimes", 1, "rpc retry times")
	err := viper.BindPFlag("global.rpcRetryTimes", cmd.Flags().Lookup("rpcretrtytimes"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// config file and command line flag are merged
func AddFsMdsAddrFlag(cmd *cobra.Command) {
	cmd.Flags().String("mdsaddr", "", "mds address, should be like 127.0.0.1:7700,127.0.0.1:7701,127.0.0.1:7702")
	err := viper.BindPFlag("curvefs.mdsAddr", cmd.Flags().Lookup("mdsaddr"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// config file and command line flag are merged
func AddShowErrorPFlag(cmd *cobra.Command) {
	cmd.PersistentFlags().Bool("showerror", false, "display all errors in command")
	err := viper.BindPFlag("global.showError", cmd.PersistentFlags().Lookup("showerror"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddFsMdsDummyAddrFlag(cmd *cobra.Command) {
	cmd.Flags().String("mdsdummyaddr", "", "mds dummy address, should be like 127.0.0.1:7700,127.0.0.1:7701,127.0.0.1:7702")
	err := viper.BindPFlag("curvefs.mdsdummyaddr", cmd.Flags().Lookup("mdsdummyaddr"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// command line flag
func AddFsIdFlag(cmd *cobra.Command) {
	cmd.Flags().String("fsid", "", "fs Id, should be like 1,2,3 "+color.Red.Sprint("[required]"))
	err := viper.BindPFlag("curvefs.fsId", cmd.Flags().Lookup("fsid"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// channel size
func MaxChannelSize() int {
	return viper.GetInt("global.maxChannelSize")
}

// command line flag
func AddFsIdOptionFlag(cmd *cobra.Command) {
	cmd.Flags().String("fsid", "*", "fs Id, should be like 1,2,3 not set means all fs")
	err := viper.BindPFlag("curvefs.fsId", cmd.Flags().Lookup("fsid"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

const (
	// global
	VIPER_GLOBALE_SHOWERROR     = "global.showError"
	VIPER_GLOBALE_HTTPTIMEOUT   = "global.httpTimeout"
	VIPER_GLOBALE_RPCTIMEOUT    = "global.rpcTimeout"
	VIPER_GLOBALE_RPCRETRYTIMES = "global.rpcRetryTimes"

	// curvefs
	VIPER_CURVEFS_MDSADDR = "curvefs.mdsAddr"
)
