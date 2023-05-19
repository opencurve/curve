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
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
)

var (
	ConfPath string // config file path
)

const (
	FORMAT = "format"
	// global
	SHOWERROR                      = "showerror"
	VIPER_GLOBALE_SHOWERROR        = "global.showError"
	HTTPTIMEOUT                    = "httptimeout"
	VIPER_GLOBALE_HTTPTIMEOUT      = "global.httpTimeout"
	DEFAULT_HTTPTIMEOUT            = 500 * time.Millisecond
	RPCTIMEOUT                     = "rpctimeout"
	VIPER_GLOBALE_RPCTIMEOUT       = "global.rpcTimeout"
	DEFAULT_RPCTIMEOUT             = 10000 * time.Millisecond
	RPCRETRYTIMES                  = "rpcretrytimes"
	VIPER_GLOBALE_RPCRETRYTIMES    = "global.rpcRetryTimes"
	DEFAULT_RPCRETRYTIMES          = int32(1)
	VERBOSE                        = "verbose"
	VIPER_GLOBALE_VERBOSE          = "global.verbose"
	DEFAULT_VERBOSE                = false
	MAX_CHANNEL_SIZE               = "maxChannelSize"
	VIPER_GLOBALE_MAX_CHANNEL_SIZE = "global.maxChannelSize"
	DEFAULT_MAX_CHANNEL_SIZE       = int32(4)
)

var (
	FLAFG_GLOBAL = []string{
		SHOWERROR, HTTPTIMEOUT, RPCTIMEOUT, RPCRETRYTIMES, VERBOSE,
	}
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

// global
// format
const (
	FORMAT_JSON  = "json"
	FORMAT_PLAIN = "plain"
	FORMAT_NOOUT = "noout"
)

func AddFormatFlag(cmd *cobra.Command) {
	cmd.PersistentFlags().StringP("format", "f", FORMAT_PLAIN, "output format (json|plain)")
	err := viper.BindPFlag("format", cmd.Flags().Lookup("format"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// http timeout
func AddHttpTimeoutFlag(cmd *cobra.Command) {
	cmd.Flags().Duration("httptimeout", 500*time.Millisecond, "http timeout")
	err := viper.BindPFlag("global.httpTimeout", cmd.Flags().Lookup("httptimeout"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// rpc time out [option]
func AddRpcTimeoutFlag(cmd *cobra.Command) {
	AddDurationOptionFlag(cmd, RPCTIMEOUT, "rpc timeout")
}

// rpc retry times
func AddRpcRetryTimesFlag(cmd *cobra.Command) {
	AddInt32OptionFlag(cmd, RPCRETRYTIMES, "rpc retry times")
}

// channel size
func MaxChannelSize() int {
	return viper.GetInt(VIPER_GLOBALE_MAX_CHANNEL_SIZE)
}

// show errors
func AddShowErrorPFlag(cmd *cobra.Command) {
	cmd.PersistentFlags().Bool(SHOWERROR, false, "display all errors in command")
	err := viper.BindPFlag(VIPER_GLOBALE_SHOWERROR, cmd.PersistentFlags().Lookup(SHOWERROR))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// Align the flag (changed) in the caller with the callee
func AlignFlagsValue(caller *cobra.Command, callee *cobra.Command, flagNames []string) {
	callee.Flags().VisitAll(func(flag *pflag.Flag) {
		index := slices.IndexFunc(flagNames, func(i string) bool {
			return flag.Name == i
		})
		if index == -1 {
			return
		}
		callerFlag := caller.Flag(flag.Name)
		if callerFlag != nil && callerFlag.Changed {
			if flag.Value.Type() == callerFlag.Value.Type() {
				flag.Value = callerFlag.Value
				flag.Changed = callerFlag.Changed
			} else {
				flag.Value.Set(callerFlag.Value.String())
				flag.Changed = callerFlag.Changed
			}
		}
	})
	// golobal flag
	for _, flagName := range FLAFG_GLOBAL {
		callerFlag := caller.Flag(flagName)
		if callerFlag != nil {
			if callee.Flag(flagName) != nil {
				callee.Flag(flagName).Value = callerFlag.Value
				callee.Flag(flagName).Changed = callerFlag.Changed
			} else {
				callee.Flags().AddFlag(callerFlag)
			}
		}
	}
}

type stringSlice struct {
	value []string
	change bool
}

func (s *stringSlice) String() string {
	return strings.Join(s.value, ",")
}

func (s *stringSlice) Set(value string) error {
	s.value = strings.Split(value, ",")
	s.change = true
	return nil
}

func (s *stringSlice) Type() string {
	return "stringSlice"
}

func ResetStringSliceFlag(flag *pflag.Flag, value string) {
	flag.Changed = false
	flag.Value = &stringSlice{
		value: strings.Split(value, ","),
		change: true,
	}
	flag.Changed = true
}

func GetFlagChanged(cmd *cobra.Command, flagName string) bool {
	flag := cmd.Flag(flagName)
	if flag != nil {
		return flag.Changed
	}
	return false
}

const (
	IP_PORT_REGEX = "((\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5]):([0-9]|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5]))|(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])"
)

func IsValidAddr(addr string) bool {
	matched, err := regexp.MatchString(IP_PORT_REGEX, addr)
	if err != nil || !matched {
		return false
	}
	return true
}
