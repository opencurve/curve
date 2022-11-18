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
 * Created Date: 2022-08-26
 * Author: chengyi (Cyber-SiKu)
 */
package config

import (
	"strings"

	"github.com/gookit/color"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// curvebs
	CURVEBS_MDSADDR             = "mdsaddr"
	VIPER_CURVEBS_MDSADDR       = "curvebs.mdsAddr"
	CURVEBS_MDSDUMMYADDR        = "mdsdummyaddr"
	VIPER_CURVEBS_MDSDUMMYADDR  = "curvebs.mdsDummyAddr"
	CURVEBS_ETCDADDR            = "etcdaddr"
	VIPER_CURVEBS_ETCDADDR      = "curvebs.etcdAddr"
	CURVEBS_PATH                = "path"
	VIPER_CURVEBS_PATH          = "curvebs.path"
	CURVEBS_USER                = "user"
	VIPER_CURVEBS_USER          = "curvebs.root.user"
	CURVEBS_DEFAULT_USER        = "root"
	CURVEBS_PASSWORD            = "password"
	VIPER_CURVEBS_PASSWORD      = "curvebs.root.password"
	CURVEBS_DEFAULT_PASSWORD    = "root_password"
	CURVEBS_FILENAME            = "filename"
	VIPER_CURVEBS_FILENAME      = "curvebs.filename"
	CURVEBS_FORCEDELETE         = "forcedelete"
	CURVEBS_DEFAULT_FORCEDELETE = "false"
	CURVEBS_DIR                 = "dir"
	VIPER_CURVEBS_DIR           = "curvebs.dir"
)

var (
	BSFLAG2VIPER = map[string]string{
		// global
		RPCTIMEOUT:    VIPER_GLOBALE_RPCTIMEOUT,
		RPCRETRYTIMES: VIPER_GLOBALE_RPCRETRYTIMES,

		// bs
		CURVEBS_MDSADDR:      VIPER_CURVEBS_MDSADDR,
		CURVEBS_MDSDUMMYADDR: VIPER_CURVEBS_MDSDUMMYADDR,
		CURVEBS_PATH:         VIPER_CURVEBS_PATH,
		CURVEBS_USER:         VIPER_CURVEBS_USER,
		CURVEBS_PASSWORD:     VIPER_CURVEBS_PASSWORD,
		CURVEBS_ETCDADDR:     VIPER_CURVEBS_ETCDADDR,
		CURVEBS_DIR:          VIPER_CURVEBS_DIR,
	}

	BSFLAG2DEFAULT = map[string]interface{}{
		// bs
		CURVEBS_USER:        CURVEBS_DEFAULT_USER,
		CURVEBS_PASSWORD:    CURVEBS_DEFAULT_PASSWORD,
		CURVEBS_FORCEDELETE: CURVEBS_DEFAULT_FORCEDELETE,
	}
)

// curvebs
// add bs option flag
func AddBsStringSliceOptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := BSFLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = []string{}
	}
	cmd.Flags().StringSlice(name, defaultValue.([]string), usage)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsStringOptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = ""
	}
	cmd.Flags().String(name, defaultValue.(string), usage)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// add bs required flag
func AddBsStringRequiredFlag(cmd *cobra.Command, name string, usage string) {
	cmd.Flags().String(name, "", usage+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsBoolOptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = false
	}
	cmd.Flags().Bool(name, defaultValue.(bool), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// add flag option
// bs mds[option]
func AddBsMdsFlagOption(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_MDSADDR, "mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702")
}
func AddBsMdsDummyFlagOption(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_MDSDUMMYADDR, "mds dummy address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702")
}

// user
func AddBsUserOptionFlag(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_USER, "user name")
}

// password
func AddBsPasswordOptionFlag(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_PASSWORD, "user password")
}

// dir
func AddBsDirOptionFlag(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_DIR, "directory path")
}

// etcd
func AddBsEtcdAddrFlag(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_ETCDADDR, "etcd address, should be like 127.0.0.1:8700,127.0.0.1:8701,127.0.0.1:8702")
}

// add flag required
// add path[required]
func AddBsPathRequiredFlag(cmd *cobra.Command) {
	AddBsStringRequiredFlag(cmd, CURVEBS_PATH, "file path")
}

func AddBsUsernameRequiredFlag(cmd *cobra.Command) {
	AddBsStringRequiredFlag(cmd, CURVEBS_USER, "username")
}

func AddBsFilenameRequiredFlag(cmd *cobra.Command) {
	AddBsStringRequiredFlag(cmd, CURVEBS_FILENAME, "the full path of file")
}

func AddBsForceDeleteOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_FORCEDELETE, "whether to force delete the file")
}

// get stingslice flag
func GetBsFlagStringSlice(cmd *cobra.Command, flagName string) []string {
	var value []string
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetStringSlice(flagName)
	} else {
		value = viper.GetStringSlice(BSFLAG2VIPER[flagName])
	}
	return value
}

// get string flag
func GetBsFlagString(cmd *cobra.Command, flagName string) string {
	var value string
	if cmd.Flag(flagName).Changed {
		value = cmd.Flag(flagName).Value.String()
	} else {
		value = viper.GetString(BSFLAG2VIPER[flagName])
	}
	return value
}

// get mdsaddr
func GetBsAddrSlice(cmd *cobra.Command, addrType string) ([]string, *cmderror.CmdError) {
	var addrsStr string
	if cmd.Flag(addrType).Changed {
		addrsStr = cmd.Flag(addrType).Value.String()
	} else {
		addrsStr = viper.GetString(BSFLAG2VIPER[addrType])
	}
	addrslice := strings.Split(addrsStr, ",")
	for _, addr := range addrslice {
		if !cobrautil.IsValidAddr(addr) {
			err := cmderror.ErrGetAddr()
			err.Format(addrType, addr)
			return addrslice, err
		}
	}
	return addrslice, cmderror.ErrSuccess()
}

func GetBsEtcdAddrSlice(cmd *cobra.Command) ([]string, *cmderror.CmdError) {
	return GetBsAddrSlice(cmd, CURVEBS_ETCDADDR)
}
func GetBsMdsAddrSlice(cmd *cobra.Command) ([]string, *cmderror.CmdError) {
	return GetBsAddrSlice(cmd, CURVEBS_MDSADDR)
}

func GetBsMdsDummyAddrSlice(cmd *cobra.Command) ([]string, *cmderror.CmdError) {
	return GetBsAddrSlice(cmd, CURVEBS_MDSDUMMYADDR)
}
