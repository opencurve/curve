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
	"time"

	"strconv"

	"github.com/gookit/color"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// curvebs
	CURVEBS_MDSADDR              = "mdsaddr"
	VIPER_CURVEBS_MDSADDR        = "curvebs.mdsAddr"
	CURVEBS_MDSDUMMYADDR         = "mdsdummyaddr"
	VIPER_CURVEBS_MDSDUMMYADDR   = "curvebs.mdsDummyAddr"
	CURVEBS_ETCDADDR             = "etcdaddr"
	VIPER_CURVEBS_ETCDADDR       = "curvebs.etcdAddr"
	CURVEBS_PATH                 = "path"
	VIPER_CURVEBS_PATH           = "curvebs.path"
	CURVEBS_DEFAULT_PATH         = "/"
	CURVEBS_USER                 = "user"
	VIPER_CURVEBS_USER           = "curvebs.root.user"
	CURVEBS_DEFAULT_USER         = "root"
	CURVEBS_PASSWORD             = "password"
	VIPER_CURVEBS_PASSWORD       = "curvebs.root.password"
	CURVEBS_DEFAULT_PASSWORD     = "root_password"
	CURVEBS_CLUSTERMAP           = "clustermap"
	VIPER_CURVEBS_CLUSTERMAP     = "curvebs.clustermap"
	CURVEBS_FILENAME             = "filename"
	VIPER_CURVEBS_FILENAME       = "curvebs.filename"
	CURVEBS_FORCEDELETE          = "forcedelete"
	CURVEBS_DEFAULT_FORCEDELETE  = false
	CURVEBS_LOGIC_POOL_ID        = "logicalpoolid"
	VIPER_CURVEBS_LOGIC_POOL_ID  = "curvebs.logicalpoolid"
	CURVEBS_COPYSET_ID           = "copysetid"
	VIPER_CURVEBS_COPYSET_ID     = "curvebs.copysetid"
	CURVEBS_PEERS_ADDRESS        = "peers"
	VIPER_CURVEBS_PEERS_ADDRESS  = "curvebs.peers"
	CURVEBS_OFFSET               = "offset"
	VIPER_CURVEBS_OFFSET         = "curvebs.offset"
	CURVEBS_SIZE                 = "size"
	VIPER_CURVEBS_SIZE           = "curvebs.size"
	CURVEBS_DEFAULT_SIZE         = uint64(10)
	CURVEBS_TYPE                 = "type"
	VIPER_CURVEBS_TYPE           = "curvebs.type"
	CURVEBS_STRIPE_UNIT          = "stripeunit"
	VIPER_CURVEBS_STRIPE_UNIT    = "curvebs.stripeunit"
	CURVEBS_DEFAULT_STRIPE_UNIT  = "32 KiB"
	CURVEBS_STRIPE_COUNT         = "stripecount"
	VIPER_CURVEBS_STRIPE_COUNT   = "curvebs.stripecount"
	CURVEBS_DEFAULT_STRIPE_COUNT = uint64(32)
	CURVEBS_LIMIT                = "limit"
	VIPER_CURVEBS_LIMIT          = "curvebs.limit"
	CURVEBS_BURST                = "burst"
	VIPER_CURVEBS_BURST          = "curvebs.burst"
	CURVEBS_DEFAULT_BURST        = uint64(30000)
	CURVEBS_BURST_LENGTH         = "burstlength"
	VIPER_CURVEBS_BURST_LENGTH   = "curvebs.burstlength"
	CURVEBS_DEFAULT_BURST_LENGTH = uint64(10)
)

var (
	BSFLAG2VIPER = map[string]string{
		// global
		RPCTIMEOUT:    VIPER_GLOBALE_RPCTIMEOUT,
		RPCRETRYTIMES: VIPER_GLOBALE_RPCRETRYTIMES,

		// bs
		CURVEBS_MDSADDR:       VIPER_CURVEBS_MDSADDR,
		CURVEBS_MDSDUMMYADDR:  VIPER_CURVEBS_MDSDUMMYADDR,
		CURVEBS_PATH:          VIPER_CURVEBS_PATH,
		CURVEBS_USER:          VIPER_CURVEBS_USER,
		CURVEBS_PASSWORD:      VIPER_CURVEBS_PASSWORD,
		CURVEBS_ETCDADDR:      VIPER_CURVEBS_ETCDADDR,
		CURVEBS_LOGIC_POOL_ID: VIPER_CURVEBS_LOGIC_POOL_ID,
		CURVEBS_COPYSET_ID:    VIPER_CURVEBS_COPYSET_ID,
		CURVEBS_PEERS_ADDRESS: VIPER_CURVEBS_PEERS_ADDRESS,
		CURVEBS_CLUSTERMAP:    VIPER_CURVEBS_CLUSTERMAP,
		CURVEBS_OFFSET:        VIPER_CURVEBS_OFFSET,
		CURVEBS_SIZE:          VIPER_CURVEBS_SIZE,
		CURVEBS_STRIPE_UNIT:   VIPER_CURVEBS_STRIPE_UNIT,
		CURVEBS_STRIPE_COUNT:  VIPER_CURVEBS_STRIPE_COUNT,
		CURVEBS_LIMIT:         VIPER_CURVEBS_LIMIT,
		CURVEBS_BURST:         VIPER_CURVEBS_BURST,
		CURVEBS_BURST_LENGTH:  VIPER_CURVEBS_BURST_LENGTH,
	}

	BSFLAG2DEFAULT = map[string]interface{}{
		// bs
		CURVEBS_USER:         CURVEBS_DEFAULT_USER,
		CURVEBS_PASSWORD:     CURVEBS_DEFAULT_PASSWORD,
		CURVEBS_FORCEDELETE:  CURVEBS_DEFAULT_FORCEDELETE,
		CURVEBS_SIZE:         CURVEBS_DEFAULT_SIZE,
		CURVEBS_STRIPE_UNIT:  CURVEBS_DEFAULT_STRIPE_UNIT,
		CURVEBS_STRIPE_COUNT: CURVEBS_DEFAULT_STRIPE_COUNT,
		CURVEBS_BURST:        CURVEBS_DEFAULT_BURST,
		CURVEBS_BURST_LENGTH: CURVEBS_DEFAULT_BURST_LENGTH,
		CURVEBS_PATH:         CURVEBS_DEFAULT_PATH,
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

func AddBsStringSliceRequiredFlag(cmd *cobra.Command, name string, usage string) {
	cmd.Flags().StringSlice(name, nil, usage+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsStringOptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := BSFLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = ""
	}
	cmd.Flags().String(name, defaultValue.(string), usage)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsUint64OptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := BSFLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = 0
	}
	cmd.Flags().Uint64(name, defaultValue.(uint64), usage)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsInt64OptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := BSFLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = 0
	}
	cmd.Flags().Int64(name, defaultValue.(int64), usage)
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
	defaultValue := BSFLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = false
	}
	cmd.Flags().Bool(name, defaultValue.(bool), usage)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsUint32RequiredFlag(cmd *cobra.Command, name string, usage string) {
	cmd.Flags().Uint32(name, uint32(0), usage+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsUint64RequiredFlag(cmd *cobra.Command, name string, usage string) {
	cmd.Flags().Uint64(name, uint64(0), usage+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
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


// etcd
func AddBsEtcdAddrFlag(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_ETCDADDR, "etcd address, should be like 127.0.0.1:8700,127.0.0.1:8701,127.0.0.1:8702")
}

func AddBsSizeOptionFlag(cmd *cobra.Command) {
	AddBsUint64OptionFlag(cmd, CURVEBS_SIZE, "size, unit is GiB")
}

func AddBsStripeUnitOptionFlag(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_STRIPE_UNIT, "stripe volume uint, just like: 32KiB")
}

func AddBsStripeCountOptionFlag(cmd *cobra.Command) {
	AddBsUint64OptionFlag(cmd, CURVEBS_STRIPE_COUNT, "stripe volume count")
}

func AddBsBurstOptionFlag(cmd *cobra.Command) {
	AddBsUint64OptionFlag(cmd, CURVEBS_BURST, "burst")
}

func AddBsBurstLengthOptionFlag(cmd *cobra.Command) {
	AddBsUint64OptionFlag(cmd, CURVEBS_BURST_LENGTH, "burst length")
}

func AddBsPathOptionFlag(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_PATH, "file or directory path")
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

func AddBSLogicalPoolIdRequiredFlag(cmd *cobra.Command) {
	AddBsUint32RequiredFlag(cmd, CURVEBS_LOGIC_POOL_ID, "logical pool id")
}

func AddBSCopysetIdRequiredFlag(cmd *cobra.Command) {
	AddBsUint32RequiredFlag(cmd, CURVEBS_COPYSET_ID, "copyset id")
}

func AddBSCopysetIdSliceRequiredFlag(cmd *cobra.Command) {
	AddBsStringSliceRequiredFlag(cmd, CURVEBS_COPYSET_ID, "copyset ids")
}

func AddBSPeersConfFlag(cmd *cobra.Command) {
	AddBsStringSliceRequiredFlag(cmd, CURVEBS_PEERS_ADDRESS, "peers info.")
}

func AddBsForceDeleteOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_FORCEDELETE, "whether to force delete the file")
}

func AddBsOffsetRequiredFlag(cmd *cobra.Command) {
	AddBsUint64RequiredFlag(cmd, CURVEBS_OFFSET, "offset")
}

func AddBsSizeRequiredFlag(cmd *cobra.Command) {
	AddBsUint64RequiredFlag(cmd, CURVEBS_SIZE, "size, uint is GiB")
}

func AddBsFileTypeRequiredFlag(cmd *cobra.Command) {
	AddBsStringRequiredFlag(cmd, CURVEBS_TYPE, "file type, file or dir")
}

func AddBsThrottleTypeRequiredFlag(cmd *cobra.Command) {
	AddBsStringRequiredFlag(cmd, CURVEBS_TYPE, "throttle type,  iops_total or iops_read or iops_write or bps_total or bps_read or bps_write")
}

func AddBsLimitRequiredFlag(cmd *cobra.Command) {
	AddBsUint64RequiredFlag(cmd, CURVEBS_LIMIT, "limit")
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

// GetBsFlagUint32 get uint32 flag
func GetBsFlagUint32(cmd *cobra.Command, flagName string) uint32 {
	var value string
	if cmd.Flag(flagName).Changed {
		value = cmd.Flag(flagName).Value.String()
	} else {
		value = viper.GetString(BSFLAG2VIPER[flagName])
	}
	val, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0
	}

	return uint32(val)
}

// get uint64 flag
func GetBsFlagUint64(cmd *cobra.Command, flagName string) uint64 {
	var value uint64
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetUint64(flagName)
	} else {
		value = viper.GetUint64(BSFLAG2VIPER[flagName])
	}
	return value
}

func GetBsFlagInt64(cmd *cobra.Command, flagName string) int64 {
	var value int64
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetInt64(flagName)
	} else {
		value = viper.GetInt64(BSFLAG2VIPER[flagName])
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

func GetBsFlagBool(cmd *cobra.Command, flagName string) bool {
	var value bool
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetBool(flagName)
	} else {
		value = viper.GetBool(BSFLAG2VIPER[flagName])
	}
	return value
}

func AddBsClusterMapRequiredFlag(cmd *cobra.Command) {
	AddStringRequiredFlag(cmd, CURVEBS_CLUSTERMAP, "clustermap file in json format")
}

func GetBsFlagDuration(cmd *cobra.Command, flagName string) time.Duration {
	var value time.Duration
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetDuration(flagName)
	} else {
		value = viper.GetDuration(BSFLAG2VIPER[flagName])
	}
	return value
}

func GetBsFlagInt32(cmd *cobra.Command, flagName string) int32 {
	var value int32
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetInt32(flagName)
	} else {
		value = viper.GetInt32(BSFLAG2VIPER[flagName])
	}
	return value
}
