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
	"strings"
	"time"

	"github.com/gookit/color"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	ConfPath string // config file path
)

const (
	FORMAT = "format"
	// global
	VIPER_GLOBALE_SHOWERROR     = "global.showError"
	HTTPTIMEOUT                 = "httptimeout"
	VIPER_GLOBALE_HTTPTIMEOUT   = "global.httpTimeout"
	DEFAULT_HTTPTIMEOUT         = 500 * time.Millisecond
	RPCTIMEOUT                  = "rpctimeout"
	VIPER_GLOBALE_RPCTIMEOUT    = "global.rpcTimeout"
	DEFAULT_RPCTIMEOUT          = 10000 * time.Millisecond
	RPCRETRYTIMES               = "rpcretrytimes"
	VIPER_GLOBALE_RPCRETRYTIMES = "global.rpcRetryTimes"
	DEFAULT_RPCRETRYTIMES       = int32(1)

	// curvefs
	CURVEFS_MDSADDR              = "mdsaddr"
	VIPER_CURVEFS_MDSADDR        = "curvefs.mdsAddr"
	CURVEFS_MDSDUMMYADDR         = "mdsdummyaddr"
	VIPER_CURVEFS_MDSDUMMYADDR   = "curvefs.mdsDummyAddr"
	CURVEFS_ETCDADDR             = "etcdaddr"
	VIPER_CURVEFS_ETCDADDR       = "curvefs.etcdAddr"
	CURVEFS_METASERVERADDR       = "metaserveraddr"
	VIPER_CURVEFS_METASERVERADDR = "curvefs.metaserverAddr"
	CURVEFS_METASERVERID         = "metaserverid"
	VIPER_CURVEFS_METASERVERID   = "curvefs.metaserverId"
	CURVEFS_FSID                 = "fsid"
	VIPER_CURVEFS_FSID           = "curvefs.fsId"
	CURVEFS_FSNAME               = "fsname"
	VIPER_CURVEFS_FSNAME         = "curvefs.fsName"
	CURVEFS_MOUNTPOINT           = "mountpoint"
	VIPER_CURVEFS_MOUNTPOINT     = "curvefs.mountpoint"
	CURVEFS_PARTITIONID          = "partitionid"
	VIPER_CURVEFS_PARTITIONID    = "curvefs.partitionid"
	CURVEFS_NOCONFIRM            = "noconfirm"
	VIPER_CURVEFS_NOCONFIRM      = "curvefs.noconfirm"
	CURVEFS_USER                 = "user"
	VIPER_CURVEFS_USER           = "curvefs.user"
	CURVEFS_CAPACITY             = "capacity"
	VIPER_CURVEFS_CAPACITY       = "curvefs.capacity"
	CURVEFS_DEFAULT_CAPACITY     = "100 GiB"
	CURVEFS_BLOCKSIZE            = "blocksize"
	VIPER_CURVEFS_BLOCKSIZE      = "curvefs.blocksize"
	CURVEFS_DEFAULT_BLOCKSIZE    = "1 MiB"
	CURVEFS_SUMINDIR             = "sumindir"
	VIPER_CURVEFS_SUMINDIR       = "curvefs.sumindir"
	CURVEFS_DEFAULT_SUMINDIR     = false
	CURVEFS_FSTYPE               = "fstype"
	VIPER_CURVEFS_FSTYPE         = "curvefs.fstype"
	CURVEFS_COPYSETID            = "copysetid"
	VIPER_CURVEFS_COPYSETID      = "curvefs.copysetid"
	CURVEFS_POOLID               = "poolid"
	VIPER_CURVEFS_POOLID         = "curvefs.poolid"
	CURVEFS_DETAIL               = "detail"
	VIPER_CURVEFS_DETAIL         = "curvefs.detail"
	CURVEFS_DEFAULT_DETAIL       = false
	CURVEFS_INODEID              = "inodeid"
	VIPER_CURVEFS_INODEID        = "curvefs.inodeid"
	CURVEFS_CLUSTERMAP           = "clustermap"
	VIPER_CURVEFS_CLUSTERMAP     = "curvefs.clustermap"
	CURVEFS_DEFAULT_CLUSTERMAP   = "topo_example.json"
	CURVEFS_MARGIN               = "margin"
	VIPER_CURVEFS_MARGIN         = "curvefs.margin"
	CURVEFS_DEFAULT_MARGIN       = uint64(1000)
	CURVEFS_FILELIST             = "filelist"
	VIPER_CURVEFS_FILELIST       = "curvefs.filelist"

	// S3
	CURVEFS_S3_AK                 = "s3.ak"
	VIPER_CURVEFS_S3_AK           = "curvefs.s3.ak"
	CURVEFS_DEFAULT_S3_AK         = "ak"
	CURVEFS_S3_SK                 = "s3.sk"
	VIPER_CURVEFS_S3_SK           = "curvefs.s3.sk"
	CURVEFS_DEFAULT_S3_SK         = "sk"
	CURVEFS_S3_ENDPOINT           = "s3.endpoint"
	VIPER_CURVEFS_S3_ENDPOINT     = "curvefs.s3.endpoint"
	CURVEFS_DEFAULT_ENDPOINT      = "http://localhost:9000"
	CURVEFS_S3_BUCKETNAME         = "s3.bucketname"
	VIPER_CURVEFS_S3_BUCKETNAME   = "curvefs.s3.bucketname"
	CURVEFS_DEFAULT_S3_BUCKETNAME = "bucketname"
	CURVEFS_S3_BLOCKSIZE          = "s3.blocksize"
	VIPER_CURVEFS_S3_BLOCKSIZE    = "curvefs.s3.blocksize"
	CURVEFS_DEFAULT_S3_BLOCKSIZE  = "4 MiB"
	CURVEFS_S3_CHUNKSIZE          = "s3.chunksize"
	VIPER_CURVEFS_S3CHUNKSIZE     = "curvefs.s3.chunksize"
	CURVEFS_DEFAULT_S3_CHUNKSIZE  = "64 MiB"
	// Volume
	CURVEFS_VOLUME_SIZE                   = "volume.size"
	VIPER_CURVEFS_VOLUME_SIZE             = "curvefs.volume.size"
	CURVEFS_DEFAULT_VOLUME_SIZE           = "1 MiB"
	CURVEFS_VOLUME_BLOCKGROUPSIZE         = "volume.blockgroupsize"
	VIPER_CURVEFS_VOLUME_BLOCKGROUPSIZE   = "curvefs.volume.blockgroupsize"
	CURVEFS_DEFAULT_VOLUME_BLOCKGROUPSIZE = "128 MiB"
	CURVEFS_VOLUME_BLOCKSIZE              = "volume.blocksize"
	VIPER_CURVEFS_VOLUME_BLOCKSIZE        = "curvefs.volume.blocksize"
	CURVEFS_DEFAULT_VOLUME_BLOCKSIZE      = "4 KiB"
	CURVEFS_VOLUME_NAME                   = "volume.name"
	VIPER_CURVEFS_VOLUME_NAME             = "curvefs.volume.name"
	CURVEFS_DEFAULT_VOLUME_NAME           = "volume"
	CURVEFS_VOLUME_USER                   = "volume.user"
	VIPER_CURVEFS_VOLUME_USER             = "curvefs.volume.user"
	CURVEFS_DEFAULT_VOLUME_USER           = "user"
	CURVEFS_VOLUME_PASSWORD               = "volume.password"
	VIPER_CURVEFS_VOLUME_PASSWORD         = "curvefs.volume.password"
	CURVEFS_DEFAULT_VOLUME_PASSWORD       = "password"
	CURVEFS_VOLUME_BITMAPLOCATION         = "volume.bitmaplocation"
	VIPER_CURVEFS_VOLUME_BITMAPLOCATION   = "curvefs.volume.bitmaplocation"
	CURVEFS_DEFAULT_VOLUME_BITMAPLOCATION = "AtStart"
	CURVEFS_VOLUME_SLICESIZE              = "volume.slicesize"
	VIPER_CURVEFS_VOLUME_SLICESIZE        = "curvefs.volume.slicesize"
	CURVEFS_DEFAULT_VOLUME_SLICESIZE      = "1 GiB"
)

var (
	FLAG2VIPER = map[string]string{
		RPCTIMEOUT:             VIPER_GLOBALE_RPCTIMEOUT,
		RPCRETRYTIMES:          VIPER_GLOBALE_RPCRETRYTIMES,
		CURVEFS_MDSADDR:        VIPER_CURVEFS_MDSADDR,
		CURVEFS_MDSDUMMYADDR:   VIPER_CURVEFS_MDSDUMMYADDR,
		CURVEFS_ETCDADDR:       VIPER_CURVEFS_ETCDADDR,
		CURVEFS_METASERVERADDR: VIPER_CURVEFS_METASERVERADDR,
		CURVEFS_METASERVERID:   VIPER_CURVEFS_METASERVERID,
		CURVEFS_FSID:           VIPER_CURVEFS_FSID,
		CURVEFS_FSNAME:         VIPER_CURVEFS_FSNAME,
		CURVEFS_MOUNTPOINT:     VIPER_CURVEFS_MOUNTPOINT,
		CURVEFS_PARTITIONID:    VIPER_CURVEFS_PARTITIONID,
		CURVEFS_NOCONFIRM:      VIPER_CURVEFS_NOCONFIRM,
		CURVEFS_USER:           VIPER_CURVEFS_USER,
		CURVEFS_CAPACITY:       VIPER_CURVEFS_CAPACITY,
		CURVEFS_BLOCKSIZE:      VIPER_CURVEFS_BLOCKSIZE,
		CURVEFS_SUMINDIR:       VIPER_CURVEFS_SUMINDIR,
		CURVEFS_FSTYPE:         VIPER_CURVEFS_FSTYPE,
		CURVEFS_COPYSETID:      VIPER_CURVEFS_COPYSETID,
		CURVEFS_POOLID:         VIPER_CURVEFS_POOLID,
		CURVEFS_DETAIL:         VIPER_CURVEFS_DETAIL,
		CURVEFS_INODEID:        VIPER_CURVEFS_INODEID,
		CURVEFS_CLUSTERMAP:     VIPER_CURVEFS_CLUSTERMAP,
		CURVEFS_MARGIN:         VIPER_CURVEFS_MARGIN,

		// S3
		CURVEFS_S3_AK:         VIPER_CURVEFS_S3_AK,
		CURVEFS_S3_SK:         VIPER_CURVEFS_S3_SK,
		CURVEFS_S3_ENDPOINT:   VIPER_CURVEFS_S3_ENDPOINT,
		CURVEFS_S3_BUCKETNAME: VIPER_CURVEFS_S3_BUCKETNAME,
		CURVEFS_S3_BLOCKSIZE:  VIPER_CURVEFS_S3_BLOCKSIZE,
		CURVEFS_S3_CHUNKSIZE:  VIPER_CURVEFS_S3CHUNKSIZE,

		// Volume
		CURVEFS_VOLUME_SIZE:           VIPER_CURVEFS_VOLUME_SIZE,
		CURVEFS_VOLUME_BLOCKGROUPSIZE: VIPER_CURVEFS_VOLUME_BLOCKGROUPSIZE,
		CURVEFS_VOLUME_BLOCKSIZE:      VIPER_CURVEFS_VOLUME_BLOCKSIZE,
		CURVEFS_VOLUME_NAME:           VIPER_CURVEFS_VOLUME_NAME,
		CURVEFS_VOLUME_USER:           VIPER_CURVEFS_VOLUME_USER,
		CURVEFS_VOLUME_PASSWORD:       VIPER_CURVEFS_VOLUME_PASSWORD,
		CURVEFS_VOLUME_BITMAPLOCATION: VIPER_CURVEFS_VOLUME_BITMAPLOCATION,
		CURVEFS_VOLUME_SLICESIZE:      VIPER_CURVEFS_VOLUME_SLICESIZE,
	}

	FLAG2DEFAULT = map[string]interface{}{
		RPCTIMEOUT:         DEFAULT_RPCTIMEOUT,
		RPCRETRYTIMES:      DEFAULT_RPCRETRYTIMES,
		CURVEFS_SUMINDIR:   CURVEFS_DEFAULT_SUMINDIR,
		CURVEFS_DETAIL:     CURVEFS_DEFAULT_DETAIL,
		CURVEFS_CLUSTERMAP: CURVEFS_DEFAULT_CLUSTERMAP,
		CURVEFS_MARGIN:     CURVEFS_DEFAULT_MARGIN,

		// S3
		CURVEFS_S3_AK:         CURVEFS_DEFAULT_S3_AK,
		CURVEFS_S3_SK:         CURVEFS_DEFAULT_S3_SK,
		CURVEFS_S3_ENDPOINT:   CURVEFS_DEFAULT_ENDPOINT,
		CURVEFS_S3_BUCKETNAME: CURVEFS_DEFAULT_S3_BUCKETNAME,
		CURVEFS_S3_BLOCKSIZE:  CURVEFS_DEFAULT_S3_BLOCKSIZE,
		CURVEFS_S3_CHUNKSIZE:  CURVEFS_DEFAULT_S3_CHUNKSIZE,

		// Volume
		CURVEFS_VOLUME_SIZE:           CURVEFS_DEFAULT_VOLUME_SIZE,
		CURVEFS_VOLUME_BLOCKGROUPSIZE: CURVEFS_DEFAULT_VOLUME_BLOCKGROUPSIZE,
		CURVEFS_VOLUME_BLOCKSIZE:      CURVEFS_DEFAULT_VOLUME_BLOCKSIZE,
		CURVEFS_VOLUME_NAME:           CURVEFS_DEFAULT_VOLUME_NAME,
		CURVEFS_VOLUME_USER:           CURVEFS_DEFAULT_VOLUME_USER,
		CURVEFS_VOLUME_PASSWORD:       CURVEFS_DEFAULT_VOLUME_PASSWORD,
		CURVEFS_VOLUME_BITMAPLOCATION: CURVEFS_DEFAULT_VOLUME_BITMAPLOCATION,
		CURVEFS_VOLUME_SLICESIZE:      CURVEFS_DEFAULT_VOLUME_SLICESIZE,
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

func AddStringOptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = ""
	}
	cmd.Flags().String(name, defaultValue.(string), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddStringSliceOptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = []string{}
	}
	cmd.Flags().StringSlice(name, defaultValue.([]string), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddStringRequiredFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = ""
	}
	cmd.Flags().String(name, defaultValue.(string), usage+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddStringSliceRequiredFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = []string{}
	}
	cmd.Flags().StringSlice(name, defaultValue.([]string), usage+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddUint64RequiredFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = uint64(0)
	}
	cmd.Flags().Uint64(name, defaultValue.(uint64), usage+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddUint32RequiredFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = uint32(0)
	}
	cmd.Flags().Uint32(name, defaultValue.(uint32), usage+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBoolOptionFlag(cmd *cobra.Command, name string, usage string) {
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

func AddDurationOptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = 0
	}
	cmd.Flags().Duration(name, defaultValue.(time.Duration), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddInt32OptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = int32(0)
	}
	cmd.Flags().Int32(name, defaultValue.(int32), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddUint64OptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = 0
	}
	cmd.Flags().Uint64(name, defaultValue.(uint64), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddUint32OptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = 0
	}
	cmd.Flags().Uint32(name, defaultValue.(uint32), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
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
	cmd.Flags().StringP("format", "f", FORMAT_PLAIN, "output format (json|plain)")
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
	return viper.GetInt("global.maxChannelSize")
}

// show errors
func AddShowErrorPFlag(cmd *cobra.Command) {
	cmd.PersistentFlags().Bool("showerror", false, "display all errors in command")
	err := viper.BindPFlag("global.showError", cmd.PersistentFlags().Lookup("showerror"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// curvefs
// mds addr
func AddFsMdsAddrFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_MDSADDR, "", "mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702")
	err := viper.BindPFlag(VIPER_CURVEFS_MDSADDR, cmd.Flags().Lookup(CURVEFS_MDSADDR))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func GetAddrSlice(cmd *cobra.Command, addrType string) ([]string, *cmderror.CmdError) {
	var addrsStr string
	if cmd.Flag(addrType).Changed {
		addrsStr = cmd.Flag(addrType).Value.String()
	} else {
		addrsStr = viper.GetString(FLAG2VIPER[addrType])
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

func GetFlagString(cmd *cobra.Command, flagName string) string {
	var value string
	if cmd.Flag(flagName).Changed {
		value = cmd.Flag(flagName).Value.String()
	} else {
		value = viper.GetString(FLAG2VIPER[flagName])
	}
	return value
}

func GetFlagBool(cmd *cobra.Command, flagName string) bool {
	var value bool
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetBool(flagName)
	} else {
		value = viper.GetBool(FLAG2VIPER[flagName])
	}
	return value
}

func GetFlagUint64(cmd *cobra.Command, flagName string) uint64 {
	var value uint64
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetUint64(flagName)
	} else {
		value = viper.GetUint64(FLAG2VIPER[flagName])
	}
	return value
}

func GetFlagUint32(cmd *cobra.Command, flagName string) uint32 {
	var value uint32
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetUint32(flagName)
	} else {
		value = viper.GetUint32(FLAG2VIPER[flagName])
	}
	return value
}

func GetFlagStringSlice(cmd *cobra.Command, flagName string) []string {
	var value []string
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetStringSlice(flagName)
	} else {
		value = viper.GetStringSlice(FLAG2VIPER[flagName])
	}
	return value
}

func GetFlagStringSliceDefaultAll(cmd *cobra.Command, flagName string) []string {
	var value []string
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetStringSlice(flagName)
	} else {
		value = []string{"*"}
	}
	return value
}

func GetFlagDuration(cmd *cobra.Command, flagName string) time.Duration {
	var value time.Duration
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetDuration(flagName)
	} else {
		value = viper.GetDuration(FLAG2VIPER[flagName])
	}
	return value
}

func GetFlagInt32(cmd *cobra.Command, flagName string) int32 {
	var value int32
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetInt32(flagName)
	} else {
		value = viper.GetInt32(FLAG2VIPER[flagName])
	}
	return value
}

func GetFsMdsAddrSlice(cmd *cobra.Command) ([]string, *cmderror.CmdError) {
	return GetAddrSlice(cmd, CURVEFS_MDSADDR)
}

func GetRpcTimeout(cmd *cobra.Command) time.Duration {
	return GetFlagDuration(cmd, RPCTIMEOUT)
}

// mds dummy addr
func AddFsMdsDummyAddrFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_MDSDUMMYADDR, "", "mds dummy address, should be like 127.0.0.1:7700,127.0.0.1:7701,127.0.0.1:7702")
	err := viper.BindPFlag(VIPER_CURVEFS_MDSDUMMYADDR, cmd.Flags().Lookup(CURVEFS_MDSDUMMYADDR))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func GetFsMdsDummyAddrSlice(cmd *cobra.Command) ([]string, *cmderror.CmdError) {
	return GetAddrSlice(cmd, CURVEFS_MDSDUMMYADDR)
}

// etcd addr
func AddEtcdAddrFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_ETCDADDR, "", "etcd address, should be like 127.0.0.1:8700,127.0.0.1:8701,127.0.0.1:8702")
	err := viper.BindPFlag(VIPER_CURVEFS_ETCDADDR, cmd.Flags().Lookup(CURVEFS_ETCDADDR))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func GetFsEtcdAddrSlice(cmd *cobra.Command) ([]string, *cmderror.CmdError) {
	return GetAddrSlice(cmd, CURVEFS_ETCDADDR)
}

// metaserver addr
func AddMetaserverAddrOptionFlag(cmd *cobra.Command) {
	cmd.Flags().StringSlice(CURVEFS_METASERVERADDR, nil, "metaserver address, should be like 127.0.0.1:9700,127.0.0.1:9701,127.0.0.1:9702")
	err := viper.BindPFlag(VIPER_CURVEFS_METASERVERADDR, cmd.Flags().Lookup(CURVEFS_METASERVERADDR))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// metaserver id
func AddMetaserverIdOptionFlag(cmd *cobra.Command) {
	cmd.Flags().StringSlice(CURVEFS_METASERVERID, nil, "metaserver id, should be like 1,2,3")
	err := viper.BindPFlag(VIPER_CURVEFS_METASERVERID, cmd.Flags().Lookup(CURVEFS_METASERVERID))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// fs id [required]
func AddFsIdFlag(cmd *cobra.Command) {
	cmd.Flags().StringSlice(CURVEFS_FSID, nil, "fs Id, should be like 1,2,3 "+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(CURVEFS_FSID)
	err := viper.BindPFlag(VIPER_CURVEFS_FSID, cmd.Flags().Lookup(CURVEFS_FSID))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// fs id
func AddFsIdOptionDefaultAllFlag(cmd *cobra.Command) {
	cmd.Flags().StringSlice(CURVEFS_FSID, []string{"*"}, "fs Id, should be like 1,2,3 not set means all fs")
	err := viper.BindPFlag(VIPER_CURVEFS_FSID, cmd.Flags().Lookup(CURVEFS_FSID))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// fs id
func AddFsIdSliceOptionFlag(cmd *cobra.Command) {
	cmd.Flags().StringSlice(CURVEFS_FSID, nil, "fs Id, should be like 1,2,3")
	err := viper.BindPFlag(VIPER_CURVEFS_FSID, cmd.Flags().Lookup(CURVEFS_FSID))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// partition id [required]
func AddPartitionIdRequiredFlag(cmd *cobra.Command) {
	cmd.Flags().StringSlice(CURVEFS_PARTITIONID, nil, "partition Id, should be like 1,2,3"+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(CURVEFS_PARTITIONID)
	err := viper.BindPFlag(FLAG2VIPER[CURVEFS_PARTITIONID], cmd.Flags().Lookup(CURVEFS_PARTITIONID))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// fs name [required]
func AddFsNameRequiredFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_FSNAME, "", "fs name"+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(CURVEFS_FSNAME)
	err := viper.BindPFlag(VIPER_CURVEFS_FSNAME, cmd.Flags().Lookup(CURVEFS_FSNAME))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// fs name
func AddFsNameSliceOptionFlag(cmd *cobra.Command) {
	cmd.Flags().StringSlice(CURVEFS_FSNAME, nil, "fs name")
	err := viper.BindPFlag(VIPER_CURVEFS_FSNAME, cmd.Flags().Lookup(CURVEFS_FSNAME))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// mountpoint [required]
func AddMountpointFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_MOUNTPOINT, "", "umount fs mountpoint"+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(CURVEFS_MOUNTPOINT)
	err := viper.BindPFlag(VIPER_CURVEFS_MOUNTPOINT, cmd.Flags().Lookup("mountpoint"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// noconfirm
func AddNoConfirmOptionFlag(cmd *cobra.Command) {
	cmd.Flags().Bool(CURVEFS_NOCONFIRM, false, "do not confirm the command")
	err := viper.BindPFlag(VIPER_CURVEFS_NOCONFIRM, cmd.Flags().Lookup(CURVEFS_NOCONFIRM))
	if err != nil {
		cobra.CheckErr(err)
	}
}

/* option */
// User [option]
func AddUserOptionFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_USER, "anonymous", "user of request")
	err := viper.BindPFlag(VIPER_CURVEFS_USER, cmd.Flags().Lookup(CURVEFS_USER))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// fs name [option]
func AddFsNameOptionFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_FSNAME, "", "fs name")
	err := viper.BindPFlag(VIPER_CURVEFS_FSNAME, cmd.Flags().Lookup(CURVEFS_FSNAME))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// Capacity [option]
func AddCapacityOptionFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_CAPACITY, CURVEFS_DEFAULT_CAPACITY, "capacity of fs")
	err := viper.BindPFlag(VIPER_CURVEFS_CAPACITY, cmd.Flags().Lookup(CURVEFS_CAPACITY))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// BlockSize [option]
func AddBlockSizeOptionFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_BLOCKSIZE, CURVEFS_DEFAULT_BLOCKSIZE, "block size")
	err := viper.BindPFlag(VIPER_CURVEFS_BLOCKSIZE, cmd.Flags().Lookup(CURVEFS_BLOCKSIZE))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// SumInDir [option]
func AddSumInDIrOptionFlag(cmd *cobra.Command) {
	cmd.Flags().Bool(CURVEFS_SUMINDIR, false, "statistic info in xattr")
	err := viper.BindPFlag(VIPER_CURVEFS_SUMINDIR, cmd.Flags().Lookup(CURVEFS_SUMINDIR))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// Fsype [option]
func AddFsTypeOptionFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_FSTYPE, "s3", "fs type, should be: s3, volume or hybrid")
	err := viper.BindPFlag(VIPER_CURVEFS_FSTYPE, cmd.Flags().Lookup(CURVEFS_FSTYPE))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// S3.Ak [option]
func AddS3AkOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_S3_AK, "s3 ak")
}

// S3.Sk [option]
func AddS3SkOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_S3_SK, "s3 sk")
}

// S3.Endpoint [option]
func AddS3EndpointOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_S3_ENDPOINT, "s3 endpoint")
}

// S3.Buckname [option]
func AddS3BucknameOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_S3_BUCKETNAME, "s3 buckname")
}

// S3.Blocksize [option]
func AddS3BlocksizeOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_S3_BLOCKSIZE, "s3 blocksize")
}

// S3.Chunksize [option]
func AddS3ChunksizeOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_S3_CHUNKSIZE, "s3 chunksize")
}

// volume.size [option]
func AddVolumeSizeOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_VOLUME_SIZE, "volume size")
}

// volume.blockgroupsize [option]
func AddVolumeBlockgroupsizeOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_VOLUME_BLOCKGROUPSIZE, "volume block group size")
}

// volume.blocksize [option]
func AddVolumeBlocksizeOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_VOLUME_BLOCKSIZE, "volume blocksize")
}

// volume.name [option]
func AddVolumeNameOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_VOLUME_NAME, "volume name")
}

// volume.user [option]
func AddVolumeUserOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_VOLUME_USER, "volume user")
}

// volume.password [option]
func AddVolumePasswordOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_VOLUME_PASSWORD, "volume password")
}

// volume.bitmaplocation [option]
func AddVolumeBitmaplocationOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_VOLUME_BITMAPLOCATION, "volume space bitmap location, support |AtStart| and |AtEnd|")
}

// volume.slicesize [option]
func AddVolumeSlicesizeOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_VOLUME_SLICESIZE, "volume extents slice size")
}

// volume.slicesize [option]
func AddDetailOptionFlag(cmd *cobra.Command) {
	AddBoolOptionFlag(cmd, CURVEFS_DETAIL, "show more infomation")
}

// margin [option]
func AddMarginOptionFlag(cmd *cobra.Command) {
	AddUint64OptionFlag(cmd, CURVEFS_MARGIN, "the maximum gap between peers")
}

func GetMarginOptionFlag(cmd *cobra.Command) uint64 {
	return GetFlagUint64(cmd, CURVEFS_MARGIN)
}

// filelist [option]
func AddFileListOptionFlag(cmd *cobra.Command) {
	AddStringOptionFlag(cmd, CURVEFS_FILELIST,
		"filelist path, save the files(dir) to warmup absPath, and should be in curvefs")
}

func GetFileListOptionFlag(cmd *cobra.Command) string {
	return GetFlagString(cmd, CURVEFS_FILELIST)
}

/* required */

// copysetid [required]
func AddCopysetidSliceRequiredFlag(cmd *cobra.Command) {
	AddStringSliceRequiredFlag(cmd, CURVEFS_COPYSETID, "copysetid")
}

// poolid [required]
func AddPoolidSliceRequiredFlag(cmd *cobra.Command) {
	AddStringSliceRequiredFlag(cmd, CURVEFS_POOLID, "poolid")
}

// inodeid [required]
func AddInodeIdRequiredFlag(cmd *cobra.Command) {
	AddUint64RequiredFlag(cmd, CURVEFS_INODEID, "inodeid")
}

// fsid [required]
func AddFsIdRequiredFlag(cmd *cobra.Command) {
	AddUint32RequiredFlag(cmd, CURVEFS_FSID, "fsid")
}

// cluserMap [required]
func AddClusterMapRequiredFlag(cmd *cobra.Command) {
	AddStringRequiredFlag(cmd, CURVEFS_CLUSTERMAP, "clusterMap")
}

// mountpoint [required]
func AddMountpointRequiredFlag(cmd *cobra.Command) {
	AddStringRequiredFlag(cmd, CURVEFS_MOUNTPOINT, "curvefs mountpoint path")
}
