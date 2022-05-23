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
	// global
	VIPER_GLOBALE_SHOWERROR     = "global.showError"
	VIPER_GLOBALE_HTTPTIMEOUT   = "global.httpTimeout"
	VIPER_GLOBALE_RPCTIMEOUT    = "global.rpcTimeout"
	VIPER_GLOBALE_RPCRETRYTIMES = "global.rpcRetryTimes"

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
)

var (
	FLAG2VIPER = map[string]string{
		CURVEFS_MDSADDR:        VIPER_CURVEFS_MDSADDR,
		CURVEFS_MDSDUMMYADDR:   VIPER_CURVEFS_MDSDUMMYADDR,
		CURVEFS_ETCDADDR:       VIPER_CURVEFS_ETCDADDR,
		CURVEFS_METASERVERADDR: VIPER_CURVEFS_METASERVERADDR,
		CURVEFS_METASERVERID:   VIPER_CURVEFS_METASERVERID,
		CURVEFS_FSID:           VIPER_CURVEFS_FSID,
		CURVEFS_FSNAME:         VIPER_CURVEFS_FSNAME,
		CURVEFS_MOUNTPOINT:     VIPER_CURVEFS_MOUNTPOINT,
		CURVEFS_PARTITIONID:    VIPER_CURVEFS_PARTITIONID,
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
	cmd.Flags().StringP("format", "f", FORMAT_PLAIN, "Output format (json|plain)")
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

// rpc time out
func AddRpcTimeoutFlag(cmd *cobra.Command) {
	cmd.Flags().Duration("rpctimeout", 10000*time.Millisecond, "rpc timeout")
	err := viper.BindPFlag("global.rpcTimeout", cmd.Flags().Lookup("rpctimeout"))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// rpc retry times
func AddRpcRetryTimesFlag(cmd *cobra.Command) {
	cmd.Flags().Int32("rpcretrtytimes", 1, "rpc retry times")
	err := viper.BindPFlag("global.rpcRetryTimes", cmd.Flags().Lookup("rpcretrtytimes"))
	if err != nil {
		cobra.CheckErr(err)
	}
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

func GetFsMdsAddrSlice(cmd *cobra.Command) ([]string, *cmderror.CmdError) {
	return GetAddrSlice(cmd, CURVEFS_MDSADDR)
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
func AddFsIdOptionFlag(cmd *cobra.Command) {
	cmd.Flags().StringSlice(CURVEFS_FSID, nil, "fs Id, should be like 1,2,3")
	err := viper.BindPFlag(VIPER_CURVEFS_FSID, cmd.Flags().Lookup(CURVEFS_FSID))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// partition id
func AddPartitionIdRequiredFlag(cmd *cobra.Command) {
	cmd.Flags().StringSlice(CURVEFS_PARTITIONID, nil, "partition Id, should be like 1,2,3"+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(CURVEFS_PARTITIONID)
	err := viper.BindPFlag(FLAG2VIPER[CURVEFS_PARTITIONID], cmd.Flags().Lookup(CURVEFS_PARTITIONID))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// fs name [required]
func AddFsNameFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_FSNAME, "", "fs name"+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(CURVEFS_FSNAME)
	err := viper.BindPFlag(VIPER_CURVEFS_FSNAME, cmd.Flags().Lookup(CURVEFS_FSNAME))
	if err != nil {
		cobra.CheckErr(err)
	}
}

// fs name
func AddFsNameOptionFlag(cmd *cobra.Command) {
	cmd.Flags().String(CURVEFS_FSNAME, "", "fs name")
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
	cmd.Flags().String("mountpoint", "", "umount fs mountpoint"+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired("mountpoint")
	err := viper.BindPFlag("curvefs.mountpoint", cmd.Flags().Lookup("mountpoint"))
	if err != nil {
		cobra.CheckErr(err)
	}
}
