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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gookit/color"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// curvebs
	CURVEBS_MDSADDR                   = "mdsaddr"
	VIPER_CURVEBS_MDSADDR             = "curvebs.mdsAddr"
	CURVEBS_MDSDUMMYADDR              = "mdsdummyaddr"
	VIPER_CURVEBS_MDSDUMMYADDR        = "curvebs.mdsDummyAddr"
	CURVEBS_ETCDADDR                  = "etcdaddr"
	VIPER_CURVEBS_ETCDADDR            = "curvebs.etcdAddr"
	CURVEBS_PATH                      = "path"
	VIPER_CURVEBS_PATH                = "curvebs.path"
	CURVEBS_DEFAULT_PATH              = "/test"
	CURVEBS_USER                      = "user"
	VIPER_CURVEBS_USER                = "curvebs.root.user"
	CURVEBS_DEFAULT_USER              = "root"
	CURVEBS_PASSWORD                  = "password"
	VIPER_CURVEBS_PASSWORD            = "curvebs.root.password"
	CURVEBS_DEFAULT_PASSWORD          = "root_password"
	CURVEBS_CLUSTERMAP                = "clustermap"
	VIPER_CURVEBS_CLUSTERMAP          = "curvebs.clustermap"
	CURVEBS_FORCE                     = "force"
	VIPER_CURVEBS_FORCE               = "curvebs.force"
	CURVEBS_DEFAULT_FORCE             = false
	CURVEBS_LOGIC_POOL_ID             = "logicalpoolid"
	VIPER_CURVEBS_LOGIC_POOL_ID       = "curvebs.logicalpoolid"
	CURVEBS_DEFAULT_LOGIC_POOL_ID     = uint32(0)
	CURVEBS_COPYSET_ID                = "copysetid"
	VIPER_CURVEBS_COPYSET_ID          = "curvebs.copysetid"
	CURVEBS_DEFAULT_COPYSET_ID        = uint32(0)
	CURVEBS_PEERS_ADDRESS             = "peers"
	VIPER_CURVEBS_PEERS_ADDRESS       = "curvebs.peers"
	CURVEBS_OFFSET                    = "offset"
	VIPER_CURVEBS_OFFSET              = "curvebs.offset"
	CURVEBS_SIZE                      = "size"
	VIPER_CURVEBS_SIZE                = "curvebs.size"
	CURVEBS_DEFAULT_SIZE              = uint64(10)
	CURVEBS_TYPE                      = "type"
	VIPER_CURVEBS_TYPE                = "curvebs.type"
	CURVEBS_STRIPE_UNIT               = "stripeunit"
	VIPER_CURVEBS_STRIPE_UNIT         = "curvebs.stripeunit"
	CURVEBS_DEFAULT_STRIPE_UNIT       = "32 KiB"
	CURVEBS_STRIPE_COUNT              = "stripecount"
	VIPER_CURVEBS_STRIPE_COUNT        = "curvebs.stripecount"
	CURVEBS_DEFAULT_STRIPE_COUNT      = uint64(32)
	CURVEBS_RECYCLE_PREFIX            = "recycleprefix"
	VIPER_RECYCLE_PREFIX              = "curvebs.recycleprefix"
	CURVEBS_EXPIRED_TIME              = "expiredtime"
	VIPER_CURVEBS_EXPIRED_TIME        = "curvebs.expiredtime"
	CURVEBS_LIMIT                     = "limit"
	VIPER_CURVEBS_LIMIT               = "curvebs.limit"
	CURVEBS_BURST                     = "burst"
	VIPER_CURVEBS_BURST               = "curvebs.burst"
	CURVEBS_DEFAULT_BURST             = uint64(30000)
	CURVEBS_BURST_LENGTH              = "burstlength"
	VIPER_CURVEBS_BURST_LENGTH        = "curvebs.burstlength"
	CURVEBS_DEFAULT_BURST_LENGTH      = uint64(10)
	CURVEBS_OP                        = "op"
	VIPER_CURVEBS_OP                  = "curvebs.op"
	CURVEBS_DEFAULT_OP                = "operator"
	CURVEBS_CHECK_TIME                = "checktime"
	VIPER_CURVEBS_CHECK_TIME          = "curvebs.checktime"
	CURVEBS_DEFAULT_CHECK_TIME        = 30 * time.Second
	CURVEBS_MARGIN                    = "margin"
	VIPER_CURVEBS_MARGIN              = "curvebs.margin"
	CURVEBS_DEFAULT_MARGIN            = uint64(1000)
	CURVEBS_SNAPSHOTADDR              = "snapshotaddr"
	VIPER_CURVEBS_SNAPSHOTADDR        = "curvebs.snapshotAddr"
	CURVEBS_SNAPSHOTDUMMYADDR         = "snapshotdummyaddr"
	VIPER_CURVEBS_SNAPSHOTDUMMYADDR   = "curvebs.snapshotDummyAddr"
	CURVEBS_SCAN                      = "scan"
	VIPER_CURVEBS_SCAN                = "curvebs.scan"
	CURVEBS_DEFAULT_SCAN              = true
	CURVEBS_CHUNKSERVER_ID            = "chunkserverid"
	VIPER_CHUNKSERVER_ID              = "curvebs.chunkserverid"
	CURVEBS_DEFAULT_CHUNKSERVER_ID    = "*"
	CURVEBS_CHECK_CSALIVE             = "checkalive"
	VIPER_CURVEBS_CHECK_CSALIVE       = "curvebs.checkalive"
	CURVEBS_CHECK_HEALTH              = "checkhealth"
	VIPER_CURVEBS_CHECK_HEALTH        = "curvebs.checkHealth"
	CURVEBS_CS_OFFLINE                = "offline"
	VIPER_CURVEBS_CS_OFFLINE          = "curvebs.offline"
	CURVEBS_CS_UNHEALTHY              = "unhealthy"
	VIPER_CURVEBS_CS_UNHEALTHY        = "curvebs.unhealthy"
	CURVEBS_DRYRUN                    = "dryrun"
	VIPER_CURVEBS_DRYRUN              = "curvebs.dryrun"
	CURVEBS_DEFAULT_DRYRUN            = true
	CURVEBS_AVAILFLAG                 = "availflag"
	VIPER_CURVEBS_AVAILFLAG           = "curvebs.availflag"
	CURVEBS_CHUNK_ID                  = "chunkid"
	VIPER_CURVEBS_CHUNK_ID            = "curvebs.chunkid"
	CURVEBS_CHUNKSERVER_ADDRESS       = "chunkserveraddr"
	VIPER_CURVEBS_CHUNKSERVER_ADDRESS = "curvebs.chunkserverAddr"
	CURVEBS_FIlTER                    = "filter"
	VIPER_CURVEBS_FILTER              = "curvebs.filter"
	CURVEBS_DEFAULT_FILTER            = false
	CURVEBS_ALL                       = "all"
	VIPER_CURVEBS_ALL                 = "curvebs.all"
	CURVEBS_DEFAULT_ALL               = false
	CURVEBS_NAME                      = "name"
)

var (
	BSFLAG2VIPER = map[string]string{
		// global
		RPCTIMEOUT:    VIPER_GLOBALE_RPCTIMEOUT,
		RPCRETRYTIMES: VIPER_GLOBALE_RPCRETRYTIMES,

		// bs
		CURVEBS_MDSADDR:             VIPER_CURVEBS_MDSADDR,
		CURVEBS_MDSDUMMYADDR:        VIPER_CURVEBS_MDSDUMMYADDR,
		CURVEBS_PATH:                VIPER_CURVEBS_PATH,
		CURVEBS_USER:                VIPER_CURVEBS_USER,
		CURVEBS_PASSWORD:            VIPER_CURVEBS_PASSWORD,
		CURVEBS_ETCDADDR:            VIPER_CURVEBS_ETCDADDR,
		CURVEBS_LOGIC_POOL_ID:       VIPER_CURVEBS_LOGIC_POOL_ID,
		CURVEBS_COPYSET_ID:          VIPER_CURVEBS_COPYSET_ID,
		CURVEBS_PEERS_ADDRESS:       VIPER_CURVEBS_PEERS_ADDRESS,
		CURVEBS_CLUSTERMAP:          VIPER_CURVEBS_CLUSTERMAP,
		CURVEBS_OFFSET:              VIPER_CURVEBS_OFFSET,
		CURVEBS_SIZE:                VIPER_CURVEBS_SIZE,
		CURVEBS_STRIPE_UNIT:         VIPER_CURVEBS_STRIPE_UNIT,
		CURVEBS_STRIPE_COUNT:        VIPER_CURVEBS_STRIPE_COUNT,
		CURVEBS_LIMIT:               VIPER_CURVEBS_LIMIT,
		CURVEBS_BURST:               VIPER_CURVEBS_BURST,
		CURVEBS_BURST_LENGTH:        VIPER_CURVEBS_BURST_LENGTH,
		CURVEBS_FORCE:               VIPER_CURVEBS_FORCE,
		CURVEBS_TYPE:                VIPER_CURVEBS_TYPE,
		CURVEBS_EXPIRED_TIME:        VIPER_CURVEBS_EXPIRED_TIME,
		CURVEBS_RECYCLE_PREFIX:      VIPER_RECYCLE_PREFIX,
		CURVEBS_MARGIN:              VIPER_CURVEBS_MARGIN,
		CURVEBS_OP:                  VIPER_CURVEBS_OP,
		CURVEBS_CHECK_TIME:          VIPER_CURVEBS_CHECK_TIME,
		CURVEBS_SNAPSHOTADDR:        VIPER_CURVEBS_SNAPSHOTADDR,
		CURVEBS_SNAPSHOTDUMMYADDR:   VIPER_CURVEBS_SNAPSHOTDUMMYADDR,
		CURVEBS_SCAN:                VIPER_CURVEBS_SCAN,
		CURVEBS_CHUNKSERVER_ID:      VIPER_CHUNKSERVER_ID,
		CURVEBS_CHECK_CSALIVE:       VIPER_CURVEBS_CHECK_CSALIVE,
		CURVEBS_CHECK_HEALTH:        VIPER_CURVEBS_CHECK_HEALTH,
		CURVEBS_CS_OFFLINE:          VIPER_CURVEBS_CS_OFFLINE,
		CURVEBS_CS_UNHEALTHY:        VIPER_CURVEBS_CS_UNHEALTHY,
		CURVEBS_DRYRUN:              VIPER_CURVEBS_DRYRUN,
		CURVEBS_AVAILFLAG:           VIPER_CURVEBS_AVAILFLAG,
		CURVEBS_CHUNK_ID:            VIPER_CURVEBS_CHUNK_ID,
		CURVEBS_CHUNKSERVER_ADDRESS: VIPER_CURVEBS_CHUNKSERVER_ADDRESS,
		CURVEBS_FIlTER:              VIPER_CURVEBS_FILTER,
		CURVEBS_ALL:                 VIPER_CURVEBS_ALL,
	}

	BSFLAG2DEFAULT = map[string]interface{}{
		// bs
		CURVEBS_USER:           CURVEBS_DEFAULT_USER,
		CURVEBS_PASSWORD:       CURVEBS_DEFAULT_PASSWORD,
		CURVEBS_SIZE:           CURVEBS_DEFAULT_SIZE,
		CURVEBS_STRIPE_UNIT:    CURVEBS_DEFAULT_STRIPE_UNIT,
		CURVEBS_STRIPE_COUNT:   CURVEBS_DEFAULT_STRIPE_COUNT,
		CURVEBS_BURST:          CURVEBS_DEFAULT_BURST,
		CURVEBS_BURST_LENGTH:   CURVEBS_DEFAULT_BURST_LENGTH,
		CURVEBS_PATH:           CURVEBS_DEFAULT_PATH,
		CURVEBS_FORCE:          CURVEBS_DEFAULT_FORCE,
		CURVEBS_MARGIN:         CURVEBS_DEFAULT_MARGIN,
		CURVEBS_OP:             CURVEBS_DEFAULT_OP,
		CURVEBS_CHECK_TIME:     CURVEBS_DEFAULT_CHECK_TIME,
		CURVEBS_SCAN:           CURVEBS_DEFAULT_SCAN,
		CURVEBS_CHUNKSERVER_ID: CURVEBS_DEFAULT_CHUNKSERVER_ID,
		CURVEBS_DRYRUN:         CURVEBS_DEFAULT_DRYRUN,
		CURVEBS_FIlTER:         CURVEBS_DEFAULT_FILTER,
		CURVEBS_ALL:            CURVEBS_DEFAULT_ALL,
		CURVEBS_LOGIC_POOL_ID:  CURVEBS_DEFAULT_LOGIC_POOL_ID,
		CURVEBS_COPYSET_ID:     CURVEBS_DEFAULT_COPYSET_ID,
		CURVEBS_NAME:           "",
	}
)

const (
	CURVEBS_OP_OPERATOR         = "operator"
	CURVEBS_OP_CHANGE_PEER      = "change_peer"
	CURVEBS_OP_ADD_PEER         = "add_peer"
	CURVEBS_OP_REMOVE_PEER      = "remove_peer"
	CURVEBS_OP_TRANSFER__LEADER = "transfer_leader"

	CURVEBS_IOPS_TOTAL = "iops_total"
	CURVEBS_IOPS_READ  = "iops_read"
	CURVEBS_IOPS_WRITE = "iops_write"
	CURVEBS_BPS_TOTAL  = "bps_total"
	CURVEBS_BPS_READ   = "bps_read"
	CURVEBS_BPS_WRITE  = "bps_write"
)

var (
	CURVEBS_OP_VALUE_SLICE = []string{CURVEBS_OP_OPERATOR, CURVEBS_OP_CHANGE_PEER, CURVEBS_OP_ADD_PEER, CURVEBS_OP_REMOVE_PEER, CURVEBS_OP_TRANSFER__LEADER}

	CURVEBS_THROTTLE_TYPE_SLICE = []string{CURVEBS_IOPS_TOTAL, CURVEBS_IOPS_READ, CURVEBS_IOPS_WRITE, CURVEBS_BPS_TOTAL, CURVEBS_BPS_READ, CURVEBS_BPS_WRITE}
)

var (
	BS_STRING_FLAG2AVAILABLE = map[string][]string{
		CURVEBS_OP:   CURVEBS_OP_VALUE_SLICE,
		CURVEBS_TYPE: CURVEBS_THROTTLE_TYPE_SLICE,
	}
)

func BsAvailableValueStr(flagName string) string {
	ret := ""
	if slice, ok := BS_STRING_FLAG2AVAILABLE[flagName]; ok {
		ret = strings.Join(slice, "|")
	} else if ret, ok = BSFLAG2DEFAULT[flagName].(string); !ok {
		ret = ""
	}
	return ret
}

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

func AddBsDurationOptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := BSFLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = 0
	}
	cmd.Flags().Duration(name, defaultValue.(time.Duration), usage)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsUint32OptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := BSFLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = uint32(0)
	}
	cmd.Flags().Uint32(name, defaultValue.(uint32), usage)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsUint32SliceOptionFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := BSFLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = []uint32{1, 2, 3}
	}
	cmd.Flags().UintSlice(name, defaultValue.([]uint), usage)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsBoolRequireFlag(cmd *cobra.Command, name string, usage string) {
	cmd.Flags().Bool(name, false, usage+color.Red.Sprint("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(BSFLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddBsDryrunOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_DRYRUN, "when dry run set true, no changes will be made")
}

func AddBsFilterOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_FIlTER, "filter scanning or not")
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

func AddBsDurationRequiredFlag(cmd *cobra.Command, name string, usage string) {
	cmd.Flags().Duration(name, 1*time.Second, usage+color.Red.Sprint("[required]"))
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

// snapshot clone
func AddBsSnapshotCloneFlagOption(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_SNAPSHOTADDR, "snapshot clone address, should be like 127.0.0.1:5550,127.0.0.1:5551,127.0.0.1:5552")
}

func AddBsSnapshotCloneDummyFlagOption(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_SNAPSHOTDUMMYADDR, "snapshot clone dummy address, should be like 127.0.0.1:8100,127.0.0.1:8101,127.0.0.1:8102")
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

func AddBsCheckTimeOptionFlag(cmd *cobra.Command) {
	AddBsDurationOptionFlag(cmd, CURVEBS_CHECK_TIME, "check time")
}

func AddBsChunkServerIdOptionFlag(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_CHUNKSERVER_ID, "chunkserver id")
}

// marigin
func AddBsMarginOptionFlag(cmd *cobra.Command) {
	AddUint64OptionFlag(cmd, CURVEBS_MARGIN, "the maximum gap between peers")
}

func GetBsMargin(cmd *cobra.Command) uint64 {
	return GetFlagUint64(cmd, CURVEBS_MARGIN)
}

// scan-state
func AddBsScanOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_SCAN, "enable/disable scan for logical pool")
}

// leader-schedule
func AddBsLogicalPoolIdOptionFlag(cmd *cobra.Command) {
	AddBsUint32OptionFlag(cmd, CURVEBS_LOGIC_POOL_ID, "logical pool id")
}

func AddBsAllOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_ALL, "all")
}

func AddBsCopysetIdOptionFlag(cmd *cobra.Command) {
	AddBsUint32OptionFlag(cmd, CURVEBS_COPYSET_ID, "copyset id")
}

// add flag required
// add path[required]
func AddBsPathRequiredFlag(cmd *cobra.Command) {
	AddBsStringRequiredFlag(cmd, CURVEBS_PATH, "file path")
}

func AddBsLogicalPoolIdRequiredFlag(cmd *cobra.Command) {
	AddBsUint32RequiredFlag(cmd, CURVEBS_LOGIC_POOL_ID, "logical pool id")
}

func AddBsCopysetIdRequiredFlag(cmd *cobra.Command) {
	AddBsUint32RequiredFlag(cmd, CURVEBS_COPYSET_ID, "copyset id")
}

func AddBsCopysetIdSliceRequiredFlag(cmd *cobra.Command) {
	AddBsStringSliceRequiredFlag(cmd, CURVEBS_COPYSET_ID, "copyset id")
}

func AddBsLogicalPoolIdSliceRequiredFlag(cmd *cobra.Command) {
	AddBsStringSliceRequiredFlag(cmd, CURVEBS_LOGIC_POOL_ID, "logical pool id")
}

func AddBsPeersConfFlag(cmd *cobra.Command) {
	AddBsStringSliceRequiredFlag(cmd, CURVEBS_PEERS_ADDRESS, "peers info.")
}

func AddBsForceDeleteOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_FORCE, "whether to force delete the file")
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
	AddBsStringRequiredFlag(cmd, CURVEBS_TYPE, fmt.Sprintf("throttle type, %s", BsAvailableValueStr(CURVEBS_TYPE)))
}

func AddBsLimitRequiredFlag(cmd *cobra.Command) {
	AddBsUint64RequiredFlag(cmd, CURVEBS_LIMIT, "limit")
}

func AddBsOpRequiredFlag(cmd *cobra.Command) {
	AddBsStringRequiredFlag(cmd, CURVEBS_OP, fmt.Sprintf("check operator name, %s", BsAvailableValueStr(CURVEBS_OP)))
}

func AddBsChunkServerIdFlag(cmd *cobra.Command) {
	AddBsUint32RequiredFlag(cmd, CURVEBS_CHUNKSERVER_ID, "chunkserver id")
}

func AddBsCheckCSAliveOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_CHECK_CSALIVE, "check chunkserver alive")
}

func AddBsCheckHealthOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_CHECK_HEALTH, "check chunkserver health")
}

func AddBsCSOfflineOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_CS_OFFLINE, "offline")
}

func AddBsCSUnhealthyOptionFlag(cmd *cobra.Command) {
	AddBsBoolOptionFlag(cmd, CURVEBS_CS_UNHEALTHY, "unhealthy")
}

func AddBsAvailFlagRequireFlag(cmd *cobra.Command) {
	AddBsBoolRequireFlag(cmd, CURVEBS_AVAILFLAG, "copysets available flag")
}

func AddBsChunkIdSliceRequiredFlag(cmd *cobra.Command) {
	AddBsStringSliceRequiredFlag(cmd, CURVEBS_CHUNK_ID, "chunk ids")
}

func AddBsChunkServerAddressSliceRequiredFlag(cmd *cobra.Command) {
	AddBsStringSliceRequiredFlag(cmd, CURVEBS_CHUNKSERVER_ADDRESS, "chunk server address")
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

// determine whether the flag is changed
func GetBsFlagChanged(cmd *cobra.Command, flagName string) bool {
	return cmd.Flag(flagName).Changed
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
	for i, addr := range addrslice {
		addrslice[i] = strings.TrimSpace(addr)
	}

	if addrType == CURVEBS_SNAPSHOTADDR && len(strings.TrimSpace(addrsStr)) == 0 {
		err := cmderror.ErrSnapShotAddrNotConfigured()
		err.Format(fmt.Sprint(CURVEBS_SNAPSHOTADDR, " is not configured"))
		return nil, err
	}

	for _, addr := range addrslice {
		if !IsValidAddr(addr) {
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

func GetBsSnapshotAddrSlice(cmd *cobra.Command) ([]string, *cmderror.CmdError) {
	return GetBsAddrSlice(cmd, CURVEBS_SNAPSHOTADDR)
}

func GetBsSnapshotDummyAddrSlice(cmd *cobra.Command) ([]string, *cmderror.CmdError) {
	return GetBsAddrSlice(cmd, CURVEBS_SNAPSHOTDUMMYADDR)
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

// flag for clean recycle bin
func AddBsRecyclePrefixOptionFlag(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_RECYCLE_PREFIX, "recycle prefix (default \"\")")
}

func AddBsExpireTimeOptionFlag(cmd *cobra.Command) {
	AddBsStringOptionFlag(cmd, CURVEBS_EXPIRED_TIME, "expire time (default 0s)")
}

func GetBsRecyclePrefix(cmd *cobra.Command) string {
	return GetBsFlagString(cmd, CURVEBS_RECYCLE_PREFIX)
}

func GetBsExpireTime(cmd *cobra.Command) time.Duration {
	return GetBsFlagDuration(cmd, CURVEBS_EXPIRED_TIME)
}

func GetBsChunkServerId(cmd *cobra.Command) []uint32 {
	chunkserveridStr := GetBsFlagString(cmd, CURVEBS_CHUNKSERVER_ID)
	if chunkserveridStr == "" || chunkserveridStr == "*" {
		return []uint32{}
	}
	chunkserveridStrSlice := strings.Split(chunkserveridStr, ",")
	var chunkserveridSlice []uint32
	for _, id := range chunkserveridStrSlice {
		idUint, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			parseError := cmderror.ErrParse()
			parseError.Format("chunkserver id", id)
			cobra.CheckErr(parseError.ToError())
		}
		chunkserveridSlice = append(chunkserveridSlice, uint32(idUint))
	}
	return chunkserveridSlice
}
