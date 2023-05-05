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
 * Created Date: 2023-04-24
 * Author: baytan0720
 */

package copyset

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/chunkserver"
	status "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/copyset"
	fscopyset "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/check/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/heartbeat"
	"github.com/spf13/cobra"
)

const (
	copysetExample = `$ curve bs check copyset --copysetid 1 --logicalpoolid 1`
)

type CopysetCommand struct {
	basecmd.FinalCurveCmd
	key2Copyset    *map[uint64]*cobrautil.BsCopysetInfoStatus
	Key2LeaderInfo *map[uint64]*fscopyset.CopysetLeaderInfo
	Key2Health     *map[uint64]cobrautil.ClUSTER_HEALTH_STATUS
	leaderAddr     mapset.Set[string]
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

func NewCopysetCommand() *cobra.Command {
	return NewCheckCopysetCommand().Cmd
}

func NewCheckCopysetCommand() *CopysetCommand {
	copysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "copyset",
			Short:   "check copysets health in curvebs",
			Example: copysetExample,
		},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func (cCmd *CopysetCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)

	config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsMarginOptionFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs := config.GetBsFlagString(cmd, config.CURVEBS_MDSADDR)
	retrytime := config.GetBsFlagInt32(cmd, config.RPCRETRYTIMES)
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	margin := config.GetBsMargin(cCmd.Cmd)

	logicalpoolidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	copysetidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_COPYSET_ID)
	if len(logicalpoolidList) != len(copysetidList) {
		return fmt.Errorf("the number of logicpoolid and copysetid is not equal")
	}
	logicalpoolIds, errParse := cobrautil.StringList2Uint32List(logicalpoolidList)
	if errParse != nil {
		return fmt.Errorf("parse logicalpoolid%v fail", logicalpoolidList)
	}
	copysetIds, errParse := cobrautil.StringList2Uint32List(copysetidList)
	if errParse != nil {
		return fmt.Errorf("parse copysetid%v fail", copysetidList)
	}

	key2Location, err := chunkserver.GetChunkServerListInCopySets(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	for i := 0; i < len(logicalpoolIds); i++ {
		logicpoolid := logicalpoolIds[i]
		copysetid := copysetIds[i]
		key := cobrautil.GetCopysetKey(uint64(logicpoolid), uint64(copysetid))

		var peerAddress []string
		for _, cs := range (*key2Location)[key] {
			address := fmt.Sprintf("%s:%d", *cs.HostIp, *cs.Port)
			peerAddress = append(peerAddress, address)
		}

		cCmd.Cmd.ResetFlags()
		config.AddBsMdsFlagOption(cCmd.Cmd)
		config.AddRpcRetryTimesFlag(cCmd.Cmd)
		config.AddRpcTimeoutFlag(cCmd.Cmd)
		config.AddBSCopysetIdRequiredFlag(cCmd.Cmd)
		config.AddBSLogicalPoolIdRequiredFlag(cCmd.Cmd)
		config.AddBSPeersConfFlag(cCmd.Cmd)
		cCmd.Cmd.ParseFlags([]string{
			fmt.Sprintf("--%s", config.CURVEBS_MDSADDR), mdsAddrs,
			fmt.Sprintf("--%s", config.RPCRETRYTIMES), fmt.Sprintf("%d", retrytime),
			fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), logicalpoolidList[i],
			fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), copysetidList[i],
			fmt.Sprintf("--%s", config.CURVEBS_PEERS_ADDRESS),
			strings.Join(peerAddress, ","),
		})
		peer2Status, err := status.GetCopysetStatus(cCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		peers := make([]*common.Peer, 0, len(*peer2Status))
		var leaderPeer *common.Peer
		for _, result := range *peer2Status {
			if result != nil {
				if *result.Leader.Address == *result.Peer.Address {
					leaderPeer = result.Peer
				}
				peers = append(peers, result.Peer)
			} else {
				peers = append(peers, nil)
			}
		}

		copysetKey := cobrautil.GetCopysetKey(uint64(logicpoolid), uint64(copysetid))
		if cCmd.key2Copyset == nil {
			key2copyset := make(map[uint64]*cobrautil.BsCopysetInfoStatus)
			cCmd.key2Copyset = &key2copyset
		}

		(*cCmd.key2Copyset)[copysetKey] = &cobrautil.BsCopysetInfoStatus{
			Info: &heartbeat.CopySetInfo{
				CopysetId:  &copysetid,
				Peers:      peers,
				LeaderPeer: leaderPeer,
			},
			Peer2Status: *peer2Status,
		}
	}
	config.AddMarginOptionFlag(cCmd.Cmd)
	config.AddFormatFlag(cCmd.Cmd)
	cCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_MARGIN),
		fmt.Sprintf("%d", margin),
	})

	header := []string{cobrautil.ROW_COPYSET_KEY, cobrautil.ROW_COPYSET_ID,
		cobrautil.ROW_POOL_ID, cobrautil.ROW_STATUS, cobrautil.ROW_LOG_GAP,
		cobrautil.ROW_EXPLAIN,
	}
	cCmd.SetHeader(header)
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		cCmd.Header, []string{cobrautil.ROW_COPYSET_ID, cobrautil.ROW_POOL_ID,
			cobrautil.ROW_STATUS,
		},
	))

	// update leaderAddr
	cCmd.leaderAddr = mapset.NewSet[string]()
	for _, cs := range *cCmd.key2Copyset {
		addr, err := cobrautil.PeerAddressToAddr(cs.Info.LeaderPeer.GetAddress())
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			err := cmderror.ErrCopysetInfo()
			err.Format(cs.Info.CopysetId)
		} else {
			cCmd.leaderAddr.Add(addr)
		}
	}

	key2LeaderInfo := make(map[uint64]*fscopyset.CopysetLeaderInfo)
	cCmd.Key2LeaderInfo = &key2LeaderInfo
	err = cCmd.UpdateCopysteGap(timeout)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	key2Health := make(map[uint64]cobrautil.ClUSTER_HEALTH_STATUS)
	cCmd.Key2Health = &key2Health
	rows := make([]map[string]string, 0)
	var errs []*cmderror.CmdError
	for k, v := range *cCmd.key2Copyset {
		copysetHealthCount := make(map[cobrautil.COPYSET_HEALTH_STATUS]uint32)
		row := make(map[string]string)
		row[cobrautil.ROW_COPYSET_KEY] = fmt.Sprintf("%d", k)
		poolid, copysetid := cobrautil.CopysetKey2PoolidCopysetid(k)
		row[cobrautil.ROW_POOL_ID] = fmt.Sprintf("%d", poolid)
		row[cobrautil.ROW_COPYSET_ID] = fmt.Sprintf("%d", copysetid)
		if v == nil {
			row[cobrautil.ROW_STATUS] = cobrautil.CopysetHealthStatus_Str[int32(cobrautil.COPYSET_NOTEXIST)]
		} else {
			status, errsCheck := cobrautil.CheckBsCopySetHealth(v)
			copysetHealthCount[status]++
			row[cobrautil.ROW_STATUS] = cobrautil.CopysetHealthStatus_Str[int32(status)]
			explain := ""
			if status != cobrautil.COPYSET_OK {
				for i, e := range errsCheck {
					if i != len(errsCheck) {
						explain += fmt.Sprintf("%s\n", e.Message)
					} else {
						explain += e.Message
					}
				}
			}
			margin := config.GetMarginOptionFlag(cCmd.Cmd)
			leaderInfo := (*cCmd.Key2LeaderInfo)[k]
			if leaderInfo == nil {
				explain = "no leader peer"
				copysetHealthCount[cobrautil.COPYSET_ERROR]++
				row[cobrautil.ROW_STATUS] = cobrautil.CopysetHealthStatus_Str[int32(cobrautil.COPYSET_ERROR)]
			} else if leaderInfo.Snapshot {
				installSnapshot := "installing snapshot"
				if len(explain) > 0 {
					explain += "\n"
				}
				explain += installSnapshot
				if row[cobrautil.ROW_STATUS] == cobrautil.COPYSET_OK_STR {
					row[cobrautil.ROW_STATUS] = cobrautil.COPYSET_WARN_STR
					copysetHealthCount[cobrautil.COPYSET_WARN]++
				}
			} else {
				gap := leaderInfo.Gap
				row[cobrautil.ROW_LOG_GAP] = fmt.Sprintf("%d", gap)
				if gap >= margin {
					behind := fmt.Sprintf("log behind %d", gap)
					if len(explain) > 0 {
						explain += "\n"
					}
					explain += behind
					if row[cobrautil.ROW_STATUS] == cobrautil.COPYSET_OK_STR {
						row[cobrautil.ROW_STATUS] = cobrautil.COPYSET_WARN_STR
						copysetHealthCount[cobrautil.COPYSET_WARN]++
					}
				}

			}
			row[cobrautil.ROW_EXPLAIN] = explain
		}
		if copysetHealthCount[cobrautil.COPYSET_NOTEXIST] > 0 || copysetHealthCount[cobrautil.COPYSET_ERROR] > 0 {
			(*cCmd.Key2Health)[k] = cobrautil.HEALTH_ERROR
		} else if copysetHealthCount[cobrautil.COPYSET_WARN] > 0 {
			(*cCmd.Key2Health)[k] = cobrautil.HEALTH_WARN
		} else {
			(*cCmd.Key2Health)[k] = cobrautil.HEALTH_OK
		}
		rows = append(rows, row)
	}
	retErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	cCmd.Error = retErr
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][cobrautil.ROW_COPYSET_KEY] < rows[j][cobrautil.ROW_COPYSET_KEY]
	})
	list := cobrautil.ListMap2ListSortByKeys(rows, cCmd.Header, []string{
		cobrautil.ROW_POOL_ID, cobrautil.ROW_STATUS, cobrautil.ROW_COPYSET_ID,
	})
	cCmd.TableNew.AppendBulk(list)
	cCmd.Result = rows
	return nil
}

func (cCmd *CopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *CopysetCommand) UpdateCopysetGap(timeout time.Duration) *cmderror.CmdError {
	var key2LeaderInfo sync.Map
	size := config.MaxChannelSize()
	errChan := make(chan *cmderror.CmdError, size)
	count := 0
	for iter := range cCmd.leaderAddr.Iter() {
		go func(addr string) {
			err := fscopyset.GetLeaderCopysetGap(addr, &key2LeaderInfo, timeout)
			errChan <- err
		}(iter)
		count++
	}
	var errs []*cmderror.CmdError
	for i := 0; i < count; i++ {
		err := <-errChan
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			errs = append(errs, err)
		}
	}
	key2LeaderInfo.Range(func(key, value interface{}) bool {
		(*cCmd.Key2LeaderInfo)[key.(uint64)] = value.(*fscopyset.CopysetLeaderInfo)
		return true
	})
	retErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	return retErr
}

func (cCmd *CopysetCommand) UpdateCopysteGap(timeout time.Duration) *cmderror.CmdError {
	var key2LeaderInfo sync.Map
	size := config.MaxChannelSize()
	errChan := make(chan *cmderror.CmdError, size)
	count := 0
	for iter := range cCmd.leaderAddr.Iter() {
		go func(addr string) {
			err := fscopyset.GetLeaderCopysetGap(addr, &key2LeaderInfo, timeout)
			errChan <- err
		}(iter)
		count++
	}
	var errs []*cmderror.CmdError
	for i := 0; i < count; i++ {
		err := <-errChan
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			errs = append(errs, err)
		}
	}
	key2LeaderInfo.Range(func(key, value interface{}) bool {
		(*cCmd.Key2LeaderInfo)[key.(uint64)] = value.(*fscopyset.CopysetLeaderInfo)
		return true
	})
	retErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	return retErr
}

func CheckCopysets(caller *cobra.Command) (*map[uint64]cobrautil.ClUSTER_HEALTH_STATUS, *cmderror.CmdError) {
	cCmd := NewCheckCopysetCommand()
	cCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	config.AlignFlagsValue(caller, cCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_COPYSET_ID, config.CURVEBS_LOGIC_POOL_ID, config.CURVEBS_MARGIN,
	})
	cCmd.Cmd.SilenceErrors = true
	cCmd.Cmd.SilenceUsage = true
	err := cCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrCheckCopyset()
		retErr.Format(err.Error())
		return cCmd.Key2Health, retErr
	}
	return cCmd.Key2Health, cmderror.Success()
}
