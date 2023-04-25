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
	"strconv"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/olekukonko/tablewriter"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/chunk"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/copyset"
	"github.com/opencurve/curve/tools-v2/proto/proto/heartbeat"
	"github.com/spf13/cobra"
)

const (
	copysetExample = `$ curve bs check copyset --copysetid 1 --logicalpoolid 1`
)

type CopysetCommand struct {
	basecmd.FinalCurveCmd
	key2Copyset           *map[uint64]*cobrautil.BsCopysetInfoStatus
	copysetKey2LeaderInfo *map[uint64]*CopysetLeaderInfo
	health                cobrautil.ClUSTER_HEALTH_STATUS
	leaderAddr            mapset.Set[string]
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

	config.AddBSCopysetIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBSLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
	config.AddMarginOptionFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	cCmd.health = cobrautil.HEALTH_ERROR
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)

	logicpoolList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	copysetidList := config.GetBsFlagStringSlice(cCmd.Cmd, config.CURVEBS_COPYSET_ID)
	if len(logicpoolList) != len(copysetidList) {
		return fmt.Errorf("the number of logicpoolid and copysetid is not equal")
	}

	logicpoolIds := make([]uint32, len(logicpoolList))
	copysetIds := make([]uint32, len(copysetidList))
	for i := 0; i < len(logicpoolList); i++ {
		id, err := strconv.ParseUint(logicpoolList[i], 10, 32)
		if err != nil {
			return fmt.Errorf("converting %s to uint32 err: %s", logicpoolList[i], err.Error())
		}
		logicpoolIds[i] = uint32(id)

		id, err = strconv.ParseUint(copysetidList[i], 10, 32)
		if err != nil {
			return fmt.Errorf("converting %s to uint32 err: %s", copysetidList[i], err.Error())
		}
		copysetIds[i] = uint32(id)
	}

	for i := 0; i < len(logicpoolIds); i++ {
		logicpoolid := logicpoolIds[i]
		copysetid := copysetIds[i]
		csl, err := chunk.GetChunkServerListInCopySets(cCmd.Cmd, logicpoolid, copysetid)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}

		var csreqs []*copyset.CopysetStatusRequest
		for _, cs := range csl.CsInfo[0].CsLocs {
			csid := uint64(*cs.ChunkServerID)
			address := fmt.Sprintf("%s:%d", *cs.HostIp, *cs.Port)
			csreqs = append(csreqs, &copyset.CopysetStatusRequest{
				LogicPoolId: &logicpoolid,
				CopysetId:   &copysetid,
				Peer: &common.Peer{
					Id:      &csid,
					Address: &address,
				},
				QueryHash: new(bool),
			})
		}

		results := GetCopysetStatus(csreqs, timeout, retrytimes)
		peers := make([]*common.Peer, 0, len(results))
		peer2Status := make(map[string]*copyset.CopysetStatusResponse)
		var leaderPeer *common.Peer
		for _, result := range results {
			if *result.Status.Leader.Address == *result.Status.Peer.Address {
				leaderPeer = result.Status.Peer
			}
			peers = append(peers, result.Status.Peer)
			peer2Status[*result.Status.Peer.Address] = result.Status
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
			Peer2Status: peer2Status,
		}
	}

	header := []string{cobrautil.ROW_COPYSET_KEY, cobrautil.ROW_COPYSET_ID,
		cobrautil.ROW_POOL_ID, cobrautil.ROW_STATUS, cobrautil.ROW_LOG_GAP,
		cobrautil.ROW_EXPLAIN,
	}
	cCmd.SetHeader(header)
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		cCmd.Header, []string{cobrautil.ROW_COPYSET_ID, cobrautil.ROW_POOL_ID,
			cobrautil.ROW_STATUS,
		}))

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

	key2LeaderInfo := make(map[uint64]*CopysetLeaderInfo)
	cCmd.copysetKey2LeaderInfo = &key2LeaderInfo
	err := cCmd.UpdateCopysteGap(timeout)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	rows := make([]map[string]string, 0)
	var errs []*cmderror.CmdError
	copysetHealthCount := make(map[cobrautil.COPYSET_HEALTH_STATUS]uint32)
	for k, v := range *cCmd.key2Copyset {
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
					errs = append(errs, e)
				}
			}
			magrin := config.GetMarginOptionFlag(cCmd.Cmd)
			leaderInfo := (*cCmd.copysetKey2LeaderInfo)[k]
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
				if gap >= magrin {
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
		rows = append(rows, row)
	}
	if copysetHealthCount[cobrautil.COPYSET_NOTEXIST] > 0 || copysetHealthCount[cobrautil.COPYSET_ERROR] > 0 {
		cCmd.health = cobrautil.HEALTH_ERROR
	} else if copysetHealthCount[cobrautil.COPYSET_WARN] > 0 {
		cCmd.health = cobrautil.HEALTH_WARN
	} else {
		cCmd.health = cobrautil.HEALTH_OK
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
			err := GetLeaderCopysetGap(addr, &key2LeaderInfo, timeout)
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
		(*cCmd.copysetKey2LeaderInfo)[key.(uint64)] = value.(*CopysetLeaderInfo)
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
			err := GetLeaderCopysetGap(addr, &key2LeaderInfo, timeout)
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
		(*cCmd.copysetKey2LeaderInfo)[key.(uint64)] = value.(*CopysetLeaderInfo)
		return true
	})
	retErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	return retErr
}

func GetCopysetsStatus(caller *cobra.Command, copysetIds string, poolIds string) (interface{}, *tablewriter.Table, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS) {
	checkCopyset := NewCheckCopysetCommand()
	checkCopyset.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.CURVEFS_COPYSETID), copysetIds,
		fmt.Sprintf("--%s", config.CURVEFS_POOLID), poolIds,
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, checkCopyset.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEFS_MDSADDR,
		config.CURVEFS_MARGIN,
	})
	checkCopyset.Cmd.SilenceErrors = true
	err := checkCopyset.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrCheckCopyset()
		retErr.Format(err.Error())
		return checkCopyset.Result, checkCopyset.TableNew, retErr, checkCopyset.health
	}
	return checkCopyset.Result, checkCopyset.TableNew, checkCopyset.Error, checkCopyset.health
}
