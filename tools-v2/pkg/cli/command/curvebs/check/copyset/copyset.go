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

	copysetutil "github.com/opencurve/curve/tools-v2/internal/copyset"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/chunkserver"
	status "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/copyset"
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
	CopysetKeySlice []uint64
	key2copysetInfo map[uint64]*cobrautil.BsCopysetInfoStatus
	key2leaderAddr  map[uint64]string
	key2leaderInfo  map[uint64]copysetutil.CopysetLeaderInfo
	key2checkResult map[uint64]cobrautil.COPYSET_CHECK_RESULT
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
	cCmd.key2checkResult = make(map[uint64]cobrautil.COPYSET_CHECK_RESULT)
	cCmd.key2copysetInfo = make(map[uint64]*cobrautil.BsCopysetInfoStatus)
	cCmd.key2leaderAddr = make(map[uint64]string)
	cCmd.key2leaderInfo = make(map[uint64]copysetutil.CopysetLeaderInfo)
	for i := 0; i < len(logicalpoolIds); i++ {
		key := cobrautil.GetCopysetKey(uint64(logicalpoolIds[i]), uint64(copysetIds[i]))
		if _, ok := cCmd.key2checkResult[key]; ok {
			return fmt.Errorf("duplicate (copysetid, logicalpoolid) pair: %d, %d", copysetIds[i], logicalpoolIds[i])
		}
		cCmd.CopysetKeySlice = append(cCmd.CopysetKeySlice, key)
		cCmd.key2checkResult[key] = cobrautil.HEALTHY
	}
	// get copysets info
	err := cCmd.GetCopysetsInfo()
	if err.Code != cmderror.CODE_SUCCESS {
		cCmd.RemoveUnhealthyCopyset()
	}
	// update leaderAddr
	for _, key := range cCmd.CopysetKeySlice {
		leader := cCmd.key2copysetInfo[key].Info.GetLeaderPeer()
		adder, err := cobrautil.PeerAddressToAddr(leader.GetAddress())
		if err.Code == cmderror.CODE_SUCCESS {
			cCmd.key2leaderAddr[key] = adder
		}
	}
	// get leader info
	timeout := config.GetRpcTimeout(cCmd.Cmd)
	err = cCmd.UpdateCopysetGap(timeout)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		cCmd.RemoveUnhealthyCopyset()
	}

	header := []string{cobrautil.ROW_COPYSET_KEY, cobrautil.ROW_POOL_ID,
		cobrautil.ROW_COPYSET_ID, cobrautil.ROW_RESULT}
	cCmd.SetHeader(header)
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		cCmd.Header, []string{cobrautil.ROW_COPYSET_ID, cobrautil.ROW_POOL_ID,
			cobrautil.ROW_RESULT,
		},
	))
	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	rows := make([]map[string]string, 0)
	for _, key := range cCmd.CopysetKeySlice {
		row := make(map[string]string)
		poolid, copysetid := cobrautil.CopysetKey2PoolidCopysetid(key)
		row[cobrautil.ROW_COPYSET_KEY] = fmt.Sprintf("%d", key)
		row[cobrautil.ROW_POOL_ID] = fmt.Sprintf("%d", poolid)
		row[cobrautil.ROW_COPYSET_ID] = fmt.Sprintf("%d", copysetid)
		info := cCmd.key2copysetInfo[key]
		if len(info.Peer2Status) < 3 {
			row[cobrautil.ROW_RESULT] = cobrautil.CopysetResultStr[cobrautil.PEERS_NO_SUFFICIENT]
			cCmd.key2checkResult[key] = cobrautil.PEERS_NO_SUFFICIENT
			rows = append(rows, row)
			continue
		}
		majority := len(info.Peer2Status)/2 + 1
		offlinePeers := 0
		for _, status := range info.Peer2Status {
			if status == nil {
				offlinePeers++
			}
			if status.Leader != nil && status.Peer != nil && status.Leader.Address == status.Peer.Address {
				margin := config.GetMarginOptionFlag(cCmd.Cmd)
				leaderInfo := cCmd.key2leaderInfo[key]
				result := cobrautil.HEALTHY
				if leaderInfo.Snapshot {
					result = cobrautil.INSTALLING_SNAPSHOT
				} else if leaderInfo.Gap >= margin {
					result = cobrautil.LOG_INDEX_GAP_TOO_BIG
				}
				if result != cobrautil.HEALTHY {
					row[cobrautil.ROW_RESULT] = cobrautil.CopysetResultStr[result]
					cCmd.key2checkResult[key] = result
					rows = append(rows, row)
					continue
				}
			}
		}
		if offlinePeers >= majority {
			row[cobrautil.ROW_RESULT] = cobrautil.CopysetResultStr[cobrautil.MAJORITY_NOT_ONLINE]
			cCmd.key2checkResult[key] = cobrautil.MAJORITY_NOT_ONLINE
		} else if offlinePeers > 0 {
			row[cobrautil.ROW_RESULT] = cobrautil.CopysetResultStr[cobrautil.MINORITY_NOT_ONLINE]
			cCmd.key2checkResult[key] = cobrautil.MINORITY_NOT_ONLINE
		} else {
			row[cobrautil.ROW_RESULT] = cobrautil.CopysetResultStr[cobrautil.HEALTHY]
			cCmd.key2checkResult[key] = cobrautil.HEALTHY
		}
		rows = append(rows, row)
	}
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

func (cCmd *CopysetCommand) RemoveUnhealthyCopyset() {
	for index, key := range cCmd.CopysetKeySlice {
		if cCmd.key2checkResult[key] != cobrautil.HEALTHY {
			cCmd.CopysetKeySlice = append(cCmd.CopysetKeySlice[:index], cCmd.CopysetKeySlice[index+1:]...)
		}
	}
}

func (cCmd *CopysetCommand) GetCopysetsInfo() (retErr *cmderror.CmdError) {
	retErr = cmderror.ErrSuccess()
	key2Location, err := chunkserver.GetChunkServerListInCopySets(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		for _, key := range cCmd.CopysetKeySlice {
			cCmd.key2checkResult[key] = cobrautil.OTHER_ERROR
		}
		return err
	}
	for _, key := range cCmd.CopysetKeySlice {
		location := (*key2Location)[key]
		var peerAddress []string
		for _, cs := range location {
			address := fmt.Sprintf("%s:%d", *cs.HostIp, *cs.Port)
			peerAddress = append(peerAddress, address)
		}

		csCmd := status.NewGetCopysetStatusCommand()
		config.AlignFlagsValue(cCmd.Cmd, csCmd.Cmd, []string{
			config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
			config.CURVEBS_COPYSET_ID, config.CURVEBS_LOGIC_POOL_ID,
		})
		csCmd.Cmd.Flags().Set(config.CURVEBS_PEERS_ADDRESS, strings.Join(peerAddress, ","))
		peer2Status, err := status.GetCopysetStatus(csCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			cCmd.key2checkResult[key] = cobrautil.OTHER_ERROR
			retErr = err
			continue
		}
		peers := make([]*common.Peer, 0, len(peer2Status))
		var leaderPeer *common.Peer
		for _, result := range peer2Status {
			if result != nil {
				if result.Leader != nil && result.Peer != nil && *result.Leader.Address == *result.Peer.Address {
					leaderPeer = result.Peer
				}
				peers = append(peers, result.Peer)
			} else {
				peers = append(peers, nil)
			}
		}
		_, copysetid := cobrautil.CopysetKey2PoolidCopysetid(key)
		cCmd.key2copysetInfo[key] = &cobrautil.BsCopysetInfoStatus{
			Info: &heartbeat.CopySetInfo{
				CopysetId:  &copysetid,
				Peers:      peers,
				LeaderPeer: leaderPeer,
			},
			Peer2Status: peer2Status,
		}
	}
	return
}

func (cCmd *CopysetCommand) UpdateCopysetGap(timeout time.Duration) *cmderror.CmdError {
	var key2LeaderInfo sync.Map
	size := config.MaxChannelSize()
	errChan := make(chan *cmderror.CmdError, size)
	count := 0
	for _, leaderAddr := range cCmd.key2leaderAddr {
		go func(addr string) {
			err := copysetutil.GetLeaderCopysetGap(leaderAddr, &key2LeaderInfo, timeout)
			errChan <- err
		}(leaderAddr)
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
		cCmd.key2leaderInfo[key.(uint64)] = value.(copysetutil.CopysetLeaderInfo)
		return true
	})
	retErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	return retErr
}

func CheckCopysets(caller *cobra.Command) (map[uint64]cobrautil.COPYSET_CHECK_RESULT, *cmderror.CmdError) {
	cCmd := NewCheckCopysetCommand()
	cCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	config.AlignFlagsValue(caller, cCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_COPYSET_ID, config.CURVEBS_LOGIC_POOL_ID, config.CURVEBS_MARGIN,
	})
	cCmd.Cmd.SilenceErrors = true
	cCmd.Cmd.Execute()
	return cCmd.key2checkResult, cmderror.ErrSuccess()
}
