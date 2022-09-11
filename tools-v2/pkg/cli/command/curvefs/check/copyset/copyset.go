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
 * Created Date: 2022-06-22
 * Author: chengyi (Cyber-SiKu)
 */

package copyset

import (
	"fmt"
	"sort"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/olekukonko/tablewriter"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/query/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	copysetExample = `$ curve fs check copyset --copysetid 1 --poolid 1`
)

type CopysetCommand struct {
	basecmd.FinalCurveCmd
	key2Copyset           *map[uint64]*cobrautil.CopysetInfoStatus
	copysetKey2Status     *map[uint64]cobrautil.COPYSET_HEALTH_STATUS
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
			Short:   "check copysets health in curvefs",
			Example: copysetExample,
		},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func GetCopysetsStatus(caller *cobra.Command, copysetIds string, poolIds string) (interface{}, *tablewriter.Table, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS) {
	checkCopyset := NewCheckCopysetCommand()
	checkCopyset.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.CURVEFS_COPYSETID), copysetIds,
		fmt.Sprintf("--%s", config.CURVEFS_POOLID), poolIds,
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	cobrautil.AlignFlagsValue(caller, checkCopyset.Cmd, []string{
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

func (cCmd *CopysetCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddFsMdsAddrFlag(cCmd.Cmd)
	config.AddCopysetidSliceRequiredFlag(cCmd.Cmd)
	config.AddPoolidSliceRequiredFlag(cCmd.Cmd)
	config.AddMarginOptionFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	cCmd.health = cobrautil.HEALTH_ERROR
	var queryCopysetErr *cmderror.CmdError
	cCmd.key2Copyset, queryCopysetErr = copyset.QueryCopysetInfoStatus(cCmd.Cmd)
	if queryCopysetErr.TypeCode() != cmderror.CODE_SUCCESS {
		return queryCopysetErr.ToError()
	}
	cCmd.Error = queryCopysetErr
	header := []string{cobrautil.ROW_COPYSET_KEY, cobrautil.ROW_COPYSET_ID,
		cobrautil.ROW_POOL_ID, cobrautil.ROW_STATUS, cobrautil.ROW_LOG_GAP,
		cobrautil.ROW_EXPLAIN,
	}
	cCmd.SetHeader(header)
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		cCmd.Header, []string{cobrautil.ROW_COPYSET_ID, cobrautil.ROW_POOL_ID,
			cobrautil.ROW_STATUS,
		}))
	copysetKey2Status := make(map[uint64]cobrautil.COPYSET_HEALTH_STATUS)
	cCmd.copysetKey2Status = &copysetKey2Status

	// update leaderAddr
	cCmd.leaderAddr = mapset.NewSet[string]()
	for _, copysetInfo := range *cCmd.key2Copyset {
		leader := copysetInfo.Info.GetLeaderPeer()
		addr, err := cobrautil.PeertoAddr(leader)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		cCmd.leaderAddr.Add(addr)
	}
	timeout := config.GetRpcTimeout(cCmd.Cmd)
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
			status, errsCheck := cobrautil.CheckCopySetHealth(v)
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
			if leaderInfo.Snapshot {
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
	cCmd.Error = &retErr
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
	return &retErr
}
