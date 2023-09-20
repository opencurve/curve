/*
*  Copyright (c) 2023 NetEase Inc.
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
* Created Date: 2023-05-18
* Author: chengyi01
 */

package copyset

import (
	"fmt"
	"strconv"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

type CopysetCommand struct {
	basecmd.FinalCurveCmd
	healthy cobrautil.ClUSTER_HEALTH_STATUS
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

func NewCopysetCommand() *cobra.Command {
	return NewStatusCopysetCommand().Cmd
}

func (cCmd *CopysetCommand) AddFlags() {
	config.AddHttpTimeoutFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddBsMarginOptionFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	header := []string{cobrautil.ROW_EXPLAIN, cobrautil.ROW_COPYSET_KEY, cobrautil.ROW_HEALTHY_RATIO, cobrautil.ROW_STATUS}
	cCmd.SetHeader(header)
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		cCmd.Header, []string{
			cobrautil.ROW_EXPLAIN,
			cobrautil.ROW_STATUS,
		},
	))
	cCmd.TableNew.SetColumnAlignment([]int{1, 3, 1, 1})
	cs := NewCopyset()
	_, err := cs.checkCopysetsInCluster(cCmd.Cmd)
	if err != nil {
		return err
	}
	copysetsFromCS := cs.getCopysetTotalNum()
	unhealthyCopysetsFromCS := cs.getCopysetUnhealthyNum()
	healthyCopysetsFromCS := copysetsFromCS - unhealthyCopysetsFromCS

	totalGroupIds := cs.copyset[COPYSET_TOTAL]
	delete(cs.copyset, COPYSET_TOTAL)
	rows := [][]string{}
	var ishealthy string
	for state, groupIds := range cs.copyset {
		switch state {
		case COPYSET_CHECK_HEALTHY:
			ishealthy = COPYSET_CHECK_HEALTHY
		default:
			ishealthy = cobrautil.ROW_UNHEALTHY
		}
		row := []string{}
		row = append(row, state)
		var str, allGroups string
		for i, group := range groupIds.ToSlice() {
			if i%5 == 0 && i >= 5 {
				str += "\n"
				allGroups += str
				str = ""
			}
			str += group + ","
		}
		allGroups += str
		row = append(row, allGroups)
		row = append(row, strconv.Itoa(int(cs.copyset[state].Cardinality()))+" / "+strconv.Itoa(int(copysetsFromCS)))
		row = append(row, ishealthy)
		rows = append(rows, row)
	}
	row := []string{
		"healthy",
		"-",
		strconv.Itoa(int(healthyCopysetsFromCS)) +
			" / " +
			strconv.Itoa(int(copysetsFromCS)),
		"healthy",
	}
	cCmd.TableNew.Append(row)
	cCmd.TableNew.AppendBulk(rows)

	// final result for 'bs status cluster'
	// has some scenarios may cause to 'error' state
	// 1. no leader peer
	// 2. most of peers offline
	// 3. not consistent with mds
	if _, ok := cs.copysetStat[int32(cobrautil.COPYSET_ERROR)]; ok &&
		cs.copysetStat[int32(cobrautil.COPYSET_ERROR)].Cardinality() > 0 {
		cCmd.healthy = cobrautil.HEALTH_ERROR
	} else if _, ok := cs.copysetStat[int32(cobrautil.COPYSET_WARN)]; ok &&
		cs.copysetStat[int32(cobrautil.COPYSET_WARN)].Cardinality() > 0 {
		cCmd.healthy = cobrautil.HEALTH_WARN
	} else {
		cCmd.healthy = cobrautil.HEALTH_OK
	}

	// check consistence with mds
	cs.copyset[COPYSET_TOTAL] = totalGroupIds
	consistent, mdsCopysets, _ := cs.checkCopysetsWithMds(cCmd.Cmd)
	if !consistent {
		cCmd.healthy = cobrautil.HEALTH_ERROR
		fmt.Printf("\nCopyset numbers in chunkservers not consistent with mds,"+
			"please check! copysets on chunkserver: %d; copysets in mds: %d\n",
			cs.copyset[COPYSET_TOTAL].Cardinality(), mdsCopysets)
	}

	ret := make(map[string]string)
	ret[cobrautil.ROW_TOTAL] = strconv.Itoa(int(copysetsFromCS))
	ret[cobrautil.ROW_HEALTHY] = strconv.Itoa(int(healthyCopysetsFromCS))
	ret[cobrautil.ROW_UNHEALTHY] = strconv.Itoa(int(unhealthyCopysetsFromCS))
	ret[cobrautil.ROW_UNHEALTHY_RATIO] = fmt.Sprintf("%.2f%%", float64(unhealthyCopysetsFromCS)/float64(copysetsFromCS)*100)
	res := []map[string]string{}
	res = append(res, ret)
	cCmd.Result = res

	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	return nil
}

func (cCmd *CopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func NewStatusCopysetCommand() *CopysetCommand {
	copysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "copyset",
			Short: "get status of copysets in cluster",
		},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd
}

func GetCopysetsStatus(caller *cobra.Command) (*interface{}, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS) {
	cCmd := NewStatusCopysetCommand()
	cCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, cCmd.Cmd, []string{config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR, config.CURVEBS_MARGIN})
	cCmd.Cmd.SilenceErrors = true
	err := cCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetCopysetStatus()
		retErr.Format(err.Error())
		return nil, retErr, cobrautil.HEALTH_ERROR
	}
	return &cCmd.Result, cmderror.Success(), cCmd.healthy
}
