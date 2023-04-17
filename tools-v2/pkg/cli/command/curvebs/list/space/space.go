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
* Project: curve
* Created Date: 2023-04-17
* Author: chengyi01
 */
package space

import (
	"fmt"
	"strconv"

	"github.com/dustin/go-humanize"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	logicalpool "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/logicalPool"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/file"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
)

const (
	serverExample = `$ curve bs list space`
)

type SpaceCommand struct {
	basecmd.FinalCurveCmd
	Metric           *basecmd.Metric
	TotalChunkSize   uint64 // total chunk size
	UsedChunkSize    uint64 // used chunk size
	TotalCapacity    uint64 // total capacity
	AllocatedSize    uint64 // total allocated size
	RecycleAllocSize uint64 // recycle allocated size
	CurrentFileSize  uint64 // root dir size
}

var _ basecmd.FinalCurveCmdFunc = (*SpaceCommand)(nil) // check interface

func NewSpaceCommand() *cobra.Command {
	return NewListSpaceCommand().Cmd
}

func NewListSpaceCommand() *SpaceCommand {
	lsCmd := &SpaceCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "space",
			Short:   "show curvebs all disk type space, include total space and used space",
			Example: serverExample,
		},
	}

	basecmd.NewFinalCurveCli(&lsCmd.FinalCurveCmd, lsCmd)
	return lsCmd
}

func (sCmd *SpaceCommand) AddFlags() {
	config.AddBsMdsFlagOption(sCmd.Cmd)
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)
}

func (sCmd *SpaceCommand) Init(cmd *cobra.Command, args []string) error {
	logicalRes, totalCapacity, allocatedSize, recycleAllocSize, err := logicalpool.ListLogicalPoolInfoAndAllocSize(sCmd.Cmd)
	sCmd.TotalCapacity = totalCapacity
	sCmd.AllocatedSize = allocatedSize
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	logicalPoolInfos := make([]*topology.LogicalPoolInfo, 0)
	for _, info := range logicalRes {
		logicalPoolInfos = append(logicalPoolInfos, info.GetLogicalPoolInfos()...)
	}
	sCmd.RecycleAllocSize = recycleAllocSize

	sCmd.TotalChunkSize = 0
	for _, lgPool := range logicalPoolInfos {
		// total chunk size
		metricName := cobrautil.GetPoolTotalChunkSizeName(lgPool.GetLogicalPoolName())
		value, err := sCmd.queryMetric(metricName)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		sCmd.TotalChunkSize += value

		// used chunk size
		metricName = cobrautil.GetPoolUsedChunkSizeName(lgPool.GetLogicalPoolName())
		value, err = sCmd.queryMetric(metricName)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		sCmd.UsedChunkSize += value
	}

	config.AddBsPathRequiredFlag(sCmd.Cmd)
	sCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_PATH),
		cobrautil.ROOT_PATH,
	})

	rootSizeRes, err := file.GetFileSize(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	sCmd.CurrentFileSize = rootSizeRes.GetFileSize()

	sCmd.SetHeader([]string{cobrautil.ROW_TYPE, cobrautil.ROW_TOTAL, cobrautil.ROW_USED,
		cobrautil.ROW_LEFT, cobrautil.ROW_RECYCLABLE, cobrautil.ROW_CREATED,
	})

	return nil
}

func (sCmd *SpaceCommand) queryMetric(metricName string) (uint64, *cmderror.CmdError) {
	sCmd.Metric.SubUri = metricName
	metric, err := basecmd.QueryMetric(sCmd.Metric)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return 0, err
	} else {
		valueStr, err := basecmd.GetMetricValue(metric)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return 0, err
		}
		value, errP := strconv.ParseUint(valueStr, 10, 64)
		if errP != nil {
			pErr := cmderror.ErrParse()
			pErr.Format(metricName, pErr)
			return 0, err
		}
		return value, nil
	}
}

func (sCmd *SpaceCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SpaceCommand) RunCommand(cmd *cobra.Command, args []string) error {
	rows := make([]map[string]string, 0)
	row := make(map[string]string)
	row[cobrautil.ROW_TYPE] = cobrautil.ROW_VALUE_PHYSICAL
	row[cobrautil.ROW_TOTAL] = humanize.IBytes(sCmd.TotalChunkSize)
	row[cobrautil.ROW_USED] = humanize.IBytes(sCmd.UsedChunkSize)
	row[cobrautil.ROW_LEFT] = humanize.IBytes(sCmd.TotalChunkSize - sCmd.UsedChunkSize)
	row[cobrautil.ROW_RECYCLABLE] = cobrautil.ROW_VALUE_NO_VALUE
	row[cobrautil.ROW_CREATED] = cobrautil.ROW_VALUE_NO_VALUE
	rows = append(rows, row)

	row = make(map[string]string)
	row[cobrautil.ROW_TYPE] = cobrautil.ROW_VALUE_LOGICAL
	row[cobrautil.ROW_TOTAL] = humanize.IBytes(sCmd.TotalCapacity)
	row[cobrautil.ROW_USED] = humanize.IBytes(sCmd.AllocatedSize)
	row[cobrautil.ROW_LEFT] = humanize.IBytes(sCmd.TotalCapacity - sCmd.AllocatedSize)
	row[cobrautil.ROW_RECYCLABLE] = humanize.IBytes(sCmd.RecycleAllocSize)
	row[cobrautil.ROW_CREATED] = humanize.IBytes(sCmd.CurrentFileSize)
	rows = append(rows, row)
	list := cobrautil.ListMap2ListSortByKeys(rows, sCmd.Header, []string{})
	sCmd.TableNew.AppendBulk(list)
	sCmd.Error = cmderror.Success()
	sCmd.Result = rows
	return nil
}

func (sCmd *SpaceCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}
