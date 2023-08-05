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
* Created Date: 2023-04-12
* Author: chengyi01
 */

package chunk

import (
	"fmt"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
)

type ChunkCommand struct {
	basecmd.FinalCurveCmd
	FileInfo        *nameserver2.FileInfo
	ChunkId         uint64
	LogicalpoolId   uint32
	CopysetId       uint32
	GroupId         uint64
	ChunkServerList []*common.ChunkServerLocation
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkCommand)(nil) // check interface

const (
	chunkExample = `$ curve bs query chunk --path /pagefile --offset 1024`
)

func NewQueryChunkCommand() *ChunkCommand {
	chunkCmd := &ChunkCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "chunk",
			Short:   "query the location of the chunk corresponding to the offset",
			Example: chunkExample,
		},
	}

	basecmd.NewFinalCurveCli(&chunkCmd.FinalCurveCmd, chunkCmd)
	return chunkCmd
}

func NewChunkCommand() *cobra.Command {
	return NewQueryChunkCommand().Cmd
}

func (cCmd *ChunkCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddBsPathRequiredFlag(cCmd.Cmd)
	config.AddBsOffsetRequiredFlag(cCmd.Cmd)
	config.AddBsUserOptionFlag(cCmd.Cmd)
	config.AddBsPasswordOptionFlag(cCmd.Cmd)
}

func (cCmd *ChunkCommand) Init(cmd *cobra.Command, args []string) error {
	var err *cmderror.CmdError
	cCmd.ChunkId, cCmd.LogicalpoolId, cCmd.CopysetId, err = QueryChunkCopyset(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	config.AddBsLogicalPoolIdRequiredFlag(cCmd.Cmd)
	config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
	cCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID),
		fmt.Sprintf("%d", cCmd.LogicalpoolId),
		fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID),
		fmt.Sprintf("%d", cCmd.CopysetId),
	})
	key2Location, err := chunkserver.GetChunkServerListInCopySets(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	key := cobrautil.GetCopysetKey(uint64(cCmd.LogicalpoolId), uint64(cCmd.CopysetId))
	cCmd.ChunkServerList = key2Location[key]
	header := []string{cobrautil.ROW_CHUNK, cobrautil.ROW_LOGICALPOOL,
		cobrautil.ROW_COPYSET, cobrautil.ROW_GROUP, cobrautil.ROW_LOCATION,
	}
	cCmd.SetHeader(header)
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(
		cobrautil.GetIndexSlice(header, []string{cobrautil.ROW_CHUNK,
			cobrautil.ROW_LOGICALPOOL, cobrautil.ROW_COPYSET, cobrautil.ROW_GROUP,
		}),
	)

	return nil
}

func (cCmd *ChunkCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *ChunkCommand) RunCommand(cmd *cobra.Command, args []string) error {
	cCmd.GroupId = (uint64(cCmd.LogicalpoolId) << 32) | uint64(cCmd.CopysetId)
	locations := []string{}
	for _, chunkServer := range cCmd.ChunkServerList {
		locations = append(locations, fmt.Sprintf("%s:%d", chunkServer.GetHostIp(), chunkServer.GetPort()))
	}
	location := strings.Join(locations, "\n")
	row := make(map[string]string)
	row[cobrautil.ROW_CHUNK] = fmt.Sprintf("%d", cCmd.ChunkId)
	row[cobrautil.ROW_LOGICALPOOL] = fmt.Sprintf("%d", cCmd.LogicalpoolId)
	row[cobrautil.ROW_COPYSET] = fmt.Sprintf("%d", cCmd.CopysetId)
	row[cobrautil.ROW_GROUP] = fmt.Sprintf("%d", cCmd.GroupId)
	row[cobrautil.ROW_LOCATION] = location
	list := cobrautil.Map2List(row, cCmd.Header)
	cCmd.TableNew.Append(list)
	return nil
}

func (cCmd *ChunkCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}
