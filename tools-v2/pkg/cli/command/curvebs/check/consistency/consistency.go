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
 * Project: tools-v2
 * Created Date: 2023-07-13
 * Author: victorseptember
 */

package consistency

import (
	"fmt"
	"strconv"
	"strings"

	mapset "github.com/deckarep/golang-set/v2"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/chunk"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/seginfo"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	consistencyExample = `$ curve bs check consistency --path file_path`
)

type ConsistencyCommand struct {
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*ConsistencyCommand)(nil)

func NewConsistencyCommand() *cobra.Command {
	return NewCheckConsistencyCommand().Cmd
}

func NewCheckConsistencyCommand() *ConsistencyCommand {
	consistencyCmd := &ConsistencyCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "consistency",
			Short:   "check the file consistency",
			Example: consistencyExample,
		}}
	basecmd.NewFinalCurveCli(&consistencyCmd.FinalCurveCmd, consistencyCmd)
	return consistencyCmd
}

func (cCmd *ConsistencyCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddBsPathRequiredFlag(cCmd.Cmd)
	config.AddHttpTimeoutFlag(cCmd.Cmd)
	config.AddBsCheckHashOptionFlag(cCmd.Cmd)
}

func (cCmd *ConsistencyCommand) Init(cmd *cobra.Command, args []string) error {
	segmentInfoResponse, err := seginfo.GetSegmentInfo(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	logicalpoolIds := make([]string, 0)
	copysetIds := make([]string, 0)
	chunkIds := make([]string, 0)

	for _, segment := range segmentInfoResponse {
		for _, chunk := range segment.GetChunks() {
			logicalpoolId := segment.GetLogicalPoolID()
			copysetId := chunk.GetCopysetID()
			chunkId := chunk.GetChunkID()
			logicalpoolIds = append(logicalpoolIds, fmt.Sprintf("%d", logicalpoolId))
			copysetIds = append(copysetIds, fmt.Sprintf("%d", copysetId))
			chunkIds = append(chunkIds, fmt.Sprintf("%d", chunkId))
		}
	}

	if len(logicalpoolIds) == 0 || len(copysetIds) == 0 {
		return fmt.Errorf("the number of logicalpoolid or copysetid is empty")
	}
	config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
	config.AddBsLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
	cCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), strings.Join(logicalpoolIds, ","),
		fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), strings.Join(copysetIds, ","),
	})

	key2Location, err := chunkserver.GetChunkServerListInCopySets(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	logicalpoolidList, errParse := cobrautil.StringList2Uint32List(logicalpoolIds)
	if errParse != nil {
		return fmt.Errorf("parse logicalpoolid%v fail", logicalpoolIds)
	}
	copysetidList, errParse := cobrautil.StringList2Uint32List(copysetIds)
	if errParse != nil {
		return fmt.Errorf("parse copysetid%v fail", copysetIds)
	}

	// check copyset raft status
	chunkServerLocationSet := mapset.NewSet[string]()
	key2Address := make(map[uint64][]string)
	copysetKeysSet := mapset.NewSet[uint64]()
	addr2Id := make(map[string]uint32)
	for i := 0; i < len(logicalpoolidList); i++ {
		logicpoolid := logicalpoolidList[i]
		copysetid := copysetidList[i]
		key := cobrautil.GetCopysetKey(uint64(logicpoolid), uint64(copysetid))
		if copysetKeysSet.Contains(key) {
			continue
		}
		copysetKeysSet.Add(key)
		for _, cs := range key2Location[key] {
			address := fmt.Sprintf("%s:%d", *cs.HostIp, *cs.Port)
			key2Address[key] = append(key2Address[key], address)
			chunkServerLocationSet.Add(address)
			addr2Id[address] = *cs.ChunkServerID
		}
	}

	cs := copyset.NewCopyset()
	filename := config.GetBsFlagString(cmd, config.CURVEBS_PATH)
	status, explain, err2 := copyset.CheckCopysetsInChunkServers(
		chunkServerLocationSet.ToSlice(), addr2Id, cs, copysetKeysSet, cmd)
	if err2 != nil {
		return fmt.Errorf("CheckCopysetsInChunkServers fail")
	}

	// chech chunk hash
	checkHash := config.GetBsFlagBool(cCmd.Cmd, config.CURVEBS_CHECK_HASH)
	if checkHash {
		config.AddBsPeersAddressFlag(cCmd.Cmd)
		config.AddBsChunkIdRequiredFlag(cCmd.Cmd)
		config.AddBsChunkSizeRequiredFlag(cCmd.Cmd)
		chunksize := segmentInfoResponse[0].GetChunkSize()
		for i := 0; i < len(logicalpoolIds); i++ {
			key := cobrautil.GetCopysetKey(uint64(logicalpoolidList[i]), uint64(copysetidList[i]))
			config.ResetStringSliceFlag(cCmd.Cmd.Flag(config.CURVEBS_LOGIC_POOL_ID), logicalpoolIds[i])
			config.ResetStringSliceFlag(cCmd.Cmd.Flag(config.CURVEBS_COPYSET_ID), copysetIds[i])
			cCmd.Cmd.ParseFlags([]string{
				fmt.Sprintf("--%s", config.CURVEBS_CHUNK_ID), chunkIds[i],
				fmt.Sprintf("--%s", config.CURVEBS_CHUNK_SIZE), fmt.Sprintf("%d", chunksize),
				fmt.Sprintf("--%s", config.CURVEBS_PEERS_ADDRESS), strings.Join(key2Address[key], ","),
			})
			result, checkHashExplain, err := chunk.GetChunkHash(cCmd.Cmd)
			if err != nil {
				return fmt.Errorf("GetChunkHash fail")
			}
			if !result {
				status = cobrautil.CopysetHealthStatus_Str[int32(cobrautil.HEALTH_ERROR)]
				explain += fmt.Sprintf("copyset %s chunkID %s %s.\n",
					strconv.FormatUint(key, 10), chunkIds[i], checkHashExplain)
			}
		}
	}
	cCmd.TableNew.Append([]string{filename, status, explain})
	header := []string{cobrautil.ROW_NAME, cobrautil.ROW_STATUS, cobrautil.ROW_EXPLAIN}
	cCmd.SetHeader(header)

	return nil
}

func (cCmd *ConsistencyCommand) RunCommand(cmd *cobra.Command, args []string) error {
	return nil
}

func (cCmd *ConsistencyCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *ConsistencyCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}
