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
* Project: curvecli
* Created Date: 2023-06-02
* Author: montaguelhz
 */

package chunkserver

import (
	"fmt"
	"strconv"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	check "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check/copyset"
	chunk "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
)

type ChunkserverCommand struct {
	basecmd.FinalCurveCmd
	status cobrautil.CHUNKSERVER_HEALTH_STATUS
	// key2State *map[uint64]cobrautil.ClUSTER_HEALTH_STATUS
	total     int
	unhealthy int
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkserverCommand)(nil) // check interface

const (
	chunkserverExample = `$ curve bs check chunkserver --chunkserverid=1,2,3
$ curve bs check chunkserver --chunkserveraddr=127.0.0.1:9700,127.0.0.1:9701,127.0.0.1:9702`
)

func NewServerCommand() *cobra.Command {
	return NewCheckChunkserverCommand().Cmd
}

func (sCmd *ChunkserverCommand) AddFlags() {
	config.AddBsMdsFlagOption(sCmd.Cmd)
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)

	config.AddBsChunkServerIdOptionFlag(sCmd.Cmd)
	config.AddBsChunkServerAddrOptionFlag(sCmd.Cmd)
	config.AddDetailOptionFlag(sCmd.Cmd)
}

func NewCheckChunkserverCommand() *ChunkserverCommand {
	cCmd := &ChunkserverCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "chunkserver",
			Short:   "check chunkserver health",
			Example: chunkserverExample,
		},
	}

	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd
}

func (sCmd *ChunkserverCommand) Init(cmd *cobra.Command, args []string) error {

	header := []string{
		cobrautil.ROW_ID,
		cobrautil.ROW_IP,
		cobrautil.ROW_PORT,
		cobrautil.ROW_STATUS,
		cobrautil.ROW_UNHEALTHY_COPYSET,
		cobrautil.ROW_COPYSET_NUM,
		cobrautil.ROW_UNHEALTHY_RATIO,
	}
	sCmd.SetHeader(header)
	sCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		sCmd.Header, []string{cobrautil.ROW_ID, cobrautil.ROW_IP, cobrautil.ROW_STATUS},
	))
	return nil
}

func (sCmd *ChunkserverCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *ChunkserverCommand) RunCommand(cmd *cobra.Command, args []string) error {
	out := make(map[string]string)
	infos, err := GetChunkServerInfo(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	for _, info := range infos {
		err := sCmd.CheckOneChunkServer(info)
		if err != nil {
			return err
		}
	}

	sCmd.Result = out
	sCmd.Error = cmderror.ErrSuccess()
	return nil
}

func (sCmd *ChunkserverCommand) CheckOneChunkServer(info *topology.ChunkServerInfo) error {
	// if chunkserver retired, don't send request
	if info.GetStatus() == topology.ChunkServerStatus_RETIRED {
		sCmd.status = cobrautil.CHUNKSERVER_HEALTHY
		return nil
	}

	isHealthy := true

	if info.GetOnlineState() == topology.OnlineState_ONLINE {
		sCmd.status = cobrautil.CHUNKSERVER_OFFLINE
		err := sCmd.CheckOfflineChunkserver(info.GetChunkServerID())
		if err != nil {
			return err
		}
	}

	sCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_CHUNKSERVER_ADDR), cobrautil.IpPort2Addr(info.GetHostIp(), info.GetPort()),
	})

	key2Status, err := GetChunkServerStatus(sCmd.Cmd)
	if err != cmderror.ErrSuccess() {
		return err.ToError()
	}

	if len(*key2Status) == 0 {
		isHealthy = false
	}
	sCmd.total = len(*key2Status)
	sCmd.unhealthy = 0
	// for key, status := range *key2Status {
	// 	state := cobrautil.COPYSET_STATE(status.GetState())
	// 	if state == cobrautil.STATE_LEADER {

	// 	} else if state == cobrautil.STATE_FOLLOWER {

	// 	} else if state == cobrautil.STATE_TRANSFERRING || state == cobrautil.STATE_CANDIDATE {
	// 		sCmd.unhealthy++
	// 	} else {
	// 		sCmd.unhealthy++
	// 	}
	// }

	if isHealthy {
		sCmd.status = cobrautil.CHUNKSERVER_HEALTHY
	} else {
		sCmd.status = cobrautil.CHUNKSERVER_UNHEALTHY
	}
	return nil
}

func (sCmd *ChunkserverCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func (sCmd *ChunkserverCommand) CheckOfflineChunkserver(chunkserverId uint32) error {
	sCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_CHUNKSERVER_ID), strconv.Itoa(int(chunkserverId)),
	})
	copysets, err := chunk.GetCopySetsInChunkServer(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	if len(copysets) == 0 {
		zeroErr := cmderror.ErrBsNoCopysets()
		zeroErr.Format(chunkserverId)
		return zeroErr.ToError()
	}

	var logicalpoolid []string
	var copysetid []string
	for i := range copysets {
		copysetid = append(copysetid, strconv.Itoa(int(copysets[i].GetCopysetId())))
		logicalpoolid = append(logicalpoolid, strconv.Itoa(int(copysets[i].GetLogicalPoolId())))
	}
	config.AddBSCopysetIdRequiredFlag(sCmd.Cmd)
	config.AddBSLogicalPoolIdRequiredFlag(sCmd.Cmd)

	sCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), strings.Join(logicalpoolid, ","),
		fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), strings.Join(copysetid, ","),
	})
	key2Health, err := check.CheckCopysets(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	sCmd.total = len(*key2Health)
	sCmd.unhealthy = len(*key2Health)
	return nil
}
