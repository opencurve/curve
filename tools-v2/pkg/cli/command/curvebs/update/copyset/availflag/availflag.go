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
 * Created Date: 2023-04-24
 * Author: baytan
 */

package availflag

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	copyset2 "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/check/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/unavailcopysets"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/chunk"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	copysetAvailflagExample = `$ curve bs update copyset availflag --availflag=true --dryrun=false`
)

type UpdateCopysetAvailflagRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.SetCopysetsAvailFlagRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*UpdateCopysetAvailflagRpc)(nil) // check interface

func (cRpc *UpdateCopysetAvailflagRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	cRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (cRpc *UpdateCopysetAvailflagRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return cRpc.topologyClient.SetCopysetsAvailFlag(ctx, cRpc.Request)
}

type CopysetAvailflagCommand struct {
	basecmd.FinalCurveCmd
	Rpc *UpdateCopysetAvailflagRpc
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetAvailflagCommand)(nil) // check interface

func NewCommand() *cobra.Command {
	return NewCopysetAvailflagCommand().Cmd
}

func NewCopysetAvailflagCommand() *CopysetAvailflagCommand {
	fsCmd := &CopysetAvailflagCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "availflag",
			Short:   "update copyset availflag",
			Example: copysetAvailflagExample,
		},
	}

	basecmd.NewFinalCurveCli(&fsCmd.FinalCurveCmd, fsCmd)
	return fsCmd
}

func (cCmd *CopysetAvailflagCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)

	config.AddBsDryrunOptionFlag(cCmd.Cmd)
	config.AddBsAvailFlagRequireFlag(cCmd.Cmd)
}

func (cCmd *CopysetAvailflagCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)
	availFlag := config.GetBsFlagBool(cCmd.Cmd, config.CURVEBS_AVAILFLAG)
	var copysets []*common.CopysetInfo
	if availFlag {
		var err *cmderror.CmdError
		copysets, err = unavailcopysets.GetUnAvailCopySets(cCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
	} else {
		// list chunkserver
		chunkserverInfos, errCmd := chunkserver.GetChunkServerInCluster(cCmd.Cmd)
		if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
			return errCmd.ToError()
		}

		// check chunkserver offline
		config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
		config.AddBsLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
		config.AddBsChunkIdSliceRequiredFlag(cCmd.Cmd)
		config.AddBsChunkServerAddressSliceRequiredFlag(cCmd.Cmd)
		for _, info := range chunkserverInfos {
			address := fmt.Sprintf("%s:%d", *info.HostIp, *info.Port)
			cCmd.Cmd.ParseFlags([]string{
				fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), "1",
				fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), "1",
				fmt.Sprintf("--%s", config.CURVEBS_CHUNK_ID), "1",
				fmt.Sprintf("--%s", config.CURVEBS_CHUNKSERVER_ADDRESS), address,
			})
		}
		addr2Chunk, errCmd := chunk.GetChunkInfo(cCmd.Cmd)
		if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
			return errCmd.ToError()
		}
		var offlineChunkServer []string
		for addr, info := range *addr2Chunk {
			if info == nil {
				offlineChunkServer = append(offlineChunkServer, addr)
			}
		}
		if len(offlineChunkServer) > 0 {
			cCmd.Cmd.ResetFlags()
			cCmd.AddFlags()
			config.AddBsChunkServerAddressSliceRequiredFlag(cCmd.Cmd)
			config.AddFormatFlag(cCmd.Cmd)
			cCmd.Cmd.SetArgs([]string{
				fmt.Sprintf("--%s", config.CURVEBS_CHUNKSERVER_ADDRESS), strings.Join(offlineChunkServer, ","),
			})
			addr2Copysets, errCmd := chunkserver.GetCopySetsInChunkServerByHost(cCmd.Cmd)
			if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
				return errCmd.ToError()
			}
			copysetIds := make([]string, 0)
			logicalpoolids := make([]string, 0)
			for _, infos := range *addr2Copysets {
				for _, info := range infos {
					copysetid := info.GetCopysetId()
					logicalpoolid := info.GetLogicalPoolId()
					copysetIds = append(copysetIds, strconv.FormatUint(uint64(copysetid), 10))
					logicalpoolids = append(logicalpoolids, strconv.FormatUint(uint64(logicalpoolid), 10))
				}
			}

			config.AddBsCopysetIdSliceRequiredFlag(cCmd.Cmd)
			config.AddBsLogicalPoolIdSliceRequiredFlag(cCmd.Cmd)
			cCmd.Cmd.SetArgs([]string{
				fmt.Sprintf("--%s", config.CURVEBS_LOGIC_POOL_ID), strings.Join(logicalpoolids, ","),
				fmt.Sprintf("--%s", config.CURVEBS_COPYSET_ID), strings.Join(copysetIds, ","),
			})
			key2Health, errCmd := copyset2.CheckCopysets(cCmd.Cmd)
			if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
				return errCmd.ToError()
			}

			for _, infos := range *addr2Copysets {
				for _, info := range infos {
					key := cobrautil.GetCopysetKey(uint64(info.GetLogicalPoolId()), uint64(info.GetCopysetId()))
					if health, ok := (*key2Health)[key]; !ok || health == cobrautil.HEALTH_ERROR {
						copysets = append(copysets, info)
					}
				}
			}
		}
	}

	cCmd.Rpc = &UpdateCopysetAvailflagRpc{
		Request: &topology.SetCopysetsAvailFlagRequest{
			AvailFlag: &availFlag,
			Copysets:  copysets,
		},
		Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "SetCopysetsAvailFlag"),
	}

	header := []string{cobrautil.ROW_COPYSET_ID,
		cobrautil.ROW_POOL_ID, cobrautil.ROW_AVAILFLAG, cobrautil.ROW_DRYRUN,
	}
	cCmd.SetHeader(header)
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		cCmd.Header, []string{cobrautil.ROW_AVAILFLAG, cobrautil.ROW_DRYRUN},
	))
	return nil
}

func (cCmd *CopysetAvailflagCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetAvailflagCommand) RunCommand(cmd *cobra.Command, args []string) error {
	if len(cCmd.Rpc.Request.Copysets) == 0 {
		return nil
	}
	rows := make([]map[string]string, 0)
	dryrun := config.GetFlagBool(cmd, config.CURVEBS_DRYRUN)
	if dryrun {
		for _, info := range cCmd.Rpc.Request.Copysets {
			row := make(map[string]string)
			row[cobrautil.ROW_POOL_ID] = strconv.FormatUint(uint64(info.GetLogicalPoolId()), 10)
			row[cobrautil.ROW_COPYSET_ID] = strconv.FormatUint(uint64(info.GetCopysetId()), 10)
			row[cobrautil.ROW_AVAILFLAG] = fmt.Sprintf("%v => %v", !*cCmd.Rpc.Request.AvailFlag, *cCmd.Rpc.Request.AvailFlag)
			row[cobrautil.ROW_DRYRUN] = cobrautil.ROW_VALUE_TRUE
			rows = append(rows, row)
		}
	} else {
		result, err := basecmd.GetRpcResponse(cCmd.Rpc.Info, cCmd.Rpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		response := result.(*topology.SetCopysetsAvailFlagResponse)
		if response.GetStatusCode() != int32(statuscode.TopoStatusCode_Success) {
			err := cmderror.ErrBsSetCopysetAvailFlagRpc(
				statuscode.TopoStatusCode(response.GetStatusCode()),
			)
			return err.ToError()
		}
		for _, info := range cCmd.Rpc.Request.Copysets {
			row := make(map[string]string)
			row[cobrautil.ROW_POOL_ID] = strconv.FormatUint(uint64(info.GetLogicalPoolId()), 10)
			row[cobrautil.ROW_COPYSET_ID] = strconv.FormatUint(uint64(info.GetCopysetId()), 10)
			row[cobrautil.ROW_AVAILFLAG] = fmt.Sprintf("%v => %v", !*cCmd.Rpc.Request.AvailFlag, *cCmd.Rpc.Request.AvailFlag)
			row[cobrautil.ROW_DRYRUN] = cobrautil.ROW_VALUE_FALSE
			rows = append(rows, row)
		}
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

func (cCmd *CopysetAvailflagCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}
