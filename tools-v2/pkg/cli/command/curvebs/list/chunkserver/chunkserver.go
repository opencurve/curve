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
 * Created Date: 2023-03-27
 * Author: Sindweller
 */

package chunkserver

import (
	"context"
	"fmt"
	"strconv"

	"github.com/dustin/go-humanize"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/server"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	chunkServerExample = `$ curve bs list chunkserver`
)

type ListChunkServerRpc struct {
	Info                  *basecmd.Rpc
	Request               *topology.ListChunkServerRequest
	topologyServiceClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListChunkServerRpc)(nil) // check interface

type ChunkServerCommand struct {
	basecmd.FinalCurveCmd
	Rpc              []*ListChunkServerRpc
	Response         []*topology.ListChunkServerResponse
	ChunkServerInfos []*topology.ChunkServerInfo
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkServerCommand)(nil) // check interface

func (lRpc *ListChunkServerRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.topologyServiceClient = topology.NewTopologyServiceClient(cc)
}

func (lRpc *ListChunkServerRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.topologyServiceClient.ListChunkServer(ctx, lRpc.Request)
}

func NewChunkServerCommand() *cobra.Command {
	return NewListChunkServerCommand().Cmd
}

func NewListChunkServerCommand() *ChunkServerCommand {
	lsCmd := &ChunkServerCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "chunkserver",
			Short:   "list chunk server in curvebs",
			Example: chunkServerExample,
		},
	}

	basecmd.NewFinalCurveCli(&lsCmd.FinalCurveCmd, lsCmd)
	return lsCmd
}

// AddFlags implements basecmd.FinalCurveCmdFunc
func (pCmd *ChunkServerCommand) AddFlags() {
	config.AddBsMdsFlagOption(pCmd.Cmd)
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
	config.AddBsUserOptionFlag(pCmd.Cmd)
	config.AddBsPasswordOptionFlag(pCmd.Cmd)
	config.AddBsCheckCSAliveOptionFlag(pCmd.Cmd)
	config.AddBsCheckHealthOptionFlag(pCmd.Cmd)
	config.AddBsCSOfflineOptionFlag(pCmd.Cmd)
	config.AddBsCSUnhealthyOptionFlag(pCmd.Cmd)

	// use either ip or serverID to list Chunkserver under one server
	config.AddBsServerIdOptionFlag(pCmd.Cmd)
	config.AddBsServerIpOptionFlag(pCmd.Cmd)
	config.AddBsServerPortOptionFlag(pCmd.Cmd)
}

// Init implements basecmd.FinalCurveCmdFunc
func (pCmd *ChunkServerCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	timeout := config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)
	serverId := config.GetBsFlagUint32(pCmd.Cmd, config.CURVEBS_SERVER_ID)
	serverIp := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_SERVER_IP)
	serverPort := config.GetBsFlagUint32(pCmd.Cmd, config.CURVEBS_SERVER_PORT)

	servers, err := server.ListServer(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	// Filter servers by serverId or serverIp
	if serverId != 0 && serverIp != "" {
		return fmt.Errorf("flag --%s and -- %s cannot be used together",
			config.CURVEBS_SERVER_ID, config.CURVEBS_SERVER_IP)
	}

	for _, server := range servers {
		rpc := &ListChunkServerRpc{
			Request: &topology.ListChunkServerRequest{},
			Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListChunkServer"),
		}
		if serverIp != "" {
			rpc.Request.Ip = &serverIp
			rpc.Request.Port = &serverPort
			pCmd.Rpc = append(pCmd.Rpc, rpc)
			break
		} else if serverId != 0 {
			rpc.Request.ServerID = &serverId
			pCmd.Rpc = append(pCmd.Rpc, rpc)
			break
		} else {
			id := server.GetServerID()
			rpc.Request.ServerID = &id
		}
		pCmd.Rpc = append(pCmd.Rpc, rpc)
	}

	header := []string{
		cobrautil.ROW_ID,
		cobrautil.ROW_TYPE,
		cobrautil.ROW_IP,
		cobrautil.ROW_PORT,
		cobrautil.ROW_RW_STATUS,
		cobrautil.ROW_DISK_STATE,
		cobrautil.ROW_COPYSET_NUM,
		cobrautil.ROW_MOUNTPOINT,
		cobrautil.ROW_DISK_CAPACITY,
		cobrautil.ROW_DISK_USED,
		cobrautil.ROW_UNHEALTHY_COPYSET,
		cobrautil.ROW_EXT_ADDR,
	}
	pCmd.SetHeader(header)
	pCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		pCmd.Header, []string{cobrautil.ROW_IP, cobrautil.ROW_ID, cobrautil.ROW_RW_STATUS, cobrautil.ROW_TYPE, cobrautil.ROW_DISK_STATE, cobrautil.ROW_EXT_ADDR},
	))
	return nil
}

// Print implements basecmd.FinalCurveCmdFunc
func (pCmd *ChunkServerCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

// RunCommand implements basecmd.FinalCurveCmdFunc
func (pCmd *ChunkServerCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var rpcs []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range pCmd.Rpc {
		rpcs = append(rpcs, rpc.Info)
		funcs = append(funcs, rpc)
	}
	results, errs := basecmd.GetRpcListResponse(rpcs, funcs)
	if len(errs) == len(rpcs) {
		mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
		return mergeErr.ToError()
	}
	var errors []*cmderror.CmdError
	rows := make([]map[string]string, 0)

	// counts
	total := 0
	online := 0
	offline := 0
	unstable := 0
	pendding := 0
	retired := 0
	penddingCopyset := 0

	for _, res := range results {
		if res == nil {
			continue
		}
		infos := res.(*topology.ListChunkServerResponse).GetChunkServerInfos()
		for _, info := range infos {
			csID := info.GetChunkServerID()
			// get copyset info
			copysetCmd := NewGetCopySetsInCopySetCommand()
			config.AlignFlagsValue(pCmd.Cmd, copysetCmd.Cmd, []string{
				config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
			})
			copysetCmd.Cmd.Flags().Set(config.CURVEBS_CHUNKSERVER_ID, strconv.FormatUint(uint64(csID), 10))
			copysets, err := GetCopySetsInChunkServer(copysetCmd.Cmd)
			if err.TypeCode() != cmderror.CODE_SUCCESS {
				errors = append(errs, err)
				continue
			}
			unhealthyRatio := 0.0

			if info.GetOnlineState() != topology.OnlineState_ONLINE {
				if info.GetOnlineState() == topology.OnlineState_OFFLINE {
					offline++
				}
				if info.GetOnlineState() == topology.OnlineState_UNSTABLE {
					unstable++
				}
				unhealthyRatio = 1.0
			} else {
				if config.GetFlagBool(pCmd.Cmd, config.CURVEBS_CS_OFFLINE) {
					continue
				}
				online++
			}

			if info.GetStatus() == topology.ChunkServerStatus_PENDDING {
				pendding++
				penddingCopyset += len(copysets)
			}
			if info.GetStatus() == topology.ChunkServerStatus_RETIRED {
				retired++
			}
			total++
			unhealthyRatio *= 100.00
			// generate row message
			row := make(map[string]string)
			row[cobrautil.ROW_ID] = fmt.Sprintf("%d", info.GetChunkServerID())
			row[cobrautil.ROW_TYPE] = info.GetDiskType()
			row[cobrautil.ROW_IP] = info.GetHostIp()
			row[cobrautil.ROW_PORT] = fmt.Sprintf("%d", info.GetPort())
			row[cobrautil.ROW_RW_STATUS] = fmt.Sprintf("%s", info.GetStatus())
			row[cobrautil.ROW_DISK_STATE] = fmt.Sprintf("%s", info.GetDiskStatus())
			row[cobrautil.ROW_COPYSET_NUM] = fmt.Sprintf("%d", len(copysets))
			row[cobrautil.ROW_MOUNTPOINT] = info.GetMountPoint()
			row[cobrautil.ROW_DISK_CAPACITY] = humanize.IBytes(info.GetDiskCapacity())
			row[cobrautil.ROW_DISK_USED] = humanize.IBytes(info.GetDiskCapacity())
			row[cobrautil.ROW_UNHEALTHY_COPYSET] = fmt.Sprintf("%.0f %%", unhealthyRatio)
			row[cobrautil.ROW_EXT_ADDR] = info.GetExternalIp()
			rows = append(rows, row)
			pCmd.ChunkServerInfos = append(pCmd.ChunkServerInfos, info)
		}
	}

	list := cobrautil.ListMap2ListSortByKeys(rows, pCmd.Header, []string{cobrautil.ROW_IP, cobrautil.ROW_ID, cobrautil.ROW_RW_STATUS, cobrautil.ROW_TYPE, cobrautil.ROW_DISK_STATE, cobrautil.ROW_EXT_ADDR})
	pCmd.TableNew.AppendBulk(list)
	if len(errors) != 0 {
		mergeErr := cmderror.MergeCmdError(errors)
		pCmd.Result, pCmd.Error = list, mergeErr
		return mergeErr.ToError()
	}
	pCmd.Result, pCmd.Error = list, cmderror.Success()
	return nil
}

// ResultPlainOutput implements basecmd.FinalCurveCmdFunc
func (pCmd *ChunkServerCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}

func ListChunkServerInfos(caller *cobra.Command) ([]*topology.ChunkServerInfo, *cmderror.CmdError) {
	lCmd := NewListChunkServerCommand()
	config.AlignFlagsValue(caller, lCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_SERVER_IP, config.CURVEBS_SERVER_PORT, config.CURVEBS_SERVER_ID,
	})
	lCmd.Cmd.SilenceErrors = true
	lCmd.Cmd.SilenceUsage = true
	lCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := lCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsListChunkServer()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return lCmd.ChunkServerInfos, cmderror.ErrSuccess()
}
