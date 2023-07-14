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
 * Created Date: 2023-06-07
 * Author: baytan0720
 */

package may_broken_vol

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	mayBrokenVolExample = `$ curve bs list may-broken-vol`
)

type ListVolumeOnCopysetsRpc struct {
	Info             *basecmd.Rpc
	Request          *nameserver2.ListVolumesOnCopysetsRequest
	nameserverClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*ListVolumeOnCopysetsRpc)(nil) // check interface

func (lRpc *ListVolumeOnCopysetsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.nameserverClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (lRpc *ListVolumeOnCopysetsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.nameserverClient.ListVolumesOnCopysets(ctx, lRpc.Request)
}

type MayBrokenVolCommand struct {
	basecmd.FinalCurveCmd
	Rpc []*ListVolumeOnCopysetsRpc
}

var _ basecmd.FinalCurveCmdFunc = (*MayBrokenVolCommand)(nil) // check interface

func NewMayBrokenVolCommand() *cobra.Command {
	return NewListMayBrokenVolCommand().Cmd
}

func NewListMayBrokenVolCommand() *MayBrokenVolCommand {
	mCmd := &MayBrokenVolCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "may-broken-vol",
			Short:   "list all volumes on majority offline copysets",
			Example: mayBrokenVolExample,
		},
	}
	basecmd.NewFinalCurveCli(&mCmd.FinalCurveCmd, mCmd)
	return mCmd
}

func (mCmd *MayBrokenVolCommand) AddFlags() {
	config.AddBsMdsFlagOption(mCmd.Cmd)
	config.AddRpcRetryTimesFlag(mCmd.Cmd)
	config.AddRpcTimeoutFlag(mCmd.Cmd)
}

func (mCmd *MayBrokenVolCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(mCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(mCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(mCmd.Cmd, config.RPCRETRYTIMES)

	offlineChunkServers, err := chunkserver.ListOfflineChunkServer(mCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	config.AddBsChunkServerIdFlag(mCmd.Cmd)
	for i, info := range offlineChunkServers {
		if i > 0 {
			mCmd.Cmd.Flag(config.CURVEBS_CHUNKSERVER_ID).Value.Set(fmt.Sprintf("%d", *info.ChunkServerID))
		} else {
			mCmd.Cmd.ParseFlags([]string{
				fmt.Sprintf("--%s", config.CURVEBS_CHUNKSERVER_ID), fmt.Sprintf("%d", *info.ChunkServerID),
			})
		}
		copysets, err := chunkserver.GetCopySetsInChunkServer(mCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		rpc := &ListVolumeOnCopysetsRpc{
			Request: &nameserver2.ListVolumesOnCopysetsRequest{
				Copysets: copysets,
			},
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListVolumesOnCopysets"),
		}
		mCmd.Rpc = append(mCmd.Rpc, rpc)
	}

	header := []string{cobrautil.ROW_FILE_NAME}
	mCmd.SetHeader(header)
	return nil
}

func (mCmd *MayBrokenVolCommand) RunCommand(cmd *cobra.Command, args []string) error {
	if len(mCmd.Rpc) == 0 {
		mCmd.Cmd.Println("no offline chunk server")
		return nil
	}
	var infos []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range mCmd.Rpc {
		infos = append(infos, rpc.Info)
		funcs = append(funcs, rpc)
	}
	results, errs := basecmd.GetRpcListResponse(infos, funcs)
	if len(errs) == len(infos) {
		mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
		return mergeErr.ToError()
	}

	rows := make([]map[string]string, 0)
	count := 0
	for _, result := range results {
		if result == nil {
			continue
		}
		resp := result.(*nameserver2.ListVolumesOnCopysetsResponse)
		for _, name := range resp.FileNames {
			row := make(map[string]string)
			row[cobrautil.ROW_FILE_NAME] = name
			rows = append(rows, row)
			count++
		}
	}

	if count == 0 {
		mCmd.Cmd.Println("no may broken volume")
	}

	mCmd.Result = rows

	return nil
}

func (mCmd *MayBrokenVolCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&mCmd.FinalCurveCmd, mCmd)
}

func (mCmd *MayBrokenVolCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&mCmd.FinalCurveCmd)
}
