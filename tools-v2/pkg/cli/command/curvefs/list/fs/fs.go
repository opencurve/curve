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
 * Created Date: 2022-06-09
 * Author: chengyi (Cyber-SiKu)
 */

package fs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/liushuochen/gotable"
	"github.com/liushuochen/gotable/table"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	mds "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/mds"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type ListFsRpc struct {
	Info      *basecmd.Rpc
	Request   *mds.ListClusterFsInfoRequest
	mdsClient mds.MdsServiceClient
}

var _ basecmd.RpcFunc = (*ListFsRpc)(nil) // check interface

type FsCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *ListFsRpc
	response *mds.ListClusterFsInfoResponse
}

var _ basecmd.FinalCurveCmdFunc = (*FsCommand)(nil) // check interface

func (fRpc *ListFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	fRpc.mdsClient = mds.NewMdsServiceClient(cc)
}

func (fRpc *ListFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return fRpc.mdsClient.ListClusterFsInfo(ctx, fRpc.Request)
}

func NewFsCommand() *cobra.Command {
	fsCmd := &FsCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "fs",
			Short: "list all fs info of the curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&fsCmd.FinalCurveCmd, fsCmd)
	return fsCmd.Cmd
}

func NewListFsCommand() *FsCommand {
	listFsCmd := &FsCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&listFsCmd.FinalCurveCmd, listFsCmd)
	return listFsCmd
}

func (fCmd *FsCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddFsMdsAddrFlag(fCmd.Cmd)
}

func (fCmd *FsCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(fCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	fCmd.Rpc = &ListFsRpc{}
	fCmd.Rpc.Request = &mds.ListClusterFsInfoRequest{}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	fCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "ListClusterFsInfo")

	table, err := gotable.Create("id", "name", "status", "capacity", "blockSize", "fsType", "sumInDir", "owner", "mountNum")
	if err != nil {
		return err
	}
	fCmd.Table = table
	return nil
}

func (fCmd *FsCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *FsCommand) RunCommand(cmd *cobra.Command, args []string) error {
	response, errCmd := basecmd.GetRpcResponse(fCmd.Rpc.Info, fCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(errCmd.Message)
	}
	fCmd.response = response.(*mds.ListClusterFsInfoResponse)
	res, err := output.MarshalProtoJson(fCmd.response)
	if err != nil {
		return err
	}
	mapRes := res.(map[string]interface{})
	fCmd.Result = mapRes
	updateTable(fCmd.Table, fCmd.response)
	fCmd.Error = cmderror.ErrSuccess()
	return nil
}

func updateTable(table *table.Table, info *mds.ListClusterFsInfoResponse) {
	fssInfo := info.GetFsInfo()
	rows := make([]map[string]string, 0)
	for _, fsInfo := range fssInfo {
		row := make(map[string]string)
		row["id"] = fmt.Sprintf("%d", fsInfo.GetFsId())
		row["name"] = fsInfo.GetFsName()
		row["status"] = fsInfo.GetStatus().String()
		row["capacity"] = fmt.Sprintf("%d", fsInfo.GetCapacity())
		row["blockSize"] = fmt.Sprintf("%d", fsInfo.GetBlockSize())
		row["fsType"] = fsInfo.GetFsType().String()
		row["sumInDir"] = fmt.Sprintf("%t", fsInfo.GetEnableSumInDir())
		row["owner"] = fsInfo.GetOwner()
		row["mountNum"] = fmt.Sprintf("%d", fsInfo.GetMountNum())
		rows = append(rows, row)
	}
	table.AddRows(rows)
}

func (fCmd *FsCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd, fCmd)
}

func GetClusterFsInfo() (*mds.ListClusterFsInfoResponse, *cmderror.CmdError) {
	listFs := NewListFsCommand()
	listFs.Cmd.SetArgs([]string{"--format", "noout"})
	listFs.Cmd.SilenceUsage = true
	err := listFs.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrGetClusterFsInfo()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return listFs.response, cmderror.ErrSuccess()
}

func GetFsIds() ([]string, *cmderror.CmdError) {
	fsInfo, err := GetClusterFsInfo()
	var ids []string
	if err.TypeCode() == cmderror.CODE_SUCCESS {
		for _, fsInfo := range fsInfo.GetFsInfo() {
			id := strconv.FormatUint(uint64(fsInfo.GetFsId()), 10)
			ids = append(ids, id)
		}
	}
	return ids, err
}
