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
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	mds "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/mds"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

const (
	fsExample = `$ curve fs list fs`
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
			Use:     "fs",
			Short:   "list all fs info in the curvefs",
			Example: fsExample,
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

	table, err := gotable.Create(cobrautil.ROW_ID, cobrautil.ROW_NAME, cobrautil.ROW_STATUS, cobrautil.ROW_CAPACITY, cobrautil.ROW_BLOCKSIZE, cobrautil.ROW_FS_TYPE, cobrautil.ROW_SUM_IN_DIR, cobrautil.ROW_OWNER, cobrautil.ROW_MOUNT_NUM)
	header := []string{cobrautil.ROW_ID, cobrautil.ROW_NAME, cobrautil.ROW_STATUS, cobrautil.ROW_CAPACITY, cobrautil.ROW_BLOCKSIZE, cobrautil.ROW_FS_TYPE, cobrautil.ROW_SUM_IN_DIR, cobrautil.ROW_OWNER, cobrautil.ROW_MOUNT_NUM}
	fCmd.SetHeader(header)
	index_owner := slices.Index(header, cobrautil.ROW_OWNER)
	index_type := slices.Index(header, cobrautil.ROW_FS_TYPE)
	fCmd.TableNew.SetAutoMergeCellsByColumnIndex([]int{index_owner, index_type})
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
	fCmd.updateTable()
	fCmd.Error = cmderror.ErrSuccess()
	return nil
}

func (fCmd *FsCommand)updateTable() {
	fssInfo := fCmd.response.GetFsInfo()
	rows := make([]map[string]string, 0)
	for _, fsInfo := range fssInfo {
		row := make(map[string]string)
		row[cobrautil.ROW_ID] = fmt.Sprintf("%d", fsInfo.GetFsId())
		row[cobrautil.ROW_NAME] = fsInfo.GetFsName()
		row[cobrautil.ROW_STATUS] = fsInfo.GetStatus().String()
		row[cobrautil.ROW_CAPACITY] = fmt.Sprintf("%d", fsInfo.GetCapacity())
		row[cobrautil.ROW_BLOCKSIZE] = fmt.Sprintf("%d", fsInfo.GetBlockSize())
		row[cobrautil.ROW_FS_TYPE] = fsInfo.GetFsType().String()
		row[cobrautil.ROW_SUM_IN_DIR] = fmt.Sprintf("%t", fsInfo.GetEnableSumInDir())
		row[cobrautil.ROW_OWNER] = fsInfo.GetOwner()
		row[cobrautil.ROW_MOUNT_NUM] = fmt.Sprintf("%d", fsInfo.GetMountNum())
		rows = append(rows, row)
	}
	fCmd.Table.AddRows(rows)
	list := cobrautil.ListMap2ListSortByKeys(rows, fCmd.Header, []string{cobrautil.ROW_OWNER, cobrautil.ROW_FS_TYPE, cobrautil.ROW_ID})
	fCmd.TableNew.AppendBulk(list)
}

func (fCmd *FsCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd, fCmd)
}

func GetClusterFsInfo(caller *cobra.Command) (*mds.ListClusterFsInfoResponse, *cmderror.CmdError) {
	listFs := NewListFsCommand()
	listFs.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	listFs.Cmd.SilenceErrors = true
	err := listFs.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrGetClusterFsInfo()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return listFs.response, cmderror.ErrSuccess()
}

func GetFsIds(caller *cobra.Command) ([]string, *cmderror.CmdError) {
	fsInfo, err := GetClusterFsInfo(caller)
	var ids []string
	if err.TypeCode() == cmderror.CODE_SUCCESS {
		for _, fsInfo := range fsInfo.GetFsInfo() {
			id := strconv.FormatUint(uint64(fsInfo.GetFsId()), 10)
			ids = append(ids, id)
		}
	}
	return ids, err
}
