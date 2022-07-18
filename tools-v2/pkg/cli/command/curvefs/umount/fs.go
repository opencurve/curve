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

package umount

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	mds "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/mds"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type UmountFsRpc struct {
	Info      *basecmd.Rpc
	Request   *mds.UmountFsRequest
	mdsClient mds.MdsServiceClient
}

var _ basecmd.RpcFunc = (*UmountFsRpc)(nil) // check interface

type FsCommand struct {
	basecmd.FinalCurveCmd
	Rpc        UmountFsRpc
	fsName     string
	mountpoint string
}

var _ basecmd.FinalCurveCmdFunc = (*FsCommand)(nil) // check interface

func (ufRp *UmountFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	ufRp.mdsClient = mds.NewMdsServiceClient(cc)
}

func (ufRp *UmountFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return ufRp.mdsClient.UmountFs(ctx, ufRp.Request)
}

const (
	fsExample = `$ curvefs fs umount fs -fsname fsname -mountpoint hostname:port:path`
)

func NewFsCommand() *cobra.Command {
	fsCmd := &FsCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "fs",
			Short:   "umount fs from the curvefs cluster",
			Example: fsExample,
		},
	}
	basecmd.NewFinalCurveCli(&fsCmd.FinalCurveCmd, fsCmd)
	return fsCmd.Cmd
}

func (fCmd *FsCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddFsMdsAddrFlag(fCmd.Cmd)
	config.AddFsNameRequiredFlag(fCmd.Cmd)
	config.AddMountpointFlag(fCmd.Cmd)
}

func (fCmd *FsCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(fCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	fCmd.Rpc.Request = &mds.UmountFsRequest{}

	fCmd.fsName = config.GetFlagString(fCmd.Cmd, config.CURVEFS_FSNAME)
	fCmd.Rpc.Request.FsName = &fCmd.fsName
	fCmd.mountpoint = config.GetFlagString(fCmd.Cmd, config.CURVEFS_MOUNTPOINT)
	mountpointSlice := strings.Split(fCmd.mountpoint, ":")
	if len(mountpointSlice) != 3 {
		return fmt.Errorf("invalid mountpoint: %s, should be like: hostname:port:path", fCmd.mountpoint)
	}
	port, err := strconv.ParseUint(mountpointSlice[1], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid point: %s", mountpointSlice[1])
	}
	port_ := uint32(port)
	fCmd.Rpc.Request.Mountpoint = &mds.Mountpoint{
		Hostname: &mountpointSlice[0],
		Port:     &port_,
		Path:     &mountpointSlice[2],
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	fCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "UmountFs")

	table, err := gotable.Create(cobrautil.ROW_FS_NAME, cobrautil.ROW_MOUNTPOINT, cobrautil.ROW_RESULT)
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
	response, errCmd := basecmd.GetRpcResponse(fCmd.Rpc.Info, &fCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(errCmd.Message)
	}
	uf := response.(*mds.UmountFsResponse)
	fCmd.updateTable(uf)
	jsonResult, err := fCmd.Table.JSON(0)
	if err != nil {
		cobra.CheckErr(err)
	}
	var m interface{}
	err = json.Unmarshal([]byte(jsonResult), &m)
	if err != nil {
		cobra.CheckErr(err)
	}
	fCmd.Result = m
	fCmd.Error = errCmd

	return nil
}

func (fCmd *FsCommand) updateTable(info *mds.UmountFsResponse) *cmderror.CmdError {
	row := make(map[string]string)
	row[cobrautil.ROW_FS_NAME] = fCmd.fsName
	row[cobrautil.ROW_MOUNTPOINT] = fCmd.mountpoint
	err := cmderror.ErrUmountFs(int(info.GetStatusCode()))
	row[cobrautil.ROW_RESULT] = err.Message

	fCmd.Table.AddRow(row)
	return err
}

func (fCmd *FsCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd, fCmd)
}
