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
 * Created Date: 2022-06-17
 * Author: chengyi (Cyber-SiKu)
 */

package fs

import (
	"context"
	"fmt"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/mds"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type DeleteFsRpc struct {
	Info      *basecmd.Rpc
	Request   *mds.DeleteFsRequest
	mdsClient mds.MdsServiceClient
}

var _ basecmd.RpcFunc = (*DeleteFsRpc)(nil) // check interface

type FsCommand struct {
	basecmd.FinalCurveCmd
	Rpc *DeleteFsRpc
}

var _ basecmd.FinalCurveCmdFunc = (*FsCommand)(nil) // check interface

func (dfRpc *DeleteFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	dfRpc.mdsClient = mds.NewMdsServiceClient(cc)
}

func (dfRpc *DeleteFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return dfRpc.mdsClient.DeleteFs(ctx, dfRpc.Request)
}

func NewFsCommand() *cobra.Command {
	fsCmd := &FsCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "fs",
			Short: "delete a fs from curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&fsCmd.FinalCurveCmd, fsCmd)
	return fsCmd.Cmd
}

func (pCmd *FsCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
	config.AddFsMdsAddrFlag(pCmd.Cmd)
	config.AddFsNameRequiredFlag(pCmd.Cmd)
	config.AddNoConfirmOptionFlag(pCmd.Cmd)
}

func (pCmd *FsCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(pCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	table, err := gotable.Create("fs name", "result")
	if err != nil {
		return err
	}
	pCmd.Table = table

	fsName := viper.GetString(config.VIPER_CURVEFS_FSNAME)

	request := &mds.DeleteFsRequest{
		FsName: &fsName,
	}
	pCmd.Rpc = &DeleteFsRpc{
		Request: request,
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	pCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "DeleteFs")

	return nil
}

func (pCmd *FsCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *FsCommand) RunCommand(cmd *cobra.Command, args []string) error {
	fsName := pCmd.Rpc.Request.GetFsName()
	if !viper.GetBool(config.VIPER_CURVEFS_NOCONFIRM) && !cobrautil.AskConfirmation("After deleting fs, the data cannot be retrieved!!", fsName) {
		pCmd.Cmd.SilenceUsage = true
		return fmt.Errorf("abort delete fs")
	}

	result, err := basecmd.GetRpcResponse(pCmd.Rpc.Info, pCmd.Rpc)
	var errs []*cmderror.CmdError
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		errs = append(errs, err)
	}
	response := result.(*mds.DeleteFsResponse)

	errDel := cmderror.ErrDeleteFs(int(response.GetStatusCode()))
	row := map[string]string{
		"fs name" : pCmd.Rpc.Request.GetFsName(),
		"result" : errDel.Message,
	}
	pCmd.Table.AddRow(row)

	res, errTranslate := output.MarshalProtoJson(response)
	if errTranslate != nil {
		errMar := cmderror.ErrMarShalProtoJson()
		errMar.Format(errTranslate.Error())
		errs = append(errs, errMar)
	}

	pCmd.Result = res
	pCmd.Error = cmderror.MostImportantCmdError(errs)

	return nil
}

func (pCmd *FsCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd, pCmd)
}
