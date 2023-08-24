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
* Created Date: 2023-0815
* Author: xuchaojie
 */
package protect

import (
    "context"

    cmderror "github.com/opencurve/curve/tools-v2/internal/error"
    cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
    basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
    "github.com/opencurve/curve/tools-v2/pkg/config"
    "github.com/opencurve/curve/tools-v2/pkg/output"
    "github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
    "github.com/spf13/cobra"
    "github.com/spf13/viper"
    "google.golang.org/grpc"
)

const (
    dirExample = `$ curve bs protect --snappath /test0@snap0 --user curve`
)

type ProtectSnapShotRpc struct {
    Info      *basecmd.Rpc
    Request   *nameserver2.ProtectSnapShotRequest
    mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*ProtectSnapShotRpc)(nil) // check interface

func (eRpc *ProtectSnapShotRpc) NewRpcClient(cc grpc.ClientConnInterface) {
    eRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (eRpc *ProtectSnapShotRpc) Stub_Func(ctx context.Context) (interface{}, error) {
    return eRpc.mdsClient.ProtectSnapShot(ctx, eRpc.Request)
}

type protectCommand struct {
    basecmd.FinalCurveCmd
    Rpc      *ProtectSnapShotRpc
    Response *nameserver2.ProtectSnapShotResponse
}

var _ basecmd.FinalCurveCmdFunc = (*protectCommand)(nil)

func (fCmd *protectCommand) Init(cmd *cobra.Command, args []string) error {
    snapFileName := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_SNAPPATH)
    date, errDat := cobrautil.GetTimeofDayUs()
    owner := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_USER)
    if errDat.TypeCode() != cmderror.CODE_SUCCESS {
        return errDat.ToError()
    }
    request := &nameserver2.ProtectSnapShotRequest {
        SnapFileName: &snapFileName,
        Owner:        &owner,
        Date:     &date,
    }
    password := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_PASSWORD)
    if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
        strSig := cobrautil.GetString2Signature(date, owner)
        sig := cobrautil.CalcString2Signature(strSig, password)
        request.Signature = &sig
    }

    mdsAddrs, errMds := config.GetBsMdsAddrSlice(fCmd.Cmd)
    if errMds.TypeCode() != cmderror.CODE_SUCCESS {
        return errMds.ToError()
    }
    timeout := config.GetFlagDuration(fCmd.Cmd, config.RPCTIMEOUT)
    retrytimes := config.GetFlagInt32(fCmd.Cmd, config.RPCRETRYTIMES)
    fCmd.Rpc = &ProtectSnapShotRpc{
        Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ProtectSnapShot"),
            Request: request,
    }
    return nil
}

func (fCmd *protectCommand) RunCommand(cmd *cobra.Command, args []string) error {
    result, err := basecmd.GetRpcResponse(fCmd.Rpc.Info, fCmd.Rpc)
    if err.TypeCode() != cmderror.CODE_SUCCESS {
        return err.ToError()
    }
    fCmd.Response = result.(*nameserver2.ProtectSnapShotResponse)
    fCmd.Result = fCmd.Response
    fCmd.Error = cmderror.ErrBsProtectSnapShot(fCmd.Response.GetStatusCode(), fCmd.Rpc.Request.GetSnapFileName())

    if fCmd.Error.TypeCode() != cmderror.CODE_SUCCESS {
        return fCmd.Error.ToError()
    }
    fCmd.TableNew.Append([]string{fCmd.Error.Message})
    return nil
}

func (fCmd *protectCommand) Print(cmd *cobra.Command, args []string) error {
    return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *protectCommand) ResultPlainOutput() error {
    return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}

func (fCmd *protectCommand) AddFlags() {
    config.AddBsMdsFlagOption(fCmd.Cmd)
    config.AddRpcTimeoutFlag(fCmd.Cmd)
    config.AddRpcRetryTimesFlag(fCmd.Cmd)
    config.AddBsSnapshotPathRequiredFlag(fCmd.Cmd)
    config.AddBsUserRequiredFlag(fCmd.Cmd)
    config.AddBsPasswordOptionFlag(fCmd.Cmd)
}

func NewProtectCommand() *cobra.Command {
    fCmd := &protectCommand{
        FinalCurveCmd: basecmd.FinalCurveCmd{
            Use:     "protect",
            Short:   "protect snapshot in curvebs cluster",
            Example: dirExample,
        },
    }
    return basecmd.NewFinalCurveCli(&fCmd.FinalCurveCmd, fCmd)
}
