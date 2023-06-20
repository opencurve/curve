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
* Created Date: 2023-06-19
* Author: xuchaojie
 */
package snapshot

import (
  "fmt"
	"context"
	"time"

	"github.com/dustin/go-humanize"
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
	dirExample = `$ curve bs create snapshot --path /test --user curve`
)

type CreateSnapShotRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.CreateSnapShotRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*CreateSnapShotRpc)(nil) // check interface


func (eRpc *CreateSnapShotRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	eRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (eRpc *CreateSnapShotRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return eRpc.mdsClient.CreateSnapShot(ctx, eRpc.Request)
}

type snapshotCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *CreateSnapShotRpc
	Response *nameserver2.CreateSnapShotResponse
}

var _ basecmd.FinalCurveCmdFunc = (*snapshotCommand)(nil)

func (fCmd *snapshotCommand) Init(cmd *cobra.Command, args []string) error {
	fileName := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_PATH)
	date, errDat := cobrautil.GetTimeofDayUs()
	owner := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_USER)
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
  request := &nameserver2.CreateSnapShotRequest {
      FileName: &fileName,
		  Date:     &date,
		  Owner:    &owner,
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
  fCmd.Rpc = &CreateSnapShotRpc{
    Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "CreateSnapshot"),
		Request: request,
  }
	header := []string{
		cobrautil.ROW_ID,
		cobrautil.ROW_FILE_NAME,
		cobrautil.ROW_PARENT_ID,
		cobrautil.ROW_FILE_TYPE,
		cobrautil.ROW_OWNER,
		cobrautil.ROW_CTIME,
		cobrautil.ROW_FILE_SIZE,
    cobrautil.ROW_SEQ,
    cobrautil.ROW_RESULT,
	}
	fCmd.SetHeader(header)
	return nil
}

func (fCmd *snapshotCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(fCmd.Rpc.Info, fCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	fCmd.Response = result.(*nameserver2.CreateSnapShotResponse)
	fCmd.Result = fCmd.Response

	if fCmd.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		err = cmderror.ErrBsCreateSnapShot(fCmd.Response.GetStatusCode(), fCmd.Rpc.Request.GetFileName())
		return err.ToError()
	}

	info := fCmd.Response.GetSnapShotFileInfo()
  row := make(map[string]string)
	row[cobrautil.ROW_ID] = fmt.Sprintf("%d", info.GetId())

  var fileName string
  path := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_PATH)
	if path[len(path)-1] == '/' {
		fileName = path + info.GetFileName()
	} else {
		fileName = path + "/" + info.GetFileName()
	}
	row[cobrautil.ROW_FILE_NAME] = fileName
	row[cobrautil.ROW_PARENT_ID] = fmt.Sprintf("%d", info.GetParentId())
	row[cobrautil.ROW_FILE_TYPE] = fmt.Sprintf("%v", info.GetFileType())
	row[cobrautil.ROW_OWNER] = info.GetOwner()
	row[cobrautil.ROW_CTIME] = time.Unix(int64(info.GetCtime()/1000000), 0).Format("2006-01-02 15:04:05")
  row[cobrautil.ROW_FILE_SIZE] = fmt.Sprintf("%s", humanize.IBytes(info.GetLength()))
  row[cobrautil.ROW_SEQ] = fmt.Sprintf("%d", info.GetSeqNum())
  row[cobrautil.ROW_RESULT] = cmderror.Success().Message

	fCmd.TableNew.Append(cobrautil.Map2List(row, fCmd.Header))
	return nil
}

func (fCmd *snapshotCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *snapshotCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}

func (fCmd *snapshotCommand) AddFlags() {
	config.AddBsMdsFlagOption(fCmd.Cmd)
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddBsPathRequiredFlag(fCmd.Cmd)
	config.AddBsUserRequiredFlag(fCmd.Cmd)
	config.AddBsPasswordOptionFlag(fCmd.Cmd)
}

func NewCreateSnapshotCommand() *snapshotCommand {
	fCmd := &snapshotCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "snapshot",
			Short:   "create snapshot for file in curvebs cluster",
			Example: dirExample,
		},
	}
	basecmd.NewFinalCurveCli(&fCmd.FinalCurveCmd, fCmd)
	return fCmd
}

func NewSnapshotCommand() *cobra.Command {
	return NewCreateSnapshotCommand().Cmd
}
