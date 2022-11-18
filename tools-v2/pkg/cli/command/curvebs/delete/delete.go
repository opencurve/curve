/*
 * Project: tools-v2
 * Created Date: 2022-11-14
 * Author: shentupenghui@gmail.com
 */

package delete

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
	deleteCliExample = `curve bs delete --filename /curvebs-file-name --username username [--password password] [--forcedelete true]`
)

type DeleteCertainFileRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.DeleteFileRequest
	mdsClient nameserver2.CurveFSServiceClient
}

// DeleteCommand definition
type DeleteCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *DeleteCertainFileRpc
	Response *nameserver2.DeleteFileResponse
}

var _ basecmd.FinalCurveCmdFunc = (*DeleteCommand)(nil)

func (gRpc *DeleteCertainFileRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)

}

func (gRpc *DeleteCertainFileRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.DeleteFile(ctx, gRpc.Request)
}

func (deleteCommand *DeleteCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(deleteCommand.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	//get the default timeout and retrytimes
	timeout := config.GetFlagDuration(deleteCommand.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(deleteCommand.Cmd, config.RPCRETRYTIMES)
	filename := config.GetBsFlagString(deleteCommand.Cmd, config.CURVEBS_FILENAME)
	username := config.GetBsFlagString(deleteCommand.Cmd, config.CURVEBS_USER)
	password := config.GetBsFlagString(deleteCommand.Cmd, config.CURVEBS_PASSWORD)
	forcedelete := config.GetFlagBool(deleteCommand.Cmd, config.CURVEBS_FORCEDELETE)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
	deleteRequest := nameserver2.DeleteFileRequest{
		FileName:    &filename,
		Owner:       &username,
		Date:        &date,
		ForceDelete: &forcedelete,
	}
	if username == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, username)
		sig := cobrautil.CalcString2Signature(strSig, password)
		deleteRequest.Signature = &sig
	}
	deleteCommand.Rpc = &DeleteCertainFileRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "DeleteFile"),
		Request: &deleteRequest,
	}
	header := []string{cobrautil.ROW_RESULT, cobrautil.ROW_REASON}
	deleteCommand.SetHeader(header)
	deleteCommand.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		deleteCommand.Header, header,
	))
	return nil
}

func (deleteCommand *DeleteCommand) RunCommand(cmd *cobra.Command, args []string) error {
	out := make(map[string]string)
	result, err := basecmd.GetRpcResponse(deleteCommand.Rpc.Info, deleteCommand.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		out[cobrautil.ROW_RESULT] = "failed"
		out[cobrautil.ROW_REASON] = err.Message
		return nil
	}
	deleteCommand.Response = result.(*nameserver2.DeleteFileResponse)
	if deleteCommand.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		err = cmderror.ErrBsDeleteFile()
		out[cobrautil.ROW_RESULT] = "failed"
		out[cobrautil.ROW_REASON] = err.Message
		return nil
	}
	out[cobrautil.ROW_RESULT] = "success"
	out[cobrautil.ROW_REASON] = ""
	list := cobrautil.Map2List(out, []string{cobrautil.ROW_RESULT, cobrautil.ROW_REASON})
	deleteCommand.TableNew.Append(list)
	return nil
}

func (deleteCommand *DeleteCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&deleteCommand.FinalCurveCmd, deleteCommand)
}

func (deleteCommand *DeleteCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&deleteCommand.FinalCurveCmd)
}

func (deleteCommand *DeleteCommand) AddFlags() {
	config.AddBsFilenameRequiredFlag(deleteCommand.Cmd)
	config.AddBsUsernameRequiredFlag(deleteCommand.Cmd)
	config.AddBsPasswordOptionFlag(deleteCommand.Cmd)
	config.AddBsForceDeleteOptionFlag(deleteCommand.Cmd)
}

// NewDeleteCommand return the mid cli
func NewDeleteCommand() *cobra.Command {
	deleteCommand := &DeleteCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "delete",
			Short:   "delete certain file in curvebs",
			Example: deleteCliExample,
		},
	}
	basecmd.NewFinalCurveCli(&deleteCommand.FinalCurveCmd, deleteCommand)
	return basecmd.NewFinalCurveCli(&deleteCommand.FinalCurveCmd, deleteCommand)
}
