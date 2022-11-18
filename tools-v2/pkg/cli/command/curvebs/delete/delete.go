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
	deleteCliExample = `delete certain file in the curvebs`
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
	filepath := config.GetBsFlagString(deleteCommand.Cmd, "filename")
	username := config.GetBsFlagString(deleteCommand.Cmd, "username")
	password := config.GetBsFlagString(deleteCommand.Cmd, config.CURVEBS_PASSWORD)
	forcedelete := config.GetFlagBool(deleteCommand.Cmd, "forcedelete")
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
	deleteRequest := nameserver2.DeleteFileRequest{
		FileName:    &filepath,
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
	return nil
}

func (deleteCommand *DeleteCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(deleteCommand.Rpc.Info, deleteCommand.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	deleteCommand.Response = result.(*nameserver2.DeleteFileResponse)
	if deleteCommand.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		err = cmderror.ErrBsDeleteFile()
		return err.ToError()
	}
	return nil
}

func (deleteCommand *DeleteCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&deleteCommand.FinalCurveCmd, deleteCommand)
}

func (deleteCommand *DeleteCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&deleteCommand.FinalCurveCmd)
}

func (deleteCommand *DeleteCommand) AddFlags() {
	config.AddBsStringRequiredFlag(deleteCommand.Cmd, "filename", "the path of the file to be deleted")
	config.AddBsStringRequiredFlag(deleteCommand.Cmd, "username", "owner of the file")
	config.AddBsPasswordOptionFlag(deleteCommand.Cmd)
	config.AddBoolOptionFlag(deleteCommand.Cmd, "forcedelete", "whether to force delete the file")
}

// NewDeleteCommand return the mid cli
func NewDeleteCommand() *cobra.Command {
	deleteCommand := &DeleteCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "delete",
			Short: deleteCliExample,
		},
	}
	basecmd.NewFinalCurveCli(&deleteCommand.FinalCurveCmd, deleteCommand)
	return basecmd.NewFinalCurveCli(&deleteCommand.FinalCurveCmd, deleteCommand)
}
