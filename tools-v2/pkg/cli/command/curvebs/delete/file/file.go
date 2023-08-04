package file

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
)

const (
	deleteCliExample = `curve bs delete file --path /curvebs-file-path --user username [--password password] [--force true]`
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
	path := config.GetBsFlagString(deleteCommand.Cmd, config.CURVEBS_PATH)
	username := config.GetBsFlagString(deleteCommand.Cmd, config.CURVEBS_USER)
	password := config.GetBsFlagString(deleteCommand.Cmd, config.CURVEBS_PASSWORD)
	forcedelete := config.GetBsFlagBool(deleteCommand.Cmd, config.CURVEBS_FORCE)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
	deleteRequest := nameserver2.DeleteFileRequest{
		FileName:    &path,
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
	header := []string{cobrautil.ROW_RESULT}
	deleteCommand.SetHeader(header)
	deleteCommand.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		deleteCommand.Header, header,
	))
	return nil
}

func (deleteCommand *DeleteCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(deleteCommand.Rpc.Info, deleteCommand.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		deleteCommand.Error = err
		deleteCommand.Result = result
		return err.ToError()
	}
	deleteCommand.Response = result.(*nameserver2.DeleteFileResponse)
	if deleteCommand.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		deleteCommand.Error = cmderror.ErrBsDeleteFile(deleteCommand.Response.GetStatusCode())
		deleteCommand.Result = result
		return deleteCommand.Error.ToError()
	}
	out := make(map[string]string)
	out[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS
	list := cobrautil.Map2List(out, []string{cobrautil.ROW_RESULT})
	deleteCommand.TableNew.Append(list)

	deleteCommand.Result, deleteCommand.Error = result, cmderror.Success()
	return nil
}

func (deleteCommand *DeleteCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&deleteCommand.FinalCurveCmd, deleteCommand)
}

func (deleteCommand *DeleteCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&deleteCommand.FinalCurveCmd)
}

func (deleteCommand *DeleteCommand) AddFlags() {
	config.AddBsMdsFlagOption(deleteCommand.Cmd)
	config.AddRpcTimeoutFlag(deleteCommand.Cmd)
	config.AddRpcRetryTimesFlag(deleteCommand.Cmd)

	config.AddBsPathRequiredFlag(deleteCommand.Cmd)
	config.AddBsUserOptionFlag(deleteCommand.Cmd)
	config.AddBsPasswordOptionFlag(deleteCommand.Cmd)
	config.AddBsForceDeleteOptionFlag(deleteCommand.Cmd)
}

// NewCommand return the mid cli
func NewDeleteFileCommand() *DeleteCommand {
	deleteCommand := &DeleteCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "file",
			Short:   "delete certain file in curvebs",
			Example: deleteCliExample,
		},
	}
	basecmd.NewFinalCurveCli(&deleteCommand.FinalCurveCmd, deleteCommand)
	return deleteCommand
}

func NewFileCommand() *cobra.Command {
	return NewDeleteFileCommand().Cmd
}

// DeleteFile function wraps the DeleteCertainFile rpc
func DeleteFile(caller *cobra.Command) (*nameserver2.DeleteFileResponse, *cmderror.CmdError) {
	delCmd := NewDeleteFileCommand()
	config.AlignFlagsValue(caller, delCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER, config.CURVEBS_PASSWORD,
		config.CURVEBS_FORCE,
	})
	delCmd.Cmd.SilenceErrors = true
	delCmd.Cmd.SilenceUsage = true
	delCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := delCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsDeleteFile(*delCmd.Response.StatusCode)
		retErr.Format(err.Error())
		return delCmd.Response, retErr
	}
	return delCmd.Response, cmderror.Success()
}
