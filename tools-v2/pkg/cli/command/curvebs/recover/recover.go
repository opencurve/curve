package recover

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
	recoverCliExample = `curve bs recover volume --path /curvebs-file-path [--user username] [--password password]`
)

type RecoverCertainFileRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.RecoverFileRequest
	mdsClient nameserver2.CurveFSServiceClient
}

// RecoverCommand definition
type RecoverCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *RecoverCertainFileRpc
	Response *nameserver2.RecoverFileResponse
}

var _ basecmd.FinalCurveCmdFunc = (*RecoverCommand)(nil)

func (gRpc *RecoverCertainFileRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)

}

func (gRpc *RecoverCertainFileRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.RecoverFile(ctx, gRpc.Request)
}

func (recoverCommand *RecoverCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(recoverCommand.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	//get the default timeout and retrytimes
	timeout := config.GetFlagDuration(recoverCommand.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(recoverCommand.Cmd, config.RPCRETRYTIMES)
	path := config.GetBsFlagString(recoverCommand.Cmd, config.CURVEBS_PATH)
	username := config.GetBsFlagString(recoverCommand.Cmd, config.CURVEBS_USER)
	password := config.GetBsFlagString(recoverCommand.Cmd, config.CURVEBS_PASSWORD)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
	recoverRequest := nameserver2.RecoverFileRequest{
		FileName: &path,
		Owner:    &username,
		Date:     &date,
	}
	if username == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, username)
		sig := cobrautil.CalcString2Signature(strSig, password)
		recoverRequest.Signature = &sig
	}
	recoverCommand.Rpc = &RecoverCertainFileRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "RecoverFile"),
		Request: &recoverRequest,
	}
	header := []string{cobrautil.ROW_RESULT}
	recoverCommand.SetHeader(header)
	recoverCommand.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		recoverCommand.Header, header,
	))
	return nil
}

func (recoverCommand *RecoverCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(recoverCommand.Rpc.Info, recoverCommand.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		recoverCommand.Error = err
		recoverCommand.Result = result
		return err.ToError()
	}
	recoverCommand.Response = result.(*nameserver2.RecoverFileResponse)
	if recoverCommand.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		recoverCommand.Error = cmderror.ErrBsRecoverFile(recoverCommand.Response.GetStatusCode(), *recoverCommand.Rpc.Request.FileName)
		recoverCommand.Result = result
		return recoverCommand.Error.ToError()
	}
	out := make(map[string]string)
	out[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS
	list := cobrautil.Map2List(out, []string{cobrautil.ROW_RESULT})
	recoverCommand.TableNew.Append(list)

	recoverCommand.Result, recoverCommand.Error = result, cmderror.Success()
	return nil
}

func (recoverCommand *RecoverCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&recoverCommand.FinalCurveCmd, recoverCommand)
}

func (recoverCommand *RecoverCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&recoverCommand.FinalCurveCmd)
}

func (recoverCommand *RecoverCommand) AddFlags() {
	config.AddBsMdsFlagOption(recoverCommand.Cmd)
	config.AddRpcTimeoutFlag(recoverCommand.Cmd)
	config.AddRpcRetryTimesFlag(recoverCommand.Cmd)

	config.AddBsPathRequiredFlag(recoverCommand.Cmd)
	config.AddBsUserOptionFlag(recoverCommand.Cmd)
	config.AddBsPasswordOptionFlag(recoverCommand.Cmd)
}

// NewCommand return the mid cli
func NewRecoverFileCommand() *RecoverCommand {
	recoverCommand := &RecoverCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "recover volume",
			Short:   "recover certain volume in curvebs",
			Example: recoverCliExample,
		},
	}
	basecmd.NewFinalCurveCli(&recoverCommand.FinalCurveCmd, recoverCommand)
	return recoverCommand
}

func NewRecoverCommand() *cobra.Command {
	return NewRecoverFileCommand().Cmd
}

// RecoverFile function wraps the recoverCertainFile rpc
func RecoverFile(caller *cobra.Command) (*nameserver2.RecoverFileResponse, *cmderror.CmdError) {
	rCmd := NewRecoverFileCommand()
	config.AlignFlagsValue(caller, rCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER, config.CURVEBS_PASSWORD,
		config.CURVEBS_FORCE,
	})
	rCmd.Cmd.SilenceErrors = true
	rCmd.Cmd.SilenceUsage = true
	rCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := rCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsRecoverFile(rCmd.Response.GetStatusCode(), *rCmd.Rpc.Request.FileName)
		retErr.Format(err.Error())
		return rCmd.Response, retErr
	}
	return rCmd.Response, cmderror.Success()
}
