package volume

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
	RecoverExample = ` $ curve bs recover volume --path /test/path --user username `
)

func NewVolumeCommand() *cobra.Command {
	return NewRecoverFileCommand().Cmd
}

func NewRecoverFileCommand() *RecoverFileCommand {
	RecoverFileCommand := &RecoverFileCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "volume",
			Short:   "recover volume in curvebs",
			Example: RecoverExample,
		},
	}
	basecmd.NewFinalCurveCli(&RecoverFileCommand.FinalCurveCmd, RecoverFileCommand)
	return RecoverFileCommand
}

type RecoverCertainFileRPC struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.RecoverFileRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*RecoverCertainFileRPC)(nil)

func (gRpc *RecoverCertainFileRPC) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (gRpc *RecoverCertainFileRPC) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.RecoverFile(ctx, gRpc.Request)
}

type RecoverFileCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *RecoverCertainFileRPC
	Response *nameserver2.RecoverFileResponse
}

var _ basecmd.FinalCurveCmdFunc = (*RecoverFileCommand)(nil)

func (rCmd *RecoverFileCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(rCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	//get the default timeout and retrytimes
	timeout := config.GetFlagDuration(rCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(rCmd.Cmd, config.RPCRETRYTIMES)

	//get the params needed from commandline
	path := config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_PATH)
	username := config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_USER)
	password := config.GetBsFlagString(rCmd.Cmd, config.CURVEBS_PASSWORD)
	fileId := config.GetBsFlagUint64(rCmd.Cmd, config.CURVEBS_FILE_ID)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}

	//add basic info
	recoverRequest := nameserver2.RecoverFileRequest{
		FileName: &path,
		Owner:    &username,
		FileId:   &fileId,
		Date:     &date,
	}

	//add signature
	if username == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, username)
		sig := cobrautil.CalcString2Signature(strSig, password)
		recoverRequest.Signature = &sig
	}
	rCmd.Rpc = &RecoverCertainFileRPC{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "recoverFile"),
		Request: &recoverRequest,
	}

	//set headers
	header := []string{
		cobrautil.ROW_FILE_NAME,
		cobrautil.ROW_RESULT,
	}
	rCmd.SetHeader(header)
	out := make(map[string]string)
	out[cobrautil.ROW_FILE_NAME] = *recoverRequest.FileName
	list := cobrautil.Map2List(out, []string{cobrautil.ROW_FILE_NAME})
	rCmd.TableNew.Append(list)
	return nil
}

func (rCmd *RecoverFileCommand) AddFlags() {
	config.AddRpcTimeoutFlag(rCmd.Cmd)
	config.AddRpcRetryTimesFlag(rCmd.Cmd)
	config.AddBsMdsFlagOption(rCmd.Cmd)

	//file path and user name is required.
	config.AddBsPathRequiredFlag(rCmd.Cmd)
	config.AddBsUserRequireFlag(rCmd.Cmd)

	//password and fileID is optional.
	config.AddBsPasswordOptionFlag(rCmd.Cmd)
	config.AddBsFileIdOptionFlag(rCmd.Cmd)
}

func (rCmd *RecoverFileCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(rCmd.Rpc.Info, rCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		rCmd.Error = err
		rCmd.Result = result
		return err.ToError()
	}
	rCmd.Response = result.(*nameserver2.RecoverFileResponse)
	if rCmd.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		rCmd.Error = cmderror.ErrBsRecoverFile()
		rCmd.Result = result
		return rCmd.Error.ToError()
	}
	out := make(map[string]string)
	out[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS
	list := cobrautil.Map2List(out, []string{cobrautil.ROW_RESULT})
	rCmd.TableNew.Append(list)
	rCmd.Result, rCmd.Error = result, cmderror.Success()
	return nil
}

func (rCmd *RecoverFileCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&rCmd.FinalCurveCmd, rCmd)
}

func (rCmd *RecoverFileCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&rCmd.FinalCurveCmd)
}

func RecoverFile(caller *cobra.Command) (*nameserver2.RecoverFileResponse, *cmderror.CmdError) {
	rCmd := NewRecoverFileCommand()
	config.AlignFlagsValue(caller, rCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER,
		config.CURVEBS_PASSWORD, config.CURVEBS_FILE_ID,
	})
	rCmd.Cmd.SilenceErrors = true
	rCmd.Cmd.SilenceUsage = true
	rCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := rCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsRecoverFile()
		retErr.Format(err.Error())
		return rCmd.Response, retErr
	}
	return rCmd.Response, cmderror.Success()
}
