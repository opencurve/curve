/*
 * Project: tools-v2
 * Created Date: 2022-11-14
 * Author: shentupenghui@gmail.com
 */

package create

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
	createCliExample = `curve bs create --filename /curvebs-file-name --user username --filelength the_length_num_of_volume [--password password]`
)

type CreateCertainFileRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.CreateFileRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*CreateCertainFileRpc)(nil)

// CreateCommand definition
type CreateCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *CreateCertainFileRpc
	Response *nameserver2.CreateFileResponse
}

var _ basecmd.FinalCurveCmdFunc = (*CreateCommand)(nil)

func (gRpc *CreateCertainFileRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)

}

func (gRpc *CreateCertainFileRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.CreateFile(ctx, gRpc.Request)
}

func (createCommand *CreateCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(createCommand.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	//get the default timeout and retrytimes
	timeout := config.GetFlagDuration(createCommand.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(createCommand.Cmd, config.RPCRETRYTIMES)
	filename := config.GetBsFlagString(createCommand.Cmd, config.CURVEBS_FILENAME)
	username := config.GetBsFlagString(createCommand.Cmd, config.CURVEBS_USER)
	password := config.GetBsFlagString(createCommand.Cmd, config.CURVEBS_PASSWORD)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}

	fileLength := config.GetFlagUint64(createCommand.Cmd, config.CURVEBS_FILELENGTH)
	fileTypeStr := config.GetBsFlagString(createCommand.Cmd, config.CURVEBS_FILETYPE)
	//fileTypeStr := "directory"
	fileType, errFstype := cobrautil.TranslateFileType(fileTypeStr)
	if errFstype.TypeCode() != cmderror.CODE_SUCCESS {
		return errFstype.ToError()
	}
	createRequest := nameserver2.CreateFileRequest{
		FileName:   &filename,
		Owner:      &username,
		Date:       &date,
		FileType:   &fileType,
		FileLength: &fileLength,
	}
	if username == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, username)
		sig := cobrautil.CalcString2Signature(strSig, password)
		createRequest.Signature = &sig
	}
	createCommand.Rpc = &CreateCertainFileRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "CreateFile"),
		Request: &createRequest,
	}
	header := []string{cobrautil.ROW_RESULT, cobrautil.ROW_REASON}
	createCommand.SetHeader(header)
	createCommand.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		createCommand.Header, header,
	))
	return nil
}

func (createCommand *CreateCommand) RunCommand(cmd *cobra.Command, args []string) error {
	out := make(map[string]string)
	result, err := basecmd.GetRpcResponse(createCommand.Rpc.Info, createCommand.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		out[cobrautil.ROW_RESULT] = "failed"
		out[cobrautil.ROW_REASON] = err.Message
		return nil
	}
	createCommand.Response = result.(*nameserver2.CreateFileResponse)
	if createCommand.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		err = cmderror.ErrBsCreateFile()
		out[cobrautil.ROW_RESULT] = "failed"
		out[cobrautil.ROW_REASON] = err.Message
		return nil
	}
	out[cobrautil.ROW_RESULT] = "success"
	out[cobrautil.ROW_REASON] = ""
	list := cobrautil.Map2List(out, []string{cobrautil.ROW_RESULT, cobrautil.ROW_REASON})
	createCommand.TableNew.Append(list)
	return nil
}

func (createCommand *CreateCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&createCommand.FinalCurveCmd, createCommand)
}

func (createCommand *CreateCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&createCommand.FinalCurveCmd)
}

func (createCommand *CreateCommand) AddFlags() {
	config.AddBsMdsFlagOption(createCommand.Cmd)
	config.AddRpcTimeoutFlag(createCommand.Cmd)
	config.AddRpcRetryTimesFlag(createCommand.Cmd)
	config.AddBsFilenameRequiredFlag(createCommand.Cmd)
	config.AddBsUserOptionFlag(createCommand.Cmd)
	config.AddBsPasswordOptionFlag(createCommand.Cmd)
	config.AddBsFileLengthOptionFlag(createCommand.Cmd)
	config.AddBsFileTypeRequiredFlag(createCommand.Cmd)
}

// NewCreateCommand return the mid cli
func NewCreateCommand() *cobra.Command {
	createCommand := &CreateCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "create",
			Short:   "create certain file in curvebs",
			Example: createCliExample,
		},
	}
	basecmd.NewFinalCurveCli(&createCommand.FinalCurveCmd, createCommand)
	return createCommand.Cmd
}
