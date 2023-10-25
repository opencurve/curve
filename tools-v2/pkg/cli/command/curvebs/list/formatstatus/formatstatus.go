package formatstatus

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	formatExample = `$ curve bs list format-status`
)

type GetFormatStatusRpc struct {
	Info      *basecmd.Rpc
	Request   *topology.ListChunkFormatStatusRequest
	mdsClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*GetFormatStatusRpc)(nil) // check interface

func (gRpc *GetFormatStatusRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = topology.NewTopologyServiceClient(cc)
}

func (gRpc *GetFormatStatusRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.ListChunkFormatStatus(ctx, gRpc.Request)
}

type FormatStatusCommand struct {
	Rpc                  *GetFormatStatusRpc
	FormatStatusInfoList []*topology.ChunkFormatStatus
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*FormatStatusCommand)(nil) // check interface

func NewListFormatStatusCommand() *FormatStatusCommand {
	formatCmd := &FormatStatusCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "format-status",
			Short:   "list all format status in cluster",
			Example: formatExample,
		},
	}

	basecmd.NewFinalCurveCli(&formatCmd.FinalCurveCmd, formatCmd)
	return formatCmd
}

func NewFormatStatusCommand() *cobra.Command {
	return NewListFormatStatusCommand().Cmd
}

func (fCmd *FormatStatusCommand) AddFlags() {
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddBsMdsFlagOption(fCmd.Cmd)
	config.AddBsFilterOptionFlag(fCmd.Cmd)
}

func (fCmd *FormatStatusCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(fCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(fCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(fCmd.Cmd, config.RPCRETRYTIMES)

	fCmd.Rpc = &GetFormatStatusRpc{
		Info:      basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListChunkFormatStatus"),
		Request:   &topology.ListChunkFormatStatusRequest{},
		mdsClient: nil,
	}
	fCmd.SetHeader([]string{
		cobrautil.ROW_CHUNKSERVER, cobrautil.ROW_IP, cobrautil.ROW_PORT, cobrautil.ROW_FORMAT_PERCENT,
	})
	return nil
}

func (fCmd *FormatStatusCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *FormatStatusCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(fCmd.Rpc.Info, fCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		fCmd.Error = err
		fCmd.Result = nil
		return err.ToError()
	}
	res := result.(*topology.ListChunkFormatStatusResponse)
	fCmd.FormatStatusInfoList = res.GetChunkFormatStatus()

	rows := make([]map[string]string, 0)
	for _, info := range fCmd.FormatStatusInfoList {
		fmt.Println("sssss: ", info.GetIp())
		row := make(map[string]string)
		row[cobrautil.ROW_CHUNKSERVER] = fmt.Sprint(info.GetChunkServerID())
		row[cobrautil.ROW_IP] = info.GetIp()
		row[cobrautil.ROW_PORT] = fmt.Sprint(uint64(info.GetPort()))
		row[cobrautil.ROW_FORMAT_PERCENT] = fmt.Sprint(info.GetFormatPercent())
		rows = append(rows, row)
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, fCmd.Header, []string{})
	fCmd.TableNew.AppendBulk(list)
	fCmd.Result, fCmd.Error = rows, cmderror.Success()
	return nil
}

func (fCmd *FormatStatusCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}

func GetFormatStatusInfoList(caller *cobra.Command) ([]*topology.ChunkFormatStatus, *cmderror.CmdError) {
	getCmd := NewListFormatStatusCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.CURVEBS_MDSADDR, config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_FIlTER,
	})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetFormatStatus()
		retErr.Format(err.Error())
		return getCmd.FormatStatusInfoList, retErr
	}
	return getCmd.FormatStatusInfoList, cmderror.Success()
}
