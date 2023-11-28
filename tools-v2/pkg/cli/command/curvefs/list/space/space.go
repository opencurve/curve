package space

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	space2 "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/space"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	spaceExample = `$ curve fs list space`
)

type ListSpaceRpc struct {
	Info        *basecmd.Rpc
	Request     *space2.StatSpaceRequest
	spaceClient space2.SpaceServiceClient
}

type SpaceCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *ListSpaceRpc
	response *space2.StatSpaceResponse
}

func (sRpc *ListSpaceRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	sRpc.spaceClient = space2.NewSpaceServiceClient(cc)
}

func (sRpc *ListSpaceRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return sRpc.spaceClient.StatSpace(ctx, sRpc.Request)
}

func NewSpaceCommand() *cobra.Command {
	return NewListSpaceCommand().Cmd
}

func NewListSpaceCommand() *SpaceCommand {
	spaceCmd := &SpaceCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "space",
			Short:   "list all space info in the curvefs",
			Example: spaceExample,
		},
	}
	basecmd.NewFinalCurveCli(&spaceCmd.FinalCurveCmd, spaceCmd)
	return spaceCmd
}

func (sCmd *SpaceCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)
	config.AddFsMdsAddrFlag(sCmd.Cmd)
}

func (sCmd *SpaceCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(sCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	sign := uint32(110)
	sCmd.Rpc = &ListSpaceRpc{}
	sCmd.Rpc.Request = &space2.StatSpaceRequest{
		FsId: &sign,
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	sCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "StatSpace")

	header := []string{cobrautil.ROW_TOTAL, cobrautil.ROW_USED, cobrautil.ROW_FREE, cobrautil.ROW_FILE_NUM, cobrautil.ROW_INODE_NUM}
	sCmd.SetHeader(header)

	return nil
}

func (sCmd *SpaceCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SpaceCommand) RunCommand(cmd *cobra.Command, args []string) error {
	response, errCmd := basecmd.GetRpcResponse(sCmd.Rpc.Info, sCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return errCmd.ToError()
	}
	sCmd.response = response.(*space2.StatSpaceResponse)
	res, err := output.MarshalProtoJson(sCmd.response)
	if err != nil {
		return err
	}
	mapRes := res.(map[string]interface{})
	sCmd.Result = mapRes
	sCmd.updateTable()
	sCmd.Error = cmderror.ErrSuccess()
	return nil
}

func (sCmd *SpaceCommand) updateTable() {
	spaceInfo := sCmd.response.GetSpaceInfo()
	row := make(map[string]string)
	row[cobrautil.ROW_TOTAL] = fmt.Sprintf("%d", spaceInfo.GetSize())
	row[cobrautil.ROW_USED] = fmt.Sprintf("%d", spaceInfo.GetSize()-spaceInfo.GetAvailable())
	row[cobrautil.ROW_FREE] = fmt.Sprintf("%d", spaceInfo.GetAvailable())
	row[cobrautil.ROW_FILE_NUM] = fmt.Sprintf("%d", 1)
	row[cobrautil.ROW_INODE_NUM] = fmt.Sprintf("%d", 2)

	list := cobrautil.Map2List(row, sCmd.Header)
	sCmd.TableNew.Append(list)
}

func (sCmd *SpaceCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}
