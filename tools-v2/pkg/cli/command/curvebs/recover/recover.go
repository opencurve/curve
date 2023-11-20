package recover

import (
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	file "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/recover/volume"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	RecoverExample = `curve bs recover volume --path /curvebs-volume-path 
	--user username [--password password] [--fileid fileid]`
)

// RecoverCommand
type RecoverCommand struct {
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*RecoverCommand)(nil) // check interface

// new RecoverCommand function
func NewRecoverCommand() *cobra.Command {
	rCmd := &RecoverCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "curve bs recover volume",
			Short:   "recover volumes from the RecycleBin",
			Example: RecoverExample,
		},
	}
	return basecmd.NewFinalCurveCli(&rCmd.FinalCurveCmd, rCmd)
}

func (rCmd *RecoverCommand) Init(cmd *cobra.Command, args []string) error {
	header := []string{cobrautil.ROW_RESULT}
	rCmd.SetHeader(header)
	rCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		rCmd.Header, header,
	))
	return nil
}

func (rCmd *RecoverCommand) RunCommand(cmd *cobra.Command, args []string) error {
	rCmd.Result, rCmd.Error = file.RecoverFile(rCmd.Cmd)
	if rCmd.Error.TypeCode() != cmderror.CODE_SUCCESS {
		return rCmd.Error.ToError()
	}
	rCmd.TableNew.Append([]string{rCmd.Error.Message})
	return nil
}

func (rCmd *RecoverCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&rCmd.FinalCurveCmd, rCmd)
}

func (rCmd *RecoverCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&rCmd.FinalCurveCmd)
}

func (rCmd *RecoverCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(rCmd.Cmd)
	config.AddRpcTimeoutFlag(rCmd.Cmd)
	config.AddBsMdsFlagOption(rCmd.Cmd)
	config.AddBsPathOptionFlag(rCmd.Cmd)
	config.AddBsUserOptionFlag(rCmd.Cmd)
	config.AddBsPasswordOptionFlag(rCmd.Cmd)
}
