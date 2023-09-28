package volumeSnapshot

import (
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	stopExample = `$ curve bs stop volumeSnapshot`
)

type StopCmd struct {
	basecmd.FinalCurveCmd
	snapshotAddrs []string
	timeout       time.Duration

	user     string
	fileName string
	uuid     string
}

var _ basecmd.FinalCurveCmdFunc = (*StopCmd)(nil)

func (sCmd *StopCmd) Init(cmd *cobra.Command, args []string) error {
	snapshotAddrs, err := config.GetBsSnapshotAddrSlice(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS || len(snapshotAddrs) == 0 {
		return err.ToError()
	}
	sCmd.snapshotAddrs = snapshotAddrs
	sCmd.timeout = config.GetFlagDuration(sCmd.Cmd, config.HTTPTIMEOUT)
	sCmd.user = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_USER)
	sCmd.fileName = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_PATH)
	sCmd.uuid = config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_SNAPSHOTSEQID)
	header := []string{cobrautil.ROW_RESULT}
	sCmd.SetHeader(header)
	return nil
}

func (sCmd *StopCmd) RunCommand(cmd *cobra.Command, args []string) error {
	s := newStopSnapShot(sCmd.snapshotAddrs, sCmd.timeout, sCmd.user, sCmd.fileName, sCmd.uuid)
	records := s.queryStopBy()
	for _, item := range records {
		err := s.stopSnapShot(item.UUID, item.User, item.File)
		if err == nil {
			item.Result = cobrautil.ROW_VALUE_SUCCESS
		} else {
			item.Result = cobrautil.ROW_VALUE_FAILED
		}
		sCmd.TableNew.Append([]string{item.Result})
	}
	sCmd.Result = records
	sCmd.Error = cmderror.Success()
	return nil
}

func (sCmd *StopCmd) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *StopCmd) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func (sCmd *StopCmd) AddFlags() {
	config.AddBsSnapshotCloneFlagOption(sCmd.Cmd)
	config.AddHttpTimeoutFlag(sCmd.Cmd)
	config.AddBsUserOptionFlag(sCmd.Cmd)
	config.AddBsSnapshotSeqIDOptionFlag(sCmd.Cmd)
	config.AddBsPathOptionFlag(sCmd.Cmd)
}

func NewStopVolumeSnapshotCommand() *cobra.Command {
	sCmd := &StopCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "volumeSnapshot",
			Short:   "stop volume snapshot in curvebs cluster",
			Example: stopExample,
		},
	}
	basecmd.NewFinalCurveCli(&sCmd.FinalCurveCmd, sCmd)
	return sCmd.Cmd
}
