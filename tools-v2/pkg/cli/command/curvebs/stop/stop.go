package stop

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	snapshot "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/stop/volumeSnapshot"
	"github.com/spf13/cobra"
)

type StopCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*StopCommand)(nil)

func (sCmd *StopCommand) AddSubCommands() {
	sCmd.Cmd.AddCommand(
		snapshot.NewStopVolumeSnapshotCommand(),
	)
}

func NewStopCommand() *cobra.Command {
	sCmd := &StopCommand{
		basecmd.MidCurveCmd{
			Use:   "stop",
			Short: "stop volume snapshot in the curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&sCmd.MidCurveCmd, sCmd)
}
