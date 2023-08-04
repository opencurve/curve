/*
 * Project: tools-v2
 * Created Date: 2022-11-14
 * Author: shentupenghui@gmail.com
 */

package delete

import (
	"github.com/spf13/cobra"

	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/delete/file"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/delete/peer"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/delete/snapshot"
)

type DeleteCommand struct {
	basecmd.MidCurveCmd
}

var _ basecmd.MidCurveCmdFunc = (*DeleteCommand)(nil) // check interface

func (dCmd *DeleteCommand) AddSubCommands() {
	dCmd.Cmd.AddCommand(
		file.NewFileCommand(),
		peer.NewCommand(),
        snapshot.NewSnapshotCommand(),
	)
}

func NewDeleteCommand() *cobra.Command {
	dCmd := &DeleteCommand{
		basecmd.MidCurveCmd{
			Use:   "delete",
			Short: "delete resources in the curvebs",
		},
	}
	return basecmd.NewMidCurveCli(&dCmd.MidCurveCmd, dCmd)
}
